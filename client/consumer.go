package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes one delivery. Returning an error dead-letters the message
// unless RequeueOnError is enabled. ErrRequeue and ErrDiscard override that behavior.
type Handler func(ctx context.Context, msg Message) error

// Consumer manages one or more subscriptions. Every subscription owns a dedicated channel.
type Consumer struct {
	conn        *Connection
	serviceName string
	log         *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// NewConsumer creates a consumer manager.
func NewConsumer(conn *Connection, serviceName string, opts ...ConsumerOption) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.Default()
	if conn != nil && conn.cfg.Logger != nil {
		logger = conn.cfg.Logger
	}

	c := &Consumer{
		conn:        conn,
		serviceName: serviceName,
		log:         logger.With("component", "rabbitmq.consumer", "service", serviceName),
		ctx:         ctx,
		cancel:      cancel,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Subscribe registers a subscription with default options.
func (c *Consumer) Subscribe(queueName, exchange, routingKey string, handler Handler) error {
	return c.SubscribeWithOpts(queueName, exchange, routingKey, handler, DefaultSubscribeOptions())
}

// SubscribeWithOpts registers a subscription and waits for its initial setup.
func (c *Consumer) SubscribeWithOpts(queueName, exchange, routingKey string, handler Handler, opts SubscribeOptions) error {
	if c == nil {
		return errors.New("rabbitmq: consumer is not initialized")
	}
	if queueName == "" {
		return ErrEmptyQueueName
	}
	if exchange == "" {
		return ErrEmptyExchangeName
	}
	if handler == nil {
		return ErrNilHandler
	}
	if c.conn == nil {
		return errors.New("rabbitmq: consumer is not initialized")
	}

	opts = opts.withDefaults()

	subCtx, subCancel := context.WithCancel(c.ctx)
	sub := &subscription{
		consumer:   c,
		queueName:  queueName,
		exchange:   exchange,
		routingKey: routingKey,
		handler:    handler,
		opts:       opts,
		ctx:        subCtx,
		cancel:     subCancel,
		backoff: newBackoff(
			c.conn.cfg.InitialBackoff,
			c.conn.cfg.MaxBackoff,
			c.conn.cfg.BackoffFactor,
			c.conn.cfg.Jitter,
		),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		subCancel()
		return ErrConsumerClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()

	started := make(chan error, 1)
	go func() {
		defer c.wg.Done()
		sub.run(started)
	}()

	select {
	case err := <-started:
		if err != nil {
			subCancel()
			return err
		}
		return nil
	case <-c.ctx.Done():
		subCancel()
		return ErrConsumerClosed
	}
}

type subscription struct {
	consumer   *Consumer
	queueName  string
	exchange   string
	routingKey string
	handler    Handler
	opts       SubscribeOptions
	ctx        context.Context
	cancel     context.CancelFunc
	backoff    *backoffPolicy
}

func (s *subscription) run(started chan<- error) {
	defer s.cancel()

	first := true
	attempt := 0

	for {
		if s.ctx.Err() != nil {
			return
		}

		attempt++
		ch, deliveries, notifyClose, err := s.setup()
		if err == nil {
			if first {
				started <- nil
				first = false
			}

			startedAt := time.Now()
			err = s.process(ch, deliveries, notifyClose)
			s.closeChannel(ch)
			if time.Since(startedAt) > 30*time.Second {
				attempt = 0
			}
		}

		if s.ctx.Err() != nil {
			return
		}

		if first {
			started <- err
			return
		}

		if err != nil {
			s.consumer.log.Warn("subscription stopped; reconnecting",
				"queue", s.queueName,
				"exchange", s.exchange,
				"routing_key", s.routingKey,
				"error", err,
			)
		}

		delay := s.backoff.next(attempt)
		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *subscription) setup() (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	ch, err := s.consumer.conn.Channel(s.ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rabbitmq: get channel: %w", err)
	}

	fail := func(err error) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
		_ = ch.Close()
		return nil, nil, nil, err
	}

	if err := s.declareTopology(ch); err != nil {
		return fail(err)
	}
	if err := ch.Qos(s.opts.Prefetch, 0, false); err != nil {
		return fail(fmt.Errorf("rabbitmq: set qos: %w", err))
	}

	tag := s.opts.ConsumerTag
	if tag == "" {
		tag = s.consumer.serviceName + "-" + s.queueName
	}

	deliveries, err := ch.Consume(
		s.queueName,
		tag,
		s.opts.AutoAck,
		s.opts.Exclusive,
		s.opts.NoLocal,
		s.opts.NoWait,
		nil,
	)
	if err != nil {
		return fail(fmt.Errorf("rabbitmq: consume %q: %w", s.queueName, err))
	}

	notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))
	return ch, deliveries, notifyClose, nil
}

func (s *subscription) declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		s.exchange,
		s.opts.ExchangeType,
		!s.opts.NonDurableExchange,
		false,
		false,
		s.opts.NoWait,
		nil,
	); err != nil {
		return fmt.Errorf("rabbitmq: declare exchange %q: %w", s.exchange, err)
	}

	args := make(amqp.Table, len(s.opts.QueueArguments)+2)
	for key, value := range s.opts.QueueArguments {
		args[key] = value
	}

	if !s.opts.DisableDLQ {
		dlqExchange, dlqQueue, dlqRoutingKey := s.opts.DLQ.names(s.queueName)
		if err := ch.ExchangeDeclare(dlqExchange, "direct", true, false, false, s.opts.NoWait, nil); err != nil {
			return fmt.Errorf("rabbitmq: declare dlq exchange %q: %w", dlqExchange, err)
		}
		if _, err := ch.QueueDeclare(dlqQueue, true, false, false, s.opts.NoWait, nil); err != nil {
			return fmt.Errorf("rabbitmq: declare dlq %q: %w", dlqQueue, err)
		}
		if err := ch.QueueBind(dlqQueue, dlqRoutingKey, dlqExchange, s.opts.NoWait, nil); err != nil {
			return fmt.Errorf("rabbitmq: bind dlq %q: %w", dlqQueue, err)
		}

		args["x-dead-letter-exchange"] = dlqExchange
		args["x-dead-letter-routing-key"] = dlqRoutingKey
	}

	if _, err := ch.QueueDeclare(
		s.queueName,
		!s.opts.NonDurableQueue,
		s.opts.AutoDelete,
		s.opts.Exclusive,
		s.opts.NoWait,
		args,
	); err != nil {
		return fmt.Errorf("rabbitmq: declare queue %q: %w", s.queueName, err)
	}

	if err := ch.QueueBind(s.queueName, s.routingKey, s.exchange, s.opts.NoWait, nil); err != nil {
		return fmt.Errorf("rabbitmq: bind queue %q: %w", s.queueName, err)
	}

	return nil
}

func (s *subscription) process(ch *amqp.Channel, deliveries <-chan amqp.Delivery, notifyClose <-chan *amqp.Error) error {
	ctx, cancel := context.WithCancel(s.ctx)
	var handlers sync.WaitGroup
	defer func() {
		cancel()
		handlers.Wait()
	}()

	sem := make(chan struct{}, s.opts.Concurrency)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err, ok := <-notifyClose:
			if !ok || err == nil {
				return ErrChannelClosed
			}
			return fmt.Errorf("rabbitmq: channel closed: %w", err)

		case delivery, ok := <-deliveries:
			if !ok {
				return ErrChannelClosed
			}

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

			handlers.Add(1)
			go func(delivery amqp.Delivery) {
				defer handlers.Done()
				defer func() { <-sem }()
				s.handle(ctx, delivery)
			}(delivery)
		}
	}
}

func (s *subscription) handle(ctx context.Context, delivery amqp.Delivery) {
	handlerCtx := ctx
	if s.opts.HandlerTimeout > 0 {
		var cancel context.CancelFunc
		handlerCtx, cancel = context.WithTimeout(ctx, s.opts.HandlerTimeout)
		defer cancel()
	}

	err := s.callHandler(handlerCtx, messageFromDelivery(delivery))
	if err == nil {
		s.ack(delivery)
		return
	}

	if s.opts.AutoAck {
		s.consumer.log.Error("handler failed in auto-ack mode; message is already acknowledged",
			"queue", s.queueName,
			"routing_key", delivery.RoutingKey,
			"delivery_tag", delivery.DeliveryTag,
			"error", err,
		)
		return
	}

	switch {
	case errors.Is(err, ErrDiscard):
		s.ack(delivery)
	case errors.Is(err, ErrRequeue):
		s.nack(delivery, true)
	case ctx.Err() != nil:
		s.nack(delivery, true)
	case errors.Is(err, context.DeadlineExceeded):
		s.nack(delivery, true)
	default:
		s.consumer.log.Error("handler failed; rejecting message",
			"queue", s.queueName,
			"exchange", delivery.Exchange,
			"routing_key", delivery.RoutingKey,
			"delivery_tag", delivery.DeliveryTag,
			"redelivered", delivery.Redelivered,
			"error", err,
		)
		s.nack(delivery, s.opts.RequeueOnError)
	}
}

func (s *subscription) callHandler(ctx context.Context, msg Message) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic in handler: %v\n%s", recovered, debug.Stack())
		}
	}()
	return s.handler(ctx, msg)
}

func (s *subscription) ack(delivery amqp.Delivery) {
	if s.opts.AutoAck {
		return
	}
	if err := delivery.Ack(false); err != nil && !isClosedError(err) {
		s.consumer.log.Warn("failed to ack message",
			"queue", s.queueName,
			"delivery_tag", delivery.DeliveryTag,
			"error", err,
		)
	}
}

func (s *subscription) nack(delivery amqp.Delivery, requeue bool) {
	if s.opts.AutoAck {
		return
	}
	if err := delivery.Nack(false, requeue); err != nil && !isClosedError(err) {
		s.consumer.log.Warn("failed to nack message",
			"queue", s.queueName,
			"delivery_tag", delivery.DeliveryTag,
			"requeue", requeue,
			"error", err,
		)
	}
}

func (s *subscription) closeChannel(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}
	if err := ch.Close(); err != nil && !isClosedError(err) {
		s.consumer.log.Warn("failed to close channel", "queue", s.queueName, "error", err)
	}
}

// Close stops all subscriptions and waits for handlers to finish.
func (c *Consumer) Close() error {
	if c == nil {
		return ErrConsumerClosed
	}
	return c.CloseContext(context.Background())
}

// CloseContext stops all subscriptions and waits for handlers or the context deadline.
func (c *Consumer) CloseContext(ctx context.Context) error {
	if c == nil {
		return ErrConsumerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrAlreadyClosed
	}
	c.closed = true
	c.cancel()
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
