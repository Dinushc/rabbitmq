package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher publishes messages with publisher confirms and automatic reconnects.
type Publisher struct {
	conn *Connection
	cfg  PublisherConfig
	log  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	ch       *amqp.Channel
	declared map[string]struct{}
	closed   bool

	chMu    sync.Mutex
	backoff *backoffPolicy
}

// NewPublisher creates a publisher. A dedicated channel is opened lazily on first use.
func NewPublisher(conn *Connection, cfg PublisherConfig, opts ...PublisherOption) *Publisher {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.Default()
	if conn != nil && conn.cfg.Logger != nil {
		logger = conn.cfg.Logger
	}

	p := &Publisher{
		conn:     conn,
		cfg:      cfg,
		log:      logger.With("component", "rabbitmq.publisher"),
		ctx:      ctx,
		cancel:   cancel,
		declared: make(map[string]struct{}),
		backoff:  newBackoff(cfg.RetryDelay, cfg.MaxRetryDelay, cfg.RetryFactor, cfg.RetryJitter),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish sends body with the publisher's default options.
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	return p.PublishWithOptions(ctx, exchange, routingKey, body, PublishOptions{})
}

// PublishJSON marshals payload as JSON and publishes it.
func (p *Publisher) PublishJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("rabbitmq: marshal payload: %w", err)
	}
	return p.PublishWithOptions(ctx, exchange, routingKey, body, PublishOptions{
		ContentType: "application/json",
	})
}

// PublishWithOptions publishes body with per-message overrides.
func (p *Publisher) PublishWithOptions(ctx context.Context, exchange, routingKey string, body []byte, opts PublishOptions) error {
	if p == nil || p.conn == nil {
		return errors.New("rabbitmq: publisher is not initialized")
	}
	if exchange == "" {
		return ErrEmptyExchangeName
	}
	if ctx == nil {
		ctx = context.Background()
	}

	opts = p.cfg.apply(opts)

	var lastErr error
	for attempt := 1; attempt <= p.cfg.MaxRetries; attempt++ {
		if p.isClosed() {
			return ErrPublisherClosed
		}

		err := p.publishOnce(ctx, exchange, routingKey, body, opts)
		if err == nil {
			return nil
		}
		lastErr = err

		if errors.Is(err, ErrPublishTimeout) && !p.cfg.RetryOnTimeout {
			return err
		}
		if !isRetryable(err) {
			return err
		}
		if attempt == p.cfg.MaxRetries {
			break
		}

		delay := p.backoff.next(attempt)
		p.log.Warn("publish failed; retrying",
			"exchange", exchange,
			"routing_key", routingKey,
			"attempt", attempt,
			"delay", delay,
			"error", err,
		)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-p.ctx.Done():
			timer.Stop()
			return ErrPublisherClosed
		case <-timer.C:
		}
	}

	return lastErr
}

func (p *Publisher) publishOnce(ctx context.Context, exchange, routingKey string, body []byte, opts PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.ctx.Err() != nil {
		return ErrPublisherClosed
	}

	ch, err := p.channel(ctx)
	if err != nil {
		return err
	}

	if err := p.declareExchange(ch, exchange, opts.ExchangeType); err != nil {
		p.invalidate(ch)
		return err
	}

	publishing := amqp.Publishing{
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		DeliveryMode:    deliveryMode(boolValue(opts.Persistent, p.cfg.Persistent && !p.cfg.Transient)),
		Priority:        opts.Priority,
		CorrelationId:   opts.CorrelationID,
		ReplyTo:         opts.ReplyTo,
		MessageId:       opts.MessageID,
		Timestamp:       opts.Timestamp,
		Type:            opts.Type,
		UserId:          opts.UserID,
		AppId:           opts.AppID,
		Headers:         opts.Headers,
		Body:            body,
	}
	if publishing.Timestamp.IsZero() {
		publishing.Timestamp = time.Now().UTC()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		boolValue(opts.Mandatory, p.cfg.Mandatory),
		boolValue(opts.Immediate, p.cfg.Immediate),
		publishing,
	)
	if err != nil {
		p.invalidate(ch)
		return fmt.Errorf("rabbitmq: publish to %q: %w", exchange, err)
	}
	if confirmation == nil {
		p.invalidate(ch)
		return ErrConfirmsNotEnabled
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = p.cfg.PublishTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	acked, err := confirmation.WaitContext(waitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrPublishTimeout
		}
		return err
	}
	if !acked {
		return ErrPublishNack
	}
	return nil
}

func (p *Publisher) channel(ctx context.Context) (*amqp.Channel, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPublisherClosed
	}
	ch := p.ch
	p.mu.RUnlock()
	if ch != nil && !ch.IsClosed() {
		return ch, nil
	}

	p.chMu.Lock()
	defer p.chMu.Unlock()

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPublisherClosed
	}
	ch = p.ch
	p.mu.RUnlock()
	if ch != nil && !ch.IsClosed() {
		return ch, nil
	}

	opCtx, cancel := mergeContext(ctx, p.ctx)
	defer cancel()

	ch, err := p.conn.Channel(opCtx)
	if err != nil {
		if p.ctx.Err() != nil {
			return nil, ErrPublisherClosed
		}
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = ch.Close()
		return nil, ErrPublisherClosed
	}
	p.ch = ch
	p.declared = make(map[string]struct{})
	p.mu.Unlock()

	return ch, nil
}

func (p *Publisher) declareExchange(ch *amqp.Channel, exchange, exchangeType string) error {
	key := exchange + "\x00" + exchangeType

	p.mu.RLock()
	_, declared := p.declared[key]
	sameChannel := p.ch == ch
	p.mu.RUnlock()
	if declared && sameChannel {
		return nil
	}

	if err := ch.ExchangeDeclare(exchange, exchangeType, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq: declare exchange %q as %q: %w", exchange, exchangeType, err)
	}

	p.mu.Lock()
	if p.ch == ch {
		p.declared[key] = struct{}{}
	}
	p.mu.Unlock()
	return nil
}

func (p *Publisher) invalidate(ch *amqp.Channel) {
	p.mu.Lock()
	if p.ch == ch {
		p.ch = nil
		p.declared = make(map[string]struct{})
	}
	p.mu.Unlock()

	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}
}

// DeclareExchange declares an exchange and caches the result for this publisher.
func (p *Publisher) DeclareExchange(ctx context.Context, exchange, exchangeType string) error {
	if p == nil || p.conn == nil {
		return errors.New("rabbitmq: publisher is not initialized")
	}
	if exchange == "" {
		return ErrEmptyExchangeName
	}
	if exchangeType == "" {
		exchangeType = p.cfg.ExchangeType
	}
	ch, err := p.channel(ctx)
	if err != nil {
		return err
	}
	return p.declareExchange(ch, exchange, exchangeType)
}

func (p *Publisher) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// Close stops the publisher, fails pending publishes and closes its channel.
func (p *Publisher) Close() error {
	if p == nil {
		return ErrPublisherClosed
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	p.closed = true
	ch := p.ch
	p.ch = nil
	p.mu.Unlock()

	p.cancel()

	if ch != nil && !ch.IsClosed() {
		return ch.Close()
	}
	return nil
}

func (c PublisherConfig) apply(opts PublishOptions) PublishOptions {
	if opts.ExchangeType == "" {
		opts.ExchangeType = c.ExchangeType
	}
	if opts.ContentType == "" {
		opts.ContentType = c.ContentType
	}
	if opts.Mandatory == nil {
		value := c.Mandatory
		opts.Mandatory = &value
	}
	if opts.Immediate == nil {
		value := c.Immediate
		opts.Immediate = &value
	}
	if opts.Persistent == nil {
		value := c.Persistent
		opts.Persistent = &value
	}
	return opts
}

func deliveryMode(persistent bool) uint8 {
	if persistent {
		return amqp.Persistent
	}
	return amqp.Transient
}

func mergeContext(parent, other context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(other, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
