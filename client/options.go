package client

import (
	"crypto/tls"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config configures a Connection.
type Config struct {
	// URL is the AMQP connection URL, for example amqp://guest:guest@localhost:5672/.
	URL string
	// Name is advertised to the broker as the connection name.
	Name string
	// Heartbeat is the AMQP heartbeat interval. Zero uses the broker default.
	Heartbeat time.Duration
	// DialTimeout bounds the initial TCP dial and every reconnect attempt.
	DialTimeout time.Duration
	// InitialBackoff is the delay before the first reconnect attempt.
	InitialBackoff time.Duration
	// MaxBackoff caps reconnect delays.
	MaxBackoff time.Duration
	// BackoffFactor multiplies the delay after every failed attempt.
	BackoffFactor float64
	// Jitter randomizes reconnect delays in the range [0, 1].
	Jitter float64
	// TLSConfig is used for amqps connections. It may be omitted.
	TLSConfig *tls.Config
	// Properties is an optional set of AMQP client properties.
	Properties amqp.Table
	// Logger receives library logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// DefaultConfig returns a production-oriented connection configuration.
func DefaultConfig() Config {
	return Config{
		Name:           "rabbitmq-client",
		Heartbeat:      10 * time.Second,
		DialTimeout:    10 * time.Second,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2,
		Jitter:         0.2,
	}
}

func (c Config) withDefaults() Config {
	if c.Name == "" {
		c.Name = "rabbitmq-client"
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 10 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.MaxBackoff < c.InitialBackoff {
		c.MaxBackoff = c.InitialBackoff
	}
	if c.BackoffFactor < 1 {
		c.BackoffFactor = 2
	}
	if c.Jitter < 0 {
		c.Jitter = 0
	}
	if c.Jitter > 1 {
		c.Jitter = 1
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Properties != nil {
		properties := make(amqp.Table, len(c.Properties))
		for key, value := range c.Properties {
			properties[key] = value
		}
		c.Properties = properties
	}
	return c
}

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// PublishTimeout is the maximum wait for a publisher confirmation.
	PublishTimeout time.Duration
	// MaxRetries is the total number of publish attempts, including the first one.
	MaxRetries int
	// RetryDelay is the delay after the first failed attempt.
	RetryDelay time.Duration
	// MaxRetryDelay caps retry delays.
	MaxRetryDelay time.Duration
	// RetryFactor multiplies the retry delay after every failed attempt.
	RetryFactor float64
	// RetryJitter randomizes retry delays in the range [0, 1].
	RetryJitter float64
	// ExchangeType is the default exchange type used for implicit declarations.
	ExchangeType string
	// Mandatory asks the broker to return unroutable messages.
	Mandatory bool
	// Immediate asks the broker to return messages with no ready consumers.
	Immediate bool
	// ContentType is the default message content type.
	ContentType string
	// Persistent publishes messages with delivery mode 2.
	Persistent bool
	// RetryOnTimeout retries publishes whose confirmation timed out.
	// The message may already be on the broker, so consumers must be idempotent.
	RetryOnTimeout bool
}

// DefaultPublisherConfig returns a production-oriented publisher configuration.
func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		PublishTimeout: 5 * time.Second,
		MaxRetries:     5,
		RetryDelay:     100 * time.Millisecond,
		MaxRetryDelay:  2 * time.Second,
		RetryFactor:    2,
		RetryJitter:    0.2,
		ExchangeType:   "topic",
		ContentType:    "application/json",
		Persistent:     true,
		RetryOnTimeout: true,
	}
}

func (c PublisherConfig) withDefaults() PublisherConfig {
	if c == (PublisherConfig{}) {
		return DefaultPublisherConfig()
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 5 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 100 * time.Millisecond
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = 2 * time.Second
	}
	if c.MaxRetryDelay < c.RetryDelay {
		c.MaxRetryDelay = c.RetryDelay
	}
	if c.RetryFactor < 1 {
		c.RetryFactor = 2
	}
	if c.RetryJitter < 0 {
		c.RetryJitter = 0
	}
	if c.RetryJitter > 1 {
		c.RetryJitter = 1
	}
	if c.ExchangeType == "" {
		c.ExchangeType = "topic"
	}
	if c.ContentType == "" {
		c.ContentType = "application/json"
	}
	return c
}

// PublishOptions overrides per-message publishing settings.
type PublishOptions struct {
	// ExchangeType overrides the publisher default for this message.
	ExchangeType string
	// Mandatory overrides the publisher default when non-nil.
	Mandatory *bool
	// Immediate overrides the publisher default when non-nil.
	Immediate *bool
	// ContentType overrides the publisher default when non-empty.
	ContentType string
	// ContentEncoding is the message content encoding.
	ContentEncoding string
	// Persistent overrides the publisher default when non-nil.
	Persistent *bool
	// Priority is the message priority.
	Priority uint8
	// CorrelationID correlates requests and replies.
	CorrelationID string
	// ReplyTo names the reply queue.
	ReplyTo string
	// MessageID is a unique message identifier.
	MessageID string
	// Timestamp is the message timestamp. Zero uses the current UTC time.
	Timestamp time.Time
	// Type is an application-specific message type.
	Type string
	// UserID is the authenticated user identifier.
	UserID string
	// AppID identifies the publishing application.
	AppID string
	// Headers contains optional AMQP headers.
	Headers amqp.Table
	// Timeout overrides PublisherConfig.PublishTimeout for this message.
	Timeout time.Duration
}

// DLQOptions configures dead-letter topology. Empty values are derived from the queue name.
type DLQOptions struct {
	Exchange   string
	Queue      string
	RoutingKey string
}

func (d DLQOptions) names(queue string) (exchange, queueName, routingKey string) {
	exchange = d.Exchange
	if exchange == "" {
		exchange = queue + ".dlq.exchange"
	}
	queueName = d.Queue
	if queueName == "" {
		queueName = queue + ".dlq"
	}
	routingKey = d.RoutingKey
	if routingKey == "" {
		routingKey = queue + ".dlq"
	}
	return exchange, queueName, routingKey
}

// SubscribeOptions configures one consumer subscription.
type SubscribeOptions struct {
	// ExchangeType is the exchange type. Defaults to "topic".
	ExchangeType string
	// ExchangeDurable declares a durable exchange.
	ExchangeDurable bool
	// QueueDurable declares a durable queue.
	QueueDurable bool
	// AutoDelete deletes the queue when the last consumer disconnects.
	AutoDelete bool
	// Exclusive restricts the queue to this connection.
	Exclusive bool
	// NoLocal is passed to basic.consume.
	NoLocal bool
	// NoWait skips broker confirmations for declarations and bindings.
	NoWait bool
	// AutoAck acknowledges messages before the handler runs.
	AutoAck bool
	// Prefetch limits unacknowledged deliveries per subscription.
	Prefetch int
	// Concurrency is the maximum number of handlers running in parallel.
	Concurrency int
	// HandlerTimeout bounds handler execution. Zero uses 30s, a negative value disables it.
	HandlerTimeout time.Duration
	// QueueArguments adds custom queue arguments.
	QueueArguments amqp.Table
	// ConsumerTag overrides the generated consumer tag.
	ConsumerTag string
	// RequeueOnError requeues messages when the handler returns an error.
	// By default messages are dead-lettered instead.
	RequeueOnError bool
	// DisableDLQ turns off dead-letter topology and argument injection.
	DisableDLQ bool
	// DLQ customizes dead-letter names.
	DLQ DLQOptions
}

// DefaultSubscribeOptions returns production-oriented subscription options.
func DefaultSubscribeOptions() SubscribeOptions {
	return SubscribeOptions{
		ExchangeType:    "topic",
		ExchangeDurable: true,
		QueueDurable:    true,
		Prefetch:        10,
		Concurrency:     10,
		HandlerTimeout:  30 * time.Second,
	}
}

func (o SubscribeOptions) withDefaults() SubscribeOptions {
	empty := o.ExchangeType == "" &&
		!o.ExchangeDurable &&
		!o.QueueDurable &&
		!o.AutoDelete &&
		!o.Exclusive &&
		!o.NoLocal &&
		!o.NoWait &&
		!o.AutoAck &&
		o.Prefetch == 0 &&
		o.Concurrency == 0 &&
		o.HandlerTimeout == 0 &&
		o.QueueArguments == nil &&
		o.ConsumerTag == "" &&
		!o.RequeueOnError &&
		!o.DisableDLQ &&
		o.DLQ == (DLQOptions{})

	if empty {
		return DefaultSubscribeOptions()
	}
	if o.ExchangeType == "" {
		o.ExchangeType = "topic"
	}
	if o.Prefetch <= 0 {
		o.Prefetch = 10
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 10
	}
	if o.HandlerTimeout == 0 {
		o.HandlerTimeout = 30 * time.Second
	}
	return o
}

// ConsumerOption customizes a Consumer.
type ConsumerOption func(*Consumer)

// WithConsumerLogger sets the consumer logger.
func WithConsumerLogger(logger *slog.Logger) ConsumerOption {
	return func(c *Consumer) {
		if logger != nil {
			c.log = logger
		}
	}
}

// PublisherOption customizes a Publisher.
type PublisherOption func(*Publisher)

// WithPublisherLogger sets the publisher logger.
func WithPublisherLogger(logger *slog.Logger) PublisherOption {
	return func(p *Publisher) {
		if logger != nil {
			p.log = logger
		}
	}
}

func boolValue(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}
