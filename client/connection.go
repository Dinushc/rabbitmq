package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ is kept as an alias for code migrating from the first library version.
type RabbitMQ = Connection

// Connection owns one AMQP connection and reconnects it in the background.
// Channels are not shared: every publisher and every subscription requests its own channel.
type Connection struct {
	cfg Config
	log *slog.Logger

	mu             sync.RWMutex
	conn           *amqp.Connection
	ready          chan struct{}
	closed         chan struct{}
	watcherDone    chan struct{}
	isShuttingDown bool

	backoff *backoffPolicy
}

// New creates a connection and starts its reconnect watcher.
func New(cfg Config) (*Connection, error) {
	cfg = cfg.withDefaults()
	if cfg.URL == "" {
		return nil, errors.New("rabbitmq: URL is required")
	}

	c := &Connection{
		cfg:         cfg,
		log:         cfg.Logger.With("component", "rabbitmq.connection", "connection", cfg.Name),
		ready:       make(chan struct{}),
		closed:      make(chan struct{}),
		watcherDone: make(chan struct{}),
		backoff:     newBackoff(cfg.InitialBackoff, cfg.MaxBackoff, cfg.BackoffFactor, cfg.Jitter),
	}

	conn, err := c.dial()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: connect: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	close(c.ready)
	c.ready = nil
	c.mu.Unlock()

	go c.watch()
	return c, nil
}

// NewRabbitMQ creates a connection from a DSN using default settings.
func NewRabbitMQ(dsn string) (*Connection, error) {
	return New(Config{URL: dsn})
}

// NewRabbitMQWithConfig is an alias for New.
func NewRabbitMQWithConfig(cfg Config) (*Connection, error) {
	return New(cfg)
}

func (c *Connection) dial() (*amqp.Connection, error) {
	props := amqp.NewConnectionProperties()
	if c.cfg.Name != "" {
		props["connection_name"] = c.cfg.Name
	}
	for key, value := range c.cfg.Properties {
		props[key] = value
	}

	dialer := &net.Dialer{
		Timeout:   c.cfg.DialTimeout,
		KeepAlive: 30 * time.Second,
	}

	return amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat:       c.cfg.Heartbeat,
		TLSClientConfig: c.cfg.TLSConfig,
		Properties:      props,
		Dial: func(network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
	})
}

func (c *Connection) watch() {
	defer close(c.watcherDone)

	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil || conn.IsClosed() {
			if !c.reconnect() {
				return
			}
			continue
		}

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-c.closed:
			return
		case err, ok := <-notifyClose:
			if ok && err != nil {
				c.log.Warn("connection closed",
					"code", err.Code,
					"reason", err.Reason,
					"server", err.Server,
				)
			} else {
				c.log.Info("connection closed")
			}

			c.markDisconnected(conn)
			if !c.reconnect() {
				return
			}
		}
	}
}

func (c *Connection) markDisconnected(conn *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != conn {
		return
	}
	c.conn = nil
	if c.ready == nil {
		c.ready = make(chan struct{})
	}
}

func (c *Connection) reconnect() bool {
	for attempt := 1; ; attempt++ {
		delay := c.backoff.next(attempt)
		timer := time.NewTimer(delay)

		select {
		case <-c.closed:
			timer.Stop()
			return false
		case <-timer.C:
		}

		c.mu.RLock()
		shuttingDown := c.isShuttingDown
		c.mu.RUnlock()
		if shuttingDown {
			return false
		}

		c.log.Info("reconnecting", "attempt", attempt, "delay", delay)

		conn, err := c.dial()
		if err != nil {
			c.log.Warn("reconnect failed", "attempt", attempt, "error", err)
			continue
		}

		c.mu.Lock()
		if c.isShuttingDown {
			c.mu.Unlock()
			_ = conn.Close()
			return false
		}
		c.conn = conn
		if c.ready != nil {
			close(c.ready)
			c.ready = nil
		}
		c.mu.Unlock()

		c.log.Info("reconnected")
		return true
	}
}

// Channel waits for an active connection and opens a new dedicated AMQP channel.
func (c *Connection) Channel(ctx context.Context) (*amqp.Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		c.mu.RLock()
		if c.isShuttingDown {
			c.mu.RUnlock()
			return nil, ErrConnectionClosed
		}
		conn := c.conn
		ready := c.ready
		c.mu.RUnlock()

		if conn != nil && !conn.IsClosed() {
			ch, err := conn.Channel()
			if err == nil {
				return ch, nil
			}
			if !errors.Is(err, amqp.ErrClosed) {
				return nil, fmt.Errorf("rabbitmq: open channel: %w", err)
			}
		}

		var readyCh <-chan struct{}
		if ready != nil {
			readyCh = ready
		}

		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-c.closed:
			timer.Stop()
			return nil, ErrConnectionClosed
		case <-readyCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// NewChannel opens a dedicated channel using a five second timeout.
func (c *Connection) NewChannel() (*amqp.Channel, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Channel(ctx)
}

// IsConnected reports whether the underlying AMQP connection is open.
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.conn.IsClosed()
}

// IsHealthy reports whether the connection is usable.
func (c *Connection) IsHealthy() bool {
	return c.IsConnected()
}

// Done is closed when the connection is shut down.
func (c *Connection) Done() <-chan struct{} {
	return c.closed
}

// Close gracefully closes the connection and stops reconnects.
func (c *Connection) Close() error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	if c.isShuttingDown {
		c.mu.Unlock()
		return ErrAlreadyClosed
	}
	c.isShuttingDown = true
	close(c.closed)
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	var err error
	if conn != nil {
		err = conn.Close()
	}

	<-c.watcherDone
	return err
}
