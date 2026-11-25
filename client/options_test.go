package client

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{}.withDefaults()
	if cfg.Name == "" {
		t.Fatal("expected default connection name")
	}
	if cfg.InitialBackoff <= 0 || cfg.MaxBackoff < cfg.InitialBackoff {
		t.Fatalf("invalid backoff defaults: %+v", cfg)
	}
	if cfg.DialTimeout <= 0 || cfg.Heartbeat <= 0 {
		t.Fatalf("invalid timeout defaults: %+v", cfg)
	}
}

func TestDefaultPublisherConfig(t *testing.T) {
	t.Parallel()

	cfg := PublisherConfig{}.withDefaults()
	if cfg.MaxRetries != 5 {
		t.Fatalf("MaxRetries = %d, want 5", cfg.MaxRetries)
	}
	if cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("PublishTimeout = %s, want 5s", cfg.PublishTimeout)
	}
	if cfg.ExchangeType != "topic" || cfg.ContentType != "application/json" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if !cfg.Persistent || !cfg.RetryOnTimeout {
		t.Fatalf("expected persistent publishes and timeout retries by default: %+v", cfg)
	}
}

func TestSubscribeOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts := SubscribeOptions{}.withDefaults()
	if opts.NonDurableExchange || opts.NonDurableQueue {
		t.Fatal("zero options should produce durable topology")
	}
	if opts.Prefetch != 10 || opts.Concurrency != 10 {
		t.Fatalf("unexpected prefetch/concurrency: %+v", opts)
	}
	if opts.HandlerTimeout != 30*time.Second {
		t.Fatalf("HandlerTimeout = %s, want 30s", opts.HandlerTimeout)
	}
	if opts.DisableDLQ {
		t.Fatal("DLQ should be enabled by default")
	}

	partial := SubscribeOptions{Prefetch: 50, Concurrency: 4}.withDefaults()
	if partial.NonDurableQueue || partial.NonDurableExchange {
		t.Fatal("partially filled options should keep durable topology defaults")
	}
	if partial.Prefetch != 50 || partial.Concurrency != 4 {
		t.Fatalf("overrides were not applied: %+v", partial)
	}

	nonDurable := SubscribeOptions{NonDurableQueue: true, NonDurableExchange: true}.withDefaults()
	if !nonDurable.NonDurableQueue || !nonDurable.NonDurableExchange {
		t.Fatalf("non-durable overrides were not applied: %+v", nonDurable)
	}
}

func TestPublisherConfigPartialDefaults(t *testing.T) {
	t.Parallel()

	cfg := PublisherConfig{MaxRetries: 3}.withDefaults()
	if !cfg.Persistent || !cfg.RetryOnTimeout {
		t.Fatalf("partial config should keep persistent and timeout retries: %+v", cfg)
	}
	if cfg.MaxRetries != 3 {
		t.Fatalf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}

	transient := PublisherConfig{Transient: true, DisableTimeoutRetries: true}.withDefaults()
	if transient.Persistent || transient.RetryOnTimeout {
		t.Fatalf("transient overrides were not applied: %+v", transient)
	}
}

func TestDLQNames(t *testing.T) {
	t.Parallel()

	exchange, queue, routingKey := DLQOptions{}.names("orders")
	if exchange != "orders.dlq.exchange" || queue != "orders.dlq" || routingKey != "orders.dlq" {
		t.Fatalf("unexpected DLQ names: %q %q %q", exchange, queue, routingKey)
	}

	exchange, queue, routingKey = DLQOptions{
		Exchange:   "dead",
		Queue:      "dead-orders",
		RoutingKey: "orders.dead",
	}.names("orders")
	if exchange != "dead" || queue != "dead-orders" || routingKey != "orders.dead" {
		t.Fatalf("unexpected custom DLQ names: %q %q %q", exchange, queue, routingKey)
	}
}

func TestApplyPublishOptions(t *testing.T) {
	t.Parallel()

	cfg := DefaultPublisherConfig()
	opts := cfg.apply(PublishOptions{})
	if opts.ExchangeType != "topic" || opts.ContentType != "application/json" {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if !boolValue(opts.Persistent, false) || boolValue(opts.Mandatory, true) {
		t.Fatalf("unexpected boolean defaults: %+v", opts)
	}

	mandatory := true
	persistent := false
	opts = cfg.apply(PublishOptions{
		ExchangeType: "fanout",
		Mandatory:    &mandatory,
		Persistent:   &persistent,
		ContentType:  "text/plain",
	})
	if opts.ExchangeType != "fanout" || opts.ContentType != "text/plain" {
		t.Fatalf("overrides were not applied: %+v", opts)
	}
	if !boolValue(opts.Mandatory, false) || boolValue(opts.Persistent, true) {
		t.Fatalf("boolean overrides were not applied: %+v", opts)
	}
}
