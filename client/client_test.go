package client

import (
	"context"
	"testing"
)

func TestNilPublisherFailsGracefully(t *testing.T) {
	t.Parallel()

	publisher := NewPublisher(nil, DefaultPublisherConfig())
	if err := publisher.Publish(context.Background(), "events", "created", []byte("{}")); err == nil {
		t.Fatal("expected an error from a publisher without a connection")
	}
}

func TestNilConsumerFailsGracefully(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer(nil, "test")
	err := consumer.Subscribe("queue", "exchange", "key", func(context.Context, Message) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected an error from a consumer without a connection")
	}
}

func TestSubscribeValidatesArguments(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer(nil, "test")
	if err := consumer.Subscribe("", "exchange", "key", func(context.Context, Message) error { return nil }); err != ErrEmptyQueueName {
		t.Fatalf("error = %v, want %v", err, ErrEmptyQueueName)
	}
	if err := consumer.Subscribe("queue", "", "key", func(context.Context, Message) error { return nil }); err != ErrEmptyExchangeName {
		t.Fatalf("error = %v, want %v", err, ErrEmptyExchangeName)
	}
	if err := consumer.Subscribe("queue", "exchange", "key", nil); err != ErrNilHandler {
		t.Fatalf("error = %v, want %v", err, ErrNilHandler)
	}
}

func TestConsumerCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer(nil, "test")
	if err := consumer.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := consumer.Close(); err != ErrAlreadyClosed {
		t.Fatalf("second Close() error = %v, want %v", err, ErrAlreadyClosed)
	}
}
