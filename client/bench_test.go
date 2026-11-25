package client

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func BenchmarkBackoffNext(b *testing.B) {
	policy := newBackoff(100*time.Millisecond, 30*time.Second, 2, 0.2)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = policy.next(10)
	}
}

func BenchmarkMessageFromDelivery(b *testing.B) {
	delivery := amqp.Delivery{
		Body:        []byte(`{"id":"benchmark"}`),
		Exchange:    "events",
		RoutingKey:  "created",
		ContentType: "application/json",
		Headers:     amqp.Table{"x-key": "value"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = messageFromDelivery(delivery)
	}
}
