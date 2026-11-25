package client

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMessageFromDelivery(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	delivery := amqp.Delivery{
		Body:            []byte("payload"),
		Exchange:        "events",
		RoutingKey:      "created",
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp.Persistent,
		Priority:        3,
		CorrelationId:   "correlation",
		ReplyTo:         "reply",
		MessageId:       "message",
		Timestamp:       now,
		Type:            "created",
		UserId:          "user",
		AppId:           "app",
		ConsumerTag:     "consumer",
		DeliveryTag:     42,
		Redelivered:     true,
		Headers:         amqp.Table{"x-key": "value"},
	}

	msg := messageFromDelivery(delivery)
	if string(msg.Body) != "payload" || msg.Exchange != "events" || msg.RoutingKey != "created" {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if msg.DeliveryTag != 42 || !msg.Redelivered || msg.CorrelationID != "correlation" {
		t.Fatalf("unexpected delivery metadata: %+v", msg)
	}
	if msg.Headers["x-key"] != "value" {
		t.Fatalf("headers were not copied: %+v", msg.Headers)
	}
}
