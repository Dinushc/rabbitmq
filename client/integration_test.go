package client_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dinushc/rabbitmq/client"
)

func TestPublishSubscribeIntegration(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL is not set")
	}

	conn, err := client.New(client.Config{
		URL:  url,
		Name: "rabbitmq-client-integration",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	exchange := "integration.events." + suffix
	queue := "integration.queue." + suffix
	routingKey := "integration.created"

	t.Cleanup(func() {
		ch, err := conn.NewChannel()
		if err != nil {
			return
		}
		defer ch.Close()

		_, _ = ch.QueueDelete(queue, false, false, false)
		_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
		_ = ch.ExchangeDelete(exchange, false, false)
		_ = ch.ExchangeDelete(queue+".dlq.exchange", false, false)
	})

	publisher := client.NewPublisher(conn, client.DefaultPublisherConfig())
	t.Cleanup(func() { _ = publisher.Close() })

	consumer := client.NewConsumer(conn, "integration-consumer")
	t.Cleanup(func() { _ = consumer.Close() })

	received := make(chan client.Message, 1)
	err = consumer.SubscribeWithOpts(
		queue,
		exchange,
		routingKey,
		func(ctx context.Context, msg client.Message) error {
			received <- msg
			return nil
		},
		client.DefaultSubscribeOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body := []byte(fmt.Sprintf(`{"id":%q}`, suffix))
	if err := publisher.Publish(ctx, exchange, routingKey, body); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if string(msg.Body) != string(body) {
			t.Fatalf("body = %q, want %q", msg.Body, body)
		}
		if msg.RoutingKey != routingKey {
			t.Fatalf("routing key = %q, want %q", msg.RoutingKey, routingKey)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the message")
	}
}
