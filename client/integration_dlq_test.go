package client_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dinushc/rabbitmq/client"
)

func TestDLQIntegration(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL is not set")
	}

	conn, err := client.New(client.Config{
		URL:  url,
		Name: "rabbitmq-client-dlq-integration",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	exchange := "integration.dlq.events." + suffix
	queue := "integration.dlq.queue." + suffix
	routingKey := "integration.dlq.created"

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

	consumer := client.NewConsumer(conn, "integration-dlq-consumer")
	t.Cleanup(func() { _ = consumer.Close() })

	err = consumer.SubscribeWithOpts(
		queue,
		exchange,
		routingKey,
		func(ctx context.Context, msg client.Message) error {
			return errors.New("handler failed")
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

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	for {
		msg, ok, err := ch.Get(queue+".dlq", true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if string(msg.Body) != string(body) {
				t.Fatalf("dlq body = %q, want %q", msg.Body, body)
			}
			return
		}

		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the dead-lettered message")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
