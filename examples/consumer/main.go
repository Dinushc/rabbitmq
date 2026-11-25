package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dinushc/rabbitmq/client"
)

func main() {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		log.Fatal("RABBITMQ_URL is required")
	}

	conn, err := client.New(client.Config{
		URL:  url,
		Name: "example-consumer",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	consumer := client.NewConsumer(conn, "example-consumer")
	defer consumer.Close()

	err = consumer.SubscribeWithOpts(
		"example.queue",
		"example.events",
		"example.created",
		func(ctx context.Context, msg client.Message) error {
			log.Printf("received %s: %s", msg.RoutingKey, msg.Body)
			return nil
		},
		client.DefaultSubscribeOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Println("shutting down")
}
