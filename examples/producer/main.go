package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/dinushc/rabbitmq/client"
)

func main() {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		log.Fatal("RABBITMQ_URL is required")
	}

	conn, err := client.New(client.Config{
		URL:  url,
		Name: "example-producer",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	publisher := client.NewPublisher(conn, client.DefaultPublisherConfig())
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := map[string]any{
		"id":        "example-1",
		"message":   "hello from rabbitmq-client",
		"createdAt": time.Now().UTC(),
	}
	if err := publisher.PublishJSON(ctx, "example.events", "example.created", event); err != nil {
		log.Fatal(err)
	}

	log.Println("message published")
}
