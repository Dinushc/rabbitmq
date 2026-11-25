package client

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message is a broker delivery passed to a Handler.
type Message struct {
	Body            []byte
	Exchange        string
	RoutingKey      string
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	MessageID       string
	Timestamp       time.Time
	Type            string
	UserID          string
	AppID           string
	ConsumerTag     string
	DeliveryTag     uint64
	Redelivered     bool
	Headers         amqp.Table
}

func messageFromDelivery(d amqp.Delivery) Message {
	return Message{
		Body:            d.Body,
		Exchange:        d.Exchange,
		RoutingKey:      d.RoutingKey,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageID:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserID:          d.UserId,
		AppID:           d.AppId,
		ConsumerTag:     d.ConsumerTag,
		DeliveryTag:     d.DeliveryTag,
		Redelivered:     d.Redelivered,
		Headers:         d.Headers,
	}
}
