package client

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrConnectionClosed is returned when the underlying connection is closed.
	ErrConnectionClosed = errors.New("rabbitmq: connection closed")
	// ErrAlreadyClosed is returned when Close is called more than once.
	ErrAlreadyClosed = errors.New("rabbitmq: already closed")
	// ErrPublisherClosed is returned when a publisher is used after Close.
	ErrPublisherClosed = errors.New("rabbitmq: publisher closed")
	// ErrConsumerClosed is returned when a consumer is used after Close.
	ErrConsumerClosed = errors.New("rabbitmq: consumer closed")
	// ErrChannelClosed is returned when the AMQP channel is closed.
	ErrChannelClosed = errors.New("rabbitmq: channel closed")
	// ErrPublishTimeout is returned when a publisher confirmation is not received in time.
	ErrPublishTimeout = errors.New("rabbitmq: publish confirmation timeout")
	// ErrPublishNack is returned when the broker negatively acknowledges a publish.
	ErrPublishNack = errors.New("rabbitmq: publish nack received")
	// ErrConfirmsNotEnabled is returned when publisher confirms are not active.
	ErrConfirmsNotEnabled = errors.New("rabbitmq: publisher confirms are not enabled")
	// ErrEmptyQueueName is returned when an empty queue name is supplied.
	ErrEmptyQueueName = errors.New("rabbitmq: queue name must not be empty")
	// ErrEmptyExchangeName is returned when an empty exchange name is supplied.
	ErrEmptyExchangeName = errors.New("rabbitmq: exchange name must not be empty")
	// ErrNilHandler is returned when a nil handler is supplied.
	ErrNilHandler = errors.New("rabbitmq: handler must not be nil")
	// ErrRequeue can be returned by a handler to requeue the message.
	ErrRequeue = errors.New("rabbitmq: requeue message")
	// ErrDiscard can be returned by a handler to acknowledge and drop the message.
	ErrDiscard = errors.New("rabbitmq: discard message")
)

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrPublisherClosed) || errors.Is(err, ErrConsumerClosed) || errors.Is(err, ErrAlreadyClosed) || errors.Is(err, ErrConfirmsNotEnabled) {
		return false
	}

	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case amqp.AccessRefused, amqp.NotFound, amqp.ResourceLocked, amqp.PreconditionFailed, amqp.NotAllowed:
			return false
		}
	}

	return true
}

func isClosedError(err error) bool {
	return errors.Is(err, amqp.ErrClosed) || errors.Is(err, ErrChannelClosed)
}
