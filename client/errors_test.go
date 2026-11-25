package client

import (
	"context"
	"errors"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: false},
		{name: "publisher closed", err: ErrPublisherClosed, want: false},
		{name: "wrapped publisher closed", err: fmt.Errorf("wrap: %w", ErrPublisherClosed), want: false},
		{name: "amqp closed", err: amqp.ErrClosed, want: true},
		{name: "channel closed", err: ErrChannelClosed, want: true},
		{name: "publish timeout", err: ErrPublishTimeout, want: true},
		{name: "publish nack", err: ErrPublishNack, want: true},
		{name: "precondition failed", err: &amqp.Error{Code: amqp.PreconditionFailed}, want: false},
		{name: "access refused", err: &amqp.Error{Code: amqp.AccessRefused}, want: false},
		{name: "connection forced", err: &amqp.Error{Code: amqp.ConnectionForced}, want: true},
		{name: "network", err: errors.New("network error"), want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetryable(tt.err); got != tt.want {
				t.Fatalf("isRetryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
