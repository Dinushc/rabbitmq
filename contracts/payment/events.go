// Package payment contains RabbitMQ event contracts for the payment domain.
package payment

import (
	"time"

	"github.com/google/uuid"
)

// DepositCompletedEvent is published when a deposit provider confirms a payment.
type DepositCompletedEvent struct {
	TransactionID  uuid.UUID `json:"transactionId"`
	ProviderTxID   string    `json:"providerTxId"`
	UserID         uuid.UUID `json:"userId"`
	PaymentSysCode string    `json:"paymentSysCode"`
	ProviderName   string    `json:"providerName"`
	Amount         string    `json:"amount"`
	Currency       string    `json:"currency"`
	Timestamp      time.Time `json:"timestamp"`
}

// DepositFailedEvent is published when a deposit fails.
type DepositFailedEvent struct {
	TransactionID  uuid.UUID `json:"transactionId"`
	UserID         uuid.UUID `json:"userId"`
	PaymentSysCode string    `json:"paymentSysCode"`
	Amount         string    `json:"amount"`
	Currency       string    `json:"currency"`
	Reason         string    `json:"reason"`
	ErrorCode      string    `json:"errorCode"`
	Timestamp      time.Time `json:"timestamp"`
}

// WithdrawalRequestedEvent is published when a withdrawal is requested.
type WithdrawalRequestedEvent struct {
	TransactionID  uuid.UUID `json:"transactionId"`
	UserID         uuid.UUID `json:"userId"`
	PaymentSysCode string    `json:"paymentSysCode"`
	Amount         string    `json:"amount"`
	Currency       string    `json:"currency"`
	Destination    string    `json:"destination,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// WithdrawalCompletedEvent is published when a withdrawal is completed.
type WithdrawalCompletedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	ProviderTxID  string    `json:"providerTxId"`
	UserID        uuid.UUID `json:"userId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}

// WithdrawalRejectedEvent is published when a withdrawal is rejected.
type WithdrawalRejectedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	RejectedBy    string    `json:"rejectedBy"`
	Reason        string    `json:"reason"`
	Timestamp     time.Time `json:"timestamp"`
}

// ChargebackOpenedEvent is published when a chargeback is opened.
type ChargebackOpenedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	ReasonCode    string    `json:"reasonCode"`
	Timestamp     time.Time `json:"timestamp"`
}
