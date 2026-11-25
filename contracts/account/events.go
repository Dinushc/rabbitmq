// Package account contains RabbitMQ event contracts for the account domain.
package account

import (
	"time"

	"github.com/google/uuid"
)

// BalanceChangedEvent is published when a wallet balance changes.
type BalanceChangedEvent struct {
	UserID        uuid.UUID `json:"userId"`
	WalletID      uuid.UUID `json:"walletId"`
	CurrencyCode  string    `json:"currencyCode"`
	OldBalance    string    `json:"oldBalance"`
	NewBalance    string    `json:"newBalance"`
	ChangeAmount  string    `json:"changeAmount"`
	Reason        string    `json:"reason"`
	TransactionID uuid.UUID `json:"transactionId,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
}

// BetPlacedEvent is published when a bet is placed.
type BetPlacedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	GameID        string    `json:"gameId"`
	ProviderID    string    `json:"providerId"`
	SessionID     string    `json:"sessionId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}

// WinPaidEvent is published when a win is paid.
type WinPaidEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	GameID        string    `json:"gameId"`
	ProviderID    string    `json:"providerId"`
	SessionID     string    `json:"sessionId"`
	BetID         uuid.UUID `json:"betId,omitempty"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}

// BetRefundedEvent is published when a bet is refunded.
type BetRefundedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	GameID        string    `json:"gameId"`
	OriginalBetID uuid.UUID `json:"originalBetId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Reason        string    `json:"reason"`
	Timestamp     time.Time `json:"timestamp"`
}

// DepositCreditedEvent is published when a deposit is credited to a wallet.
type DepositCreditedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	PaymentSysID  uuid.UUID `json:"paymentSysId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}

// WithdrawalDeductedEvent is published when a withdrawal is deducted from a wallet.
type WithdrawalDeductedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	PaymentSysID  uuid.UUID `json:"paymentSysId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}

// WithdrawalRefundedEvent is published when a withdrawal is refunded.
type WithdrawalRefundedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	OriginalWdID  uuid.UUID `json:"originalWithdrawalId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Reason        string    `json:"reason"`
	Timestamp     time.Time `json:"timestamp"`
}

// BonusCreditedEvent is published when a bonus is credited to a wallet.
type BonusCreditedEvent struct {
	TransactionID uuid.UUID `json:"transactionId"`
	UserID        uuid.UUID `json:"userId"`
	CampaignID    uuid.UUID `json:"campaignId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Timestamp     time.Time `json:"timestamp"`
}
