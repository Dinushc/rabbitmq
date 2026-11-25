package payment

const (
	ExchangeEvents = "payment.events"
)

const (
	RoutingKeyDepositCompleted    = "deposit.completed"
	RoutingKeyDepositFailed       = "deposit.failed"
	RoutingKeyWithdrawalRequested = "withdrawal.requested"
	RoutingKeyWithdrawalCompleted = "withdrawal.completed"
	RoutingKeyWithdrawalRejected  = "withdrawal.rejected"
	RoutingKeyChargebackOpened    = "chargeback.opened"
)
