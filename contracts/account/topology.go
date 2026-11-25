package account

const (
	ExchangeEvents = "account.events"
)

const (
	RoutingKeyBalanceChanged = "balance.changed"

	RoutingKeyBetPlaced   = "bet.placed"
	RoutingKeyWinPaid     = "win.paid"
	RoutingKeyBetRefunded = "bet.refunded"

	RoutingKeyDepositCredited    = "deposit.credited"
	RoutingKeyWithdrawalDeducted = "withdrawal.deducted"
	RoutingKeyWithdrawalRefunded = "withdrawal.refunded"

	RoutingKeyBonusCredited = "bonus.credited"
)
