package shared

// Exchange names
const (
	ExchangeCommands = "saga.commands"
	ExchangeEvents   = "saga.events"
)

// Queue names
const (
	QueueInventoryCommands = "inventory.commands"
	QueuePaymentCommands   = "payment.commands"
	QueueOrderEvents       = "order.events"
)

// Routing keys
const (
	RoutingKeyInventoryReserve  = "inventory.reserve"
	RoutingKeyInventoryRelease  = "inventory.release"
	RoutingKeyPaymentProcess    = "payment.process"
	RoutingKeyPaymentRefund     = "payment.refund"
	RoutingKeyInventoryReserved = "inventory.reserved"
	RoutingKeyInventoryReleased = "inventory.released"
	RoutingKeyPaymentProcessed  = "payment.processed"
)

// ReserveInventoryCmd is published to saga.commands with routing key inventory.reserve
type ReserveInventoryCmd struct {
	SagaID    string `json:"saga_id"`
	OrderID   uint64 `json:"order_id"`
	ProductID uint64 `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// InventoryReservedEvent is published to saga.events with routing key inventory.reserved
type InventoryReservedEvent struct {
	SagaID    string `json:"saga_id"`
	OrderID   uint64 `json:"order_id"`
	ProductID uint64 `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Success   bool   `json:"success"`
	Reason    string `json:"reason,omitempty"`
}

// ProcessPaymentCmd is published to saga.commands with routing key payment.process
type ProcessPaymentCmd struct {
	SagaID  string  `json:"saga_id"`
	OrderID uint64  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

// PaymentProcessedEvent is published to saga.events with routing key payment.processed
type PaymentProcessedEvent struct {
	SagaID        string `json:"saga_id"`
	OrderID       uint64 `json:"order_id"`
	TransactionID string `json:"transaction_id"`
	Success       bool   `json:"success"`
	Reason        string `json:"reason,omitempty"`
}

// ReleaseInventoryCmd is published to saga.commands with routing key inventory.release
type ReleaseInventoryCmd struct {
	SagaID    string `json:"saga_id"`
	OrderID   uint64 `json:"order_id"`
	ProductID uint64 `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// InventoryReleasedEvent is published to saga.events with routing key inventory.released
type InventoryReleasedEvent struct {
	SagaID    string `json:"saga_id"`
	OrderID   uint64 `json:"order_id"`
	ProductID uint64 `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Success   bool   `json:"success"`
	Reason    string `json:"reason,omitempty"`
}
