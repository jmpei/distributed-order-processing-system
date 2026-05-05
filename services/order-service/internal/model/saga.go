package model

import "time"

const (
	SagaStepReservingInventory = "RESERVING_INVENTORY"
	SagaStepProcessingPayment  = "PROCESSING_PAYMENT"
	SagaStepReleasingInventory = "RELEASING_INVENTORY"
	SagaStepDone               = "DONE"

	SagaStatusInProgress  = "IN_PROGRESS"
	SagaStatusCompleted   = "COMPLETED"
	SagaStatusFailed      = "FAILED"
	SagaStatusCompensating = "COMPENSATING"
	SagaStatusCompensated = "COMPENSATED"
)

type SagaState struct {
	SagaID      string    `gorm:"primaryKey;type:varchar(64)"   json:"saga_id"`
	OrderID     uint64    `gorm:"not null;uniqueIndex"           json:"order_id"`
	CurrentStep string    `gorm:"type:varchar(32);not null"      json:"current_step"`
	Status      string    `gorm:"type:varchar(32);not null"      json:"status"`
	LastEventID string    `gorm:"type:varchar(64)"               json:"last_event_id"`
	RetryCount  int       `gorm:"not null;default:0"             json:"retry_count"`
	LastError   string    `gorm:"type:varchar(500)"              json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ProcessedEvent struct {
	EventID    string    `gorm:"primaryKey;type:varchar(64)" json:"event_id"`
	ConsumedAt time.Time `gorm:"not null"                    json:"consumed_at"`
}
