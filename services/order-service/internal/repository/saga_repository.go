package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

// SagaRepoIface is the surface SagaOrchestrator depends on; allows mocking.
type SagaRepoIface interface {
	Create(ctx context.Context, s *model.SagaState) error
	GetBySagaID(ctx context.Context, sagaID string) (*model.SagaState, error)
	GetByOrderID(ctx context.Context, orderID uint64) (*model.SagaState, error)
	ListByStatus(ctx context.Context, status string) ([]model.SagaState, error)
	List(ctx context.Context) ([]model.SagaState, error)
	CommitInventoryReserved(ctx context.Context, eventID string, event shared.InventoryReservedEvent) (InventoryReservedOutcome, error)
	CommitPaymentProcessed(ctx context.Context, eventID string, event shared.PaymentProcessedEvent) (PaymentProcessedOutcome, error)
	CommitInventoryReleased(ctx context.Context, eventID string, event shared.InventoryReleasedEvent) (InventoryReleasedOutcome, error)
}

// InventoryReservedOutcome lets the orchestrator decide the next saga step.
type InventoryReservedOutcome struct {
	Skip   bool
	Failed bool
	Amount float64 // populated on success
}

// PaymentProcessedOutcome carries data needed to compose ReleaseInventoryCmd.
type PaymentProcessedOutcome struct {
	Skip       bool
	Compensate bool
	ProductID  uint64
	Quantity   int
}

// InventoryReleasedOutcome marks compensation result.
type InventoryReleasedOutcome struct {
	Skip   bool
	Failed bool
}

type SagaRepository struct {
	db *gorm.DB
}

func NewSagaRepository(db *gorm.DB) *SagaRepository {
	return &SagaRepository{db: db}
}

func (r *SagaRepository) Create(ctx context.Context, s *model.SagaState) error {
	if err := r.db.WithContext(ctx).Create(s).Error; err != nil {
		return fmt.Errorf("create saga_state: %w", err)
	}
	return nil
}

func (r *SagaRepository) GetBySagaID(ctx context.Context, sagaID string) (*model.SagaState, error) {
	var s model.SagaState
	if err := r.db.WithContext(ctx).First(&s, "saga_id = ?", sagaID).Error; err != nil {
		return nil, fmt.Errorf("get saga_state %s: %w", sagaID, err)
	}
	return &s, nil
}

func (r *SagaRepository) GetByOrderID(ctx context.Context, orderID uint64) (*model.SagaState, error) {
	var s model.SagaState
	if err := r.db.WithContext(ctx).First(&s, "order_id = ?", orderID).Error; err != nil {
		return nil, fmt.Errorf("get saga_state order %d: %w", orderID, err)
	}
	return &s, nil
}

func (r *SagaRepository) ListByStatus(ctx context.Context, status string) ([]model.SagaState, error) {
	var states []model.SagaState
	if err := r.db.WithContext(ctx).Where("status = ?", status).Find(&states).Error; err != nil {
		return nil, fmt.Errorf("list saga_state status=%s: %w", status, err)
	}
	return states, nil
}

func (r *SagaRepository) List(ctx context.Context) ([]model.SagaState, error) {
	var states []model.SagaState
	if err := r.db.WithContext(ctx).Order("created_at DESC").Find(&states).Error; err != nil {
		return nil, fmt.Errorf("list saga_state: %w", err)
	}
	return states, nil
}

// CommitInventoryReserved applies the InventoryReservedEvent to order/saga/processed_event
// atomically. Skip indicates duplicate event or wrong step; Failed indicates terminal failure.
func (r *SagaRepository) CommitInventoryReserved(ctx context.Context, eventID string, event shared.InventoryReservedEvent) (InventoryReservedOutcome, error) {
	var out InventoryReservedOutcome
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			out.Skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepReservingInventory {
			out.Skip = true
			return nil
		}

		if !event.Success {
			out.Failed = true
			if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
				Update("status", model.OrderStatusFailed).Error; err != nil {
				return fmt.Errorf("update order status FAILED: %w", err)
			}
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepDone,
				"status":        model.SagaStatusFailed,
				"last_event_id": eventID,
				"last_error":    truncate(event.Reason, 500),
			}).Error; err != nil {
				return fmt.Errorf("update saga_state FAILED: %w", err)
			}
		} else {
			if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
				Update("status", model.OrderStatusInventoryReserved).Error; err != nil {
				return fmt.Errorf("update order status: %w", err)
			}
			var order model.Order
			if err := tx.First(&order, event.OrderID).Error; err != nil {
				return fmt.Errorf("get order: %w", err)
			}
			out.Amount = order.TotalAmount
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepProcessingPayment,
				"last_event_id": eventID,
			}).Error; err != nil {
				return fmt.Errorf("update saga_state: %w", err)
			}
		}

		return tx.Create(&model.ProcessedEvent{
			EventID:    eventID,
			ConsumedAt: time.Now().UTC(),
		}).Error
	})
	if err != nil {
		return InventoryReservedOutcome{}, err
	}
	return out, nil
}

// CommitPaymentProcessed applies the PaymentProcessedEvent to order/saga/processed_event
// atomically. Compensate=true means the orchestrator must publish ReleaseInventoryCmd next.
func (r *SagaRepository) CommitPaymentProcessed(ctx context.Context, eventID string, event shared.PaymentProcessedEvent) (PaymentProcessedOutcome, error) {
	var out PaymentProcessedOutcome
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			out.Skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepProcessingPayment {
			out.Skip = true
			return nil
		}

		if !event.Success {
			out.Compensate = true
			var order model.Order
			if err := tx.First(&order, event.OrderID).Error; err != nil {
				return fmt.Errorf("get order: %w", err)
			}
			out.ProductID = order.ProductID
			out.Quantity = order.Quantity

			if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
				Update("status", model.OrderStatusFailed).Error; err != nil {
				return fmt.Errorf("update order status FAILED: %w", err)
			}
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepReleasingInventory,
				"status":        model.SagaStatusCompensating,
				"last_event_id": eventID,
				"last_error":    truncate(event.Reason, 500),
			}).Error; err != nil {
				return fmt.Errorf("update saga_state COMPENSATING: %w", err)
			}
		} else {
			if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
				Update("status", model.OrderStatusConfirmed).Error; err != nil {
				return fmt.Errorf("update order status: %w", err)
			}
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepDone,
				"status":        model.SagaStatusCompleted,
				"last_event_id": eventID,
			}).Error; err != nil {
				return fmt.Errorf("update saga_state: %w", err)
			}
		}

		return tx.Create(&model.ProcessedEvent{
			EventID:    eventID,
			ConsumedAt: time.Now().UTC(),
		}).Error
	})
	if err != nil {
		return PaymentProcessedOutcome{}, err
	}
	return out, nil
}

// CommitInventoryReleased applies the InventoryReleasedEvent to order/saga/processed_event
// atomically. Failed=true means compensation itself failed and needs manual intervention.
func (r *SagaRepository) CommitInventoryReleased(ctx context.Context, eventID string, event shared.InventoryReleasedEvent) (InventoryReleasedOutcome, error) {
	var out InventoryReleasedOutcome
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			out.Skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.Status != model.SagaStatusCompensating || state.CurrentStep != model.SagaStepReleasingInventory {
			out.Skip = true
			return nil
		}

		if event.Success {
			if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
				Update("status", model.OrderStatusCompensated).Error; err != nil {
				return fmt.Errorf("update order status COMPENSATED: %w", err)
			}
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepDone,
				"status":        model.SagaStatusCompensated,
				"last_event_id": eventID,
			}).Error; err != nil {
				return fmt.Errorf("update saga_state COMPENSATED: %w", err)
			}
		} else {
			out.Failed = true
			if err := tx.Model(&state).Updates(map[string]any{
				"current_step":  model.SagaStepDone,
				"status":        model.SagaStatusFailed,
				"last_event_id": eventID,
				"last_error":    truncate("compensation failed: "+event.Reason, 500),
			}).Error; err != nil {
				return fmt.Errorf("update saga_state FAILED (compensation): %w", err)
			}
		}

		return tx.Create(&model.ProcessedEvent{
			EventID:    eventID,
			ConsumedAt: time.Now().UTC(),
		}).Error
	})
	if err != nil {
		return InventoryReleasedOutcome{}, err
	}
	return out, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
