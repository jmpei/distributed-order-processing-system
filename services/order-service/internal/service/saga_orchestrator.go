package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
)

const recoveryInterval = 30 * time.Second

type SagaOrchestrator struct {
	db        *gorm.DB
	sagaRepo  *repository.SagaRepository
	orderRepo *repository.OrderRepository
	pub       *messaging.Publisher
}

func NewSagaOrchestrator(
	db *gorm.DB,
	sagaRepo *repository.SagaRepository,
	orderRepo *repository.OrderRepository,
	pub *messaging.Publisher,
) *SagaOrchestrator {
	return &SagaOrchestrator{db: db, sagaRepo: sagaRepo, orderRepo: orderRepo, pub: pub}
}

// StartSaga writes saga_state then publishes ReserveInventoryCmd.
func (o *SagaOrchestrator) StartSaga(ctx context.Context, order *model.Order) error {
	sagaID := newUUID()
	state := &model.SagaState{
		SagaID:      sagaID,
		OrderID:     order.ID,
		CurrentStep: model.SagaStepReservingInventory,
		Status:      model.SagaStatusInProgress,
	}
	if err := o.sagaRepo.Create(ctx, state); err != nil {
		return fmt.Errorf("start saga: %w", err)
	}

	cmd := shared.ReserveInventoryCmd{
		SagaID:    sagaID,
		OrderID:   order.ID,
		ProductID: order.ProductID,
		Quantity:  order.Quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryReserve, newUUID(), cmd); err != nil {
		return fmt.Errorf("publish ReserveInventoryCmd: %w", err)
	}
	log.Printf("saga started saga_id=%s order_id=%d", sagaID, order.ID)
	return nil
}

// HandleEvent is the consumer handler for the order.events queue.
func (o *SagaOrchestrator) HandleEvent(ctx context.Context, msg amqp.Delivery) error {
	eventID := msg.MessageId
	if eventID == "" {
		if v, ok := msg.Headers["message_id"].(string); ok {
			eventID = v
		}
	}
	if eventID == "" {
		log.Printf("saga event has no message_id, skipping (routing_key=%s)", msg.RoutingKey)
		return nil
	}

	switch msg.RoutingKey {
	case shared.RoutingKeyInventoryReserved:
		var event shared.InventoryReservedEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return fmt.Errorf("unmarshal InventoryReservedEvent: %w", err)
		}
		return o.onInventoryReserved(ctx, event, eventID)

	case shared.RoutingKeyPaymentProcessed:
		var event shared.PaymentProcessedEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return fmt.Errorf("unmarshal PaymentProcessedEvent: %w", err)
		}
		return o.onPaymentProcessed(ctx, event, eventID)

	case shared.RoutingKeyInventoryReleased:
		var event shared.InventoryReleasedEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return fmt.Errorf("unmarshal InventoryReleasedEvent: %w", err)
		}
		return o.onInventoryReleased(ctx, event, eventID)

	default:
		log.Printf("saga: unknown routing key %s, skipping", msg.RoutingKey)
		return nil
	}
}

func (o *SagaOrchestrator) onInventoryReserved(ctx context.Context, event shared.InventoryReservedEvent, eventID string) error {
	var (
		skip       bool
		amount     float64
		sagaID     string
		failed     bool
		failReason string
	)

	if err := o.db.Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepReservingInventory {
			skip = true
			return nil
		}

		sagaID = event.SagaID

		if !event.Success {
			// Reservation failed → terminal failure, no compensation needed
			failed = true
			failReason = event.Reason
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
			amount = order.TotalAmount

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
	}); err != nil {
		return fmt.Errorf("onInventoryReserved tx: %w", err)
	}

	if skip {
		return nil
	}

	if failed {
		log.Printf("saga: FAILED at RESERVING_INVENTORY saga_id=%s reason=%s", sagaID, failReason)
		return nil
	}

	cmd := shared.ProcessPaymentCmd{
		SagaID:  sagaID,
		OrderID: event.OrderID,
		Amount:  amount,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
		log.Printf("WARN: publish ProcessPaymentCmd failed after DB commit: %v", err)
		return fmt.Errorf("publish ProcessPaymentCmd: %w", err)
	}
	log.Printf("saga: inventory reserved, payment cmd sent saga_id=%s", sagaID)
	return nil
}

func (o *SagaOrchestrator) onPaymentProcessed(ctx context.Context, event shared.PaymentProcessedEvent, eventID string) error {
	var (
		skip       bool
		compensate bool
		productID  uint64
		quantity   int
	)

	if err := o.db.Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepProcessingPayment {
			skip = true
			return nil
		}

		if !event.Success {
			// Payment failed → compensate by releasing inventory
			compensate = true

			var order model.Order
			if err := tx.First(&order, event.OrderID).Error; err != nil {
				return fmt.Errorf("get order: %w", err)
			}
			productID = order.ProductID
			quantity = order.Quantity

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
	}); err != nil {
		return fmt.Errorf("onPaymentProcessed tx: %w", err)
	}

	if skip {
		return nil
	}

	if !compensate {
		log.Printf("saga: COMPLETED saga_id=%s order_id=%d", event.SagaID, event.OrderID)
		return nil
	}

	cmd := shared.ReleaseInventoryCmd{
		SagaID:    event.SagaID,
		OrderID:   event.OrderID,
		ProductID: productID,
		Quantity:  quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease, newUUID(), cmd); err != nil {
		log.Printf("WARN: publish ReleaseInventoryCmd failed after DB commit: %v", err)
		return fmt.Errorf("publish ReleaseInventoryCmd: %w", err)
	}
	log.Printf("saga: COMPENSATING saga_id=%s reason=%s release_cmd sent", event.SagaID, event.Reason)
	return nil
}

func (o *SagaOrchestrator) onInventoryReleased(ctx context.Context, event shared.InventoryReleasedEvent, eventID string) error {
	var (
		skip          bool
		releaseFailed bool
	)

	if err := o.db.Transaction(func(tx *gorm.DB) error {
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.Status != model.SagaStatusCompensating || state.CurrentStep != model.SagaStepReleasingInventory {
			skip = true
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
			releaseFailed = true
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
	}); err != nil {
		return fmt.Errorf("onInventoryReleased tx: %w", err)
	}

	if skip {
		return nil
	}
	if releaseFailed {
		log.Printf("ERROR: saga compensation failed saga_id=%s reason=%s — manual intervention required",
			event.SagaID, event.Reason)
		return nil
	}
	log.Printf("saga: COMPENSATED saga_id=%s order_id=%d", event.SagaID, event.OrderID)
	return nil
}

// RecoverInProgressSagas resumes interrupted sagas by re-publishing the command
// for the current step. Runs once on startup, then every recoveryInterval.
// All downstream consumers are idempotent so re-publishes are safe.
func (o *SagaOrchestrator) RecoverInProgressSagas(ctx context.Context) {
	o.runRecovery(ctx)

	t := time.NewTicker(recoveryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("saga: recovery loop stopped")
			return
		case <-t.C:
			o.runRecovery(ctx)
		}
	}
}

func (o *SagaOrchestrator) runRecovery(ctx context.Context) {
	inProgress, err := o.sagaRepo.ListByStatus(ctx, model.SagaStatusInProgress)
	if err != nil {
		log.Printf("saga recovery: list IN_PROGRESS: %v", err)
	} else {
		for _, s := range inProgress {
			if err := o.recoverInProgress(ctx, s); err != nil {
				log.Printf("saga recovery: saga_id=%s step=%s: %v", s.SagaID, s.CurrentStep, err)
			}
		}
	}

	compensating, err := o.sagaRepo.ListByStatus(ctx, model.SagaStatusCompensating)
	if err != nil {
		log.Printf("saga recovery: list COMPENSATING: %v", err)
		return
	}
	for _, s := range compensating {
		if err := o.recoverCompensating(ctx, s); err != nil {
			log.Printf("saga recovery: saga_id=%s step=%s: %v", s.SagaID, s.CurrentStep, err)
		}
	}
}

func (o *SagaOrchestrator) recoverInProgress(ctx context.Context, s model.SagaState) error {
	order, err := o.orderRepo.GetByID(ctx, s.OrderID)
	if err != nil {
		return fmt.Errorf("get order: %w", err)
	}

	switch s.CurrentStep {
	case model.SagaStepReservingInventory:
		cmd := shared.ReserveInventoryCmd{
			SagaID:    s.SagaID,
			OrderID:   order.ID,
			ProductID: order.ProductID,
			Quantity:  order.Quantity,
		}
		if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryReserve, newUUID(), cmd); err != nil {
			return fmt.Errorf("republish ReserveInventoryCmd: %w", err)
		}
		log.Printf("saga recovery: re-sent ReserveInventoryCmd saga_id=%s", s.SagaID)
	case model.SagaStepProcessingPayment:
		cmd := shared.ProcessPaymentCmd{
			SagaID:  s.SagaID,
			OrderID: order.ID,
			Amount:  order.TotalAmount,
		}
		if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
			return fmt.Errorf("republish ProcessPaymentCmd: %w", err)
		}
		log.Printf("saga recovery: re-sent ProcessPaymentCmd saga_id=%s", s.SagaID)
	default:
		log.Printf("saga recovery: skipping IN_PROGRESS saga_id=%s with unexpected step=%s", s.SagaID, s.CurrentStep)
	}
	return nil
}

func (o *SagaOrchestrator) recoverCompensating(ctx context.Context, s model.SagaState) error {
	if s.CurrentStep != model.SagaStepReleasingInventory {
		log.Printf("saga recovery: skipping COMPENSATING saga_id=%s with unexpected step=%s", s.SagaID, s.CurrentStep)
		return nil
	}
	order, err := o.orderRepo.GetByID(ctx, s.OrderID)
	if err != nil {
		return fmt.Errorf("get order: %w", err)
	}
	cmd := shared.ReleaseInventoryCmd{
		SagaID:    s.SagaID,
		OrderID:   order.ID,
		ProductID: order.ProductID,
		Quantity:  order.Quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease, newUUID(), cmd); err != nil {
		return fmt.Errorf("republish ReleaseInventoryCmd: %w", err)
	}
	log.Printf("saga recovery: re-sent ReleaseInventoryCmd saga_id=%s", s.SagaID)
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("rand.Read: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
