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
// It dispatches to the appropriate saga step based on routing key.
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

	default:
		log.Printf("saga: unknown routing key %s, skipping", msg.RoutingKey)
		return nil
	}
}

func (o *SagaOrchestrator) onInventoryReserved(ctx context.Context, event shared.InventoryReservedEvent, eventID string) error {
	var (
		skip    bool
		amount  float64
		sagaID  string
	)

	if err := o.db.Transaction(func(tx *gorm.DB) error {
		// Idempotency: check processed_events
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		// Get saga state
		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepReservingInventory {
			skip = true
			return nil
		}

		// Update order status
		if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
			Update("status", model.OrderStatusInventoryReserved).Error; err != nil {
			return fmt.Errorf("update order status: %w", err)
		}

		// Read order amount for payment command
		var order model.Order
		if err := tx.First(&order, event.OrderID).Error; err != nil {
			return fmt.Errorf("get order: %w", err)
		}
		amount = order.TotalAmount
		sagaID = event.SagaID

		// Advance saga state
		if err := tx.Model(&state).Updates(map[string]any{
			"current_step": model.SagaStepProcessingPayment,
			"last_event_id": eventID,
		}).Error; err != nil {
			return fmt.Errorf("update saga_state: %w", err)
		}

		// Mark event processed
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

	cmd := shared.ProcessPaymentCmd{
		SagaID:  sagaID,
		OrderID: event.OrderID,
		Amount:  amount,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
		// Publish failed after DB commit: saga is stuck at PROCESSING_PAYMENT.
		// Phase 4 timeout recovery will re-send the command.
		log.Printf("WARN: publish ProcessPaymentCmd failed after DB commit: %v", err)
		return fmt.Errorf("publish ProcessPaymentCmd: %w", err)
	}
	log.Printf("saga: inventory reserved, payment cmd sent saga_id=%s", sagaID)
	return nil
}

func (o *SagaOrchestrator) onPaymentProcessed(ctx context.Context, event shared.PaymentProcessedEvent, eventID string) error {
	var skip bool

	if err := o.db.Transaction(func(tx *gorm.DB) error {
		// Idempotency
		var pe model.ProcessedEvent
		if err := tx.First(&pe, "event_id = ?", eventID).Error; err == nil {
			skip = true
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check processed_event: %w", err)
		}

		// Get saga state
		var state model.SagaState
		if err := tx.First(&state, "saga_id = ?", event.SagaID).Error; err != nil {
			return fmt.Errorf("get saga_state: %w", err)
		}
		if state.CurrentStep != model.SagaStepProcessingPayment {
			skip = true
			return nil
		}

		// Update order to CONFIRMED (PAID→CONFIRMED in one step since we're the last)
		if err := tx.Model(&model.Order{}).Where("id = ?", event.OrderID).
			Update("status", model.OrderStatusConfirmed).Error; err != nil {
			return fmt.Errorf("update order status: %w", err)
		}

		// Complete saga
		if err := tx.Model(&state).Updates(map[string]any{
			"current_step":  model.SagaStepDone,
			"status":        model.SagaStatusCompleted,
			"last_event_id": eventID,
		}).Error; err != nil {
			return fmt.Errorf("update saga_state: %w", err)
		}

		// Mark event processed
		return tx.Create(&model.ProcessedEvent{
			EventID:    eventID,
			ConsumedAt: time.Now().UTC(),
		}).Error
	}); err != nil {
		return fmt.Errorf("onPaymentProcessed tx: %w", err)
	}

	if !skip {
		log.Printf("saga: COMPLETED saga_id=%s order_id=%d", event.SagaID, event.OrderID)
	}
	return nil
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
