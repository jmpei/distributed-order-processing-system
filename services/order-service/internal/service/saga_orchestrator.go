package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
)

const recoveryInterval = 30 * time.Second

type SagaOrchestrator struct {
	sagaRepo  repository.SagaRepoIface
	orderRepo repository.OrderRepoIface
	pub       messaging.PublisherIface
}

func NewSagaOrchestrator(
	sagaRepo repository.SagaRepoIface,
	orderRepo repository.OrderRepoIface,
	pub messaging.PublisherIface,
) *SagaOrchestrator {
	return &SagaOrchestrator{sagaRepo: sagaRepo, orderRepo: orderRepo, pub: pub}
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
	out, err := o.sagaRepo.CommitInventoryReserved(ctx, eventID, event)
	if err != nil {
		return fmt.Errorf("onInventoryReserved: %w", err)
	}
	if out.Skip {
		return nil
	}
	if out.Failed {
		log.Printf("saga: FAILED at RESERVING_INVENTORY saga_id=%s reason=%s", event.SagaID, event.Reason)
		return nil
	}

	cmd := shared.ProcessPaymentCmd{
		SagaID:  event.SagaID,
		OrderID: event.OrderID,
		Amount:  out.Amount,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
		log.Printf("WARN: publish ProcessPaymentCmd failed after DB commit: %v", err)
		return fmt.Errorf("publish ProcessPaymentCmd: %w", err)
	}
	log.Printf("saga: inventory reserved, payment cmd sent saga_id=%s", event.SagaID)
	return nil
}

func (o *SagaOrchestrator) onPaymentProcessed(ctx context.Context, event shared.PaymentProcessedEvent, eventID string) error {
	out, err := o.sagaRepo.CommitPaymentProcessed(ctx, eventID, event)
	if err != nil {
		return fmt.Errorf("onPaymentProcessed: %w", err)
	}
	if out.Skip {
		return nil
	}
	if !out.Compensate {
		log.Printf("saga: COMPLETED saga_id=%s order_id=%d", event.SagaID, event.OrderID)
		return nil
	}

	cmd := shared.ReleaseInventoryCmd{
		SagaID:    event.SagaID,
		OrderID:   event.OrderID,
		ProductID: out.ProductID,
		Quantity:  out.Quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease, newUUID(), cmd); err != nil {
		log.Printf("WARN: publish ReleaseInventoryCmd failed after DB commit: %v", err)
		return fmt.Errorf("publish ReleaseInventoryCmd: %w", err)
	}
	log.Printf("saga: COMPENSATING saga_id=%s reason=%s release_cmd sent", event.SagaID, event.Reason)
	return nil
}

func (o *SagaOrchestrator) onInventoryReleased(ctx context.Context, event shared.InventoryReleasedEvent, eventID string) error {
	out, err := o.sagaRepo.CommitInventoryReleased(ctx, eventID, event)
	if err != nil {
		return fmt.Errorf("onInventoryReleased: %w", err)
	}
	if out.Skip {
		return nil
	}
	if out.Failed {
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
