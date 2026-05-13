package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
)

const recoveryInterval = 30 * time.Second

type SagaOrchestrator struct {
	sagaRepo  repository.SagaRepoIface
	orderRepo repository.OrderRepoIface
	pub       messaging.PublisherIface
	logger    *zap.Logger
}

func NewSagaOrchestrator(
	sagaRepo repository.SagaRepoIface,
	orderRepo repository.OrderRepoIface,
	pub messaging.PublisherIface,
	logger *zap.Logger,
) *SagaOrchestrator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SagaOrchestrator{sagaRepo: sagaRepo, orderRepo: orderRepo, pub: pub, logger: logger}
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

	ctx = observability.WithSagaID(ctx, sagaID)

	cmd := shared.ReserveInventoryCmd{
		SagaID:    sagaID,
		OrderID:   order.ID,
		ProductID: order.ProductID,
		Quantity:  order.Quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryReserve, newUUID(), cmd); err != nil {
		return fmt.Errorf("publish ReserveInventoryCmd: %w", err)
	}
	observability.LoggerFrom(ctx).Info("saga started", zap.Uint64("order_id", order.ID))
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
		observability.LoggerFrom(ctx).Warn("saga event missing message_id, skipping",
			zap.String("routing_key", msg.RoutingKey))
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
		observability.LoggerFrom(ctx).Warn("saga: unknown routing key, skipping",
			zap.String("routing_key", msg.RoutingKey))
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
		o.recordTerminal(ctx, event.SagaID, model.SagaStatusFailed, "RESERVING_INVENTORY", event.Reason)
		return nil
	}

	cmd := shared.ProcessPaymentCmd{
		SagaID:  event.SagaID,
		OrderID: event.OrderID,
		Amount:  out.Amount,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
		observability.LoggerFrom(ctx).Warn("publish ProcessPaymentCmd failed after DB commit", zap.Error(err))
		return fmt.Errorf("publish ProcessPaymentCmd: %w", err)
	}
	observability.LoggerFrom(ctx).Info("saga: inventory reserved, payment cmd sent")
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
		o.recordTerminal(ctx, event.SagaID, model.SagaStatusCompleted, "DONE", "")
		return nil
	}

	cmd := shared.ReleaseInventoryCmd{
		SagaID:    event.SagaID,
		OrderID:   event.OrderID,
		ProductID: out.ProductID,
		Quantity:  out.Quantity,
	}
	if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease, newUUID(), cmd); err != nil {
		observability.LoggerFrom(ctx).Warn("publish ReleaseInventoryCmd failed after DB commit", zap.Error(err))
		return fmt.Errorf("publish ReleaseInventoryCmd: %w", err)
	}
	observability.LoggerFrom(ctx).Info("saga: COMPENSATING, release cmd sent",
		zap.String("reason", event.Reason))
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
		// Saga compensation itself failed — mark FAILED, record metric, alert via log.
		observability.LoggerFrom(ctx).Error("saga compensation failed, manual intervention required",
			zap.String("reason", event.Reason))
		o.recordTerminal(ctx, event.SagaID, model.SagaStatusFailed, "RELEASING_INVENTORY", event.Reason)
		return nil
	}
	o.recordTerminal(ctx, event.SagaID, model.SagaStatusCompensated, "DONE", "")
	return nil
}

// recordTerminal centralises the metric + log for any terminal saga state.
// It looks up the saga to compute end-to-end duration; the extra read is
// negligible against the 1 query/saga we already do for state transitions.
func (o *SagaOrchestrator) recordTerminal(ctx context.Context, sagaID, status, step, reason string) {
	observability.SagaTotal.WithLabelValues(status).Inc()

	if s, err := o.sagaRepo.GetBySagaID(ctx, sagaID); err == nil {
		observability.SagaDuration.WithLabelValues(status).
			Observe(time.Since(s.CreatedAt).Seconds())
	}

	fields := []zap.Field{
		zap.String("status", status),
		zap.String("step", step),
	}
	if reason != "" {
		fields = append(fields, zap.String("reason", reason))
	}
	observability.LoggerFrom(ctx).Info("saga: terminal", fields...)
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
			o.logger.Info("saga recovery loop stopped")
			return
		case <-t.C:
			o.runRecovery(ctx)
		}
	}
}

func (o *SagaOrchestrator) runRecovery(ctx context.Context) {
	inProgress, err := o.sagaRepo.ListByStatus(ctx, model.SagaStatusInProgress)
	if err != nil {
		o.logger.Warn("saga recovery: list IN_PROGRESS", zap.Error(err))
	} else {
		for _, s := range inProgress {
			if err := o.recoverInProgress(ctx, s); err != nil {
				o.logger.Warn("saga recovery",
					zap.String("saga_id", s.SagaID),
					zap.String("step", s.CurrentStep),
					zap.Error(err))
			}
		}
	}

	compensating, err := o.sagaRepo.ListByStatus(ctx, model.SagaStatusCompensating)
	if err != nil {
		o.logger.Warn("saga recovery: list COMPENSATING", zap.Error(err))
		return
	}
	for _, s := range compensating {
		if err := o.recoverCompensating(ctx, s); err != nil {
			o.logger.Warn("saga recovery",
				zap.String("saga_id", s.SagaID),
				zap.String("step", s.CurrentStep),
				zap.Error(err))
		}
	}
}

func (o *SagaOrchestrator) recoverInProgress(ctx context.Context, s model.SagaState) error {
	// Recovery runs outside any HTTP request — seed a fresh ctx with
	// the orchestrator's base logger + the saga_id so re-publish logs
	// remain correlatable.
	ctx = observability.WithLogger(ctx, o.logger)
	ctx = observability.WithSagaID(ctx, s.SagaID)

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
		observability.LoggerFrom(ctx).Info("saga recovery: re-sent ReserveInventoryCmd")
	case model.SagaStepProcessingPayment:
		cmd := shared.ProcessPaymentCmd{
			SagaID:  s.SagaID,
			OrderID: order.ID,
			Amount:  order.TotalAmount,
		}
		if err := o.pub.Publish(ctx, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess, newUUID(), cmd); err != nil {
			return fmt.Errorf("republish ProcessPaymentCmd: %w", err)
		}
		observability.LoggerFrom(ctx).Info("saga recovery: re-sent ProcessPaymentCmd")
	default:
		observability.LoggerFrom(ctx).Warn("saga recovery: skipping IN_PROGRESS with unexpected step",
			zap.String("step", s.CurrentStep))
	}
	return nil
}

func (o *SagaOrchestrator) recoverCompensating(ctx context.Context, s model.SagaState) error {
	ctx = observability.WithLogger(ctx, o.logger)
	ctx = observability.WithSagaID(ctx, s.SagaID)

	if s.CurrentStep != model.SagaStepReleasingInventory {
		observability.LoggerFrom(ctx).Warn("saga recovery: skipping COMPENSATING with unexpected step",
			zap.String("step", s.CurrentStep))
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
	observability.LoggerFrom(ctx).Info("saga recovery: re-sent ReleaseInventoryCmd")
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
