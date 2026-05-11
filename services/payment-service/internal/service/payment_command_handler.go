package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/repository"
)

type PaymentCommandHandler struct {
	paymentRepo repository.PaymentRepoIface
	eventRepo   repository.ProcessedEventRepoIface
	pub         messaging.PublisherIface
	failureRate float64
}

func NewPaymentCommandHandler(
	paymentRepo repository.PaymentRepoIface,
	eventRepo repository.ProcessedEventRepoIface,
	pub messaging.PublisherIface,
	failureRate float64,
) *PaymentCommandHandler {
	return &PaymentCommandHandler{
		paymentRepo: paymentRepo,
		eventRepo:   eventRepo,
		pub:         pub,
		failureRate: failureRate,
	}
}

func (h *PaymentCommandHandler) Handle(ctx context.Context, msg amqp.Delivery) error {
	switch msg.RoutingKey {
	case shared.RoutingKeyPaymentProcess:
		var cmd shared.ProcessPaymentCmd
		if err := json.Unmarshal(msg.Body, &cmd); err != nil {
			return fmt.Errorf("unmarshal ProcessPaymentCmd: %w", err)
		}
		return h.handleProcess(ctx, cmd, msg.MessageId)
	default:
		log.Printf("payment: unknown routing key %s, skipping", msg.RoutingKey)
		return nil
	}
}

func (h *PaymentCommandHandler) handleProcess(ctx context.Context, cmd shared.ProcessPaymentCmd, msgID string) error {
	// Idempotency: check processed_events first
	done, err := h.eventRepo.IsProcessed(ctx, msgID)
	if err != nil {
		return fmt.Errorf("check processed_events: %w", err)
	}
	if done {
		log.Printf("payment: cmd already processed (msg_id=%s), skipping", msgID)
		return nil
	}

	// Check if payment already exists for this order (payments.order_id UNIQUE)
	existing, err := h.paymentRepo.GetByOrderID(ctx, cmd.OrderID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("check payment: %w", err)
	}

	var (
		txID    string
		success bool
		reason  string
	)
	if existing != nil {
		// Payment row already exists — idempotent re-publish
		txID = existing.TransactionID
		success = existing.Status == model.PaymentStatusSuccess
		if !success {
			reason = "SIMULATED_FAILURE"
		}
		log.Printf("payment: payment row exists for order %d status=%s (idempotent)", cmd.OrderID, existing.Status)
	} else {
		// Simulate failure if configured
		failed := h.failureRate > 0 && rand.Float64() < h.failureRate
		txID = newUUID()
		status := model.PaymentStatusSuccess
		if failed {
			status = model.PaymentStatusFailed
			reason = "SIMULATED_FAILURE"
		}
		payment := &model.Payment{
			OrderID:       cmd.OrderID,
			Amount:        cmd.Amount,
			Status:        status,
			TransactionID: txID,
		}
		if err := h.paymentRepo.CreatePaymentWithEvent(ctx, payment, msgID); err != nil {
			return fmt.Errorf("payment transaction: %w", err)
		}
		success = !failed
		log.Printf("payment: processed order %d tx_id=%s status=%s", cmd.OrderID, txID, status)
	}

	event := shared.PaymentProcessedEvent{
		SagaID:        cmd.SagaID,
		OrderID:       cmd.OrderID,
		TransactionID: txID,
		Success:       success,
		Reason:        reason,
	}
	if err := h.pub.Publish(ctx, shared.ExchangeEvents, shared.RoutingKeyPaymentProcessed, newUUID(), event); err != nil {
		return fmt.Errorf("publish PaymentProcessedEvent: %w", err)
	}
	return nil
}
