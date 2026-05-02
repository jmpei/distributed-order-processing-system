package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/repository"
)

type PaymentCommandHandler struct {
	db           *gorm.DB
	paymentRepo  *repository.PaymentRepository
	eventRepo    *repository.ProcessedEventRepository
	pub          *messaging.Publisher
}

func NewPaymentCommandHandler(
	db *gorm.DB,
	paymentRepo *repository.PaymentRepository,
	eventRepo *repository.ProcessedEventRepository,
	pub *messaging.Publisher,
) *PaymentCommandHandler {
	return &PaymentCommandHandler{db: db, paymentRepo: paymentRepo, eventRepo: eventRepo, pub: pub}
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

	var txID string
	if existing != nil {
		// Payment row already exists — idempotent re-publish
		txID = existing.TransactionID
		log.Printf("payment: payment row exists for order %d (idempotent)", cmd.OrderID)
	} else {
		// Create payment in transaction + mark event processed atomically
		txID = newUUID()
		payment := &model.Payment{
			OrderID:       cmd.OrderID,
			Amount:        cmd.Amount,
			Status:        model.PaymentStatusSuccess, // Phase 3: always success
			TransactionID: txID,
		}
		if err := h.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(payment).Error; err != nil {
				return fmt.Errorf("create payment: %w", err)
			}
			return repository.MarkProcessed(tx, msgID)
		}); err != nil {
			return fmt.Errorf("payment transaction: %w", err)
		}
		log.Printf("payment: processed order %d tx_id=%s", cmd.OrderID, txID)
	}

	event := shared.PaymentProcessedEvent{
		SagaID:        cmd.SagaID,
		OrderID:       cmd.OrderID,
		TransactionID: txID,
		Success:       true,
	}
	if err := h.pub.Publish(ctx, shared.ExchangeEvents, shared.RoutingKeyPaymentProcessed, newUUID(), event); err != nil {
		return fmt.Errorf("publish PaymentProcessedEvent: %w", err)
	}
	return nil
}
