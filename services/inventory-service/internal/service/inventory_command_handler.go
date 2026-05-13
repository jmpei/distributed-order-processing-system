package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/repository"
)

type InventoryCommandHandler struct {
	invRepo           repository.InventoryRepoIface
	logRepo           repository.InventoryLogRepoIface
	pub               messaging.PublisherIface
	reserveMaxRetries int
}

func NewInventoryCommandHandler(
	invRepo repository.InventoryRepoIface,
	logRepo repository.InventoryLogRepoIface,
	pub messaging.PublisherIface,
	reserveMaxRetries int,
) *InventoryCommandHandler {
	return &InventoryCommandHandler{
		invRepo:           invRepo,
		logRepo:           logRepo,
		pub:               pub,
		reserveMaxRetries: reserveMaxRetries,
	}
}

func (h *InventoryCommandHandler) Handle(ctx context.Context, msg amqp.Delivery) error {
	switch msg.RoutingKey {
	case shared.RoutingKeyInventoryReserve:
		var cmd shared.ReserveInventoryCmd
		if err := json.Unmarshal(msg.Body, &cmd); err != nil {
			return fmt.Errorf("unmarshal ReserveInventoryCmd: %w", err)
		}
		return h.handleReserve(ctx, cmd)
	case shared.RoutingKeyInventoryRelease:
		var cmd shared.ReleaseInventoryCmd
		if err := json.Unmarshal(msg.Body, &cmd); err != nil {
			return fmt.Errorf("unmarshal ReleaseInventoryCmd: %w", err)
		}
		return h.handleRelease(ctx, cmd)
	default:
		observability.LoggerFrom(ctx).Warn("inventory: unknown routing key, skipping",
			zap.String("routing_key", msg.RoutingKey))
		return nil
	}
}

func (h *InventoryCommandHandler) handleReserve(ctx context.Context, cmd shared.ReserveInventoryCmd) error {
	// Idempotency: check inventory_logs
	exists, err := h.logRepo.ExistsReserve(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("check inventory_log: %w", err)
	}
	logger := observability.LoggerFrom(ctx)
	if exists {
		logger.Info("reserve already done (idempotent)", zap.Uint64("order_id", cmd.OrderID))
		return h.publishReserved(ctx, cmd, true, "")
	}

	for attempt := range h.reserveMaxRetries {
		inv, err := h.invRepo.GetByProductID(ctx, cmd.ProductID)
		if err != nil {
			return fmt.Errorf("get inventory: %w", err)
		}
		if inv.AvailableQty < cmd.Quantity {
			logger.Info("insufficient stock",
				zap.Uint64("product_id", cmd.ProductID),
				zap.Int("available", inv.AvailableQty),
				zap.Int("requested", cmd.Quantity))
			return h.publishReserved(ctx, cmd, false, "INSUFFICIENT_STOCK")
		}

		rowsAffected, err := h.invRepo.ReserveAtomic(ctx, cmd.ProductID, cmd.OrderID, inv.Version, cmd.Quantity)
		if err != nil {
			return fmt.Errorf("reserve atomic: %w", err)
		}
		if rowsAffected > 0 {
			logger.Info("reserved",
				zap.Int("quantity", cmd.Quantity),
				zap.Uint64("product_id", cmd.ProductID),
				zap.Uint64("order_id", cmd.OrderID),
				zap.Int("attempt", attempt+1))
			return h.publishReserved(ctx, cmd, true, "")
		}
		logger.Debug("version conflict, retrying",
			zap.Uint64("product_id", cmd.ProductID),
			zap.Int("attempt", attempt+1),
			zap.Int("max", h.reserveMaxRetries))
	}

	// All retries exhausted under contention; surface as insufficient stock to avoid
	// infinite republish loops. Bump INVENTORY_RESERVE_MAX_RETRIES if this fires.
	logger.Warn("optimistic lock failed after all retries, treating as insufficient stock",
		zap.Int("retries", h.reserveMaxRetries),
		zap.Uint64("product_id", cmd.ProductID))
	return h.publishReserved(ctx, cmd, false, "INSUFFICIENT_STOCK")
}

func (h *InventoryCommandHandler) handleRelease(ctx context.Context, cmd shared.ReleaseInventoryCmd) error {
	// Idempotency: check inventory_logs
	exists, err := h.logRepo.ExistsRelease(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("check inventory_log: %w", err)
	}
	logger := observability.LoggerFrom(ctx)
	if exists {
		logger.Info("release already done (idempotent)", zap.Uint64("order_id", cmd.OrderID))
		return h.publishReleased(ctx, cmd, true, "")
	}

	rowsAffected, err := h.invRepo.ReleaseAtomic(ctx, cmd.ProductID, cmd.OrderID, cmd.Quantity)
	if err != nil {
		logger.Error("release transaction failed", zap.Uint64("order_id", cmd.OrderID), zap.Error(err))
		return h.publishReleased(ctx, cmd, false, err.Error())
	}
	if rowsAffected == 0 {
		err := fmt.Errorf("product %d not found", cmd.ProductID)
		logger.Error("release failed", zap.Uint64("order_id", cmd.OrderID), zap.Error(err))
		return h.publishReleased(ctx, cmd, false, err.Error())
	}

	logger.Info("released",
		zap.Int("quantity", cmd.Quantity),
		zap.Uint64("product_id", cmd.ProductID),
		zap.Uint64("order_id", cmd.OrderID))
	return h.publishReleased(ctx, cmd, true, "")
}

func (h *InventoryCommandHandler) publishReserved(ctx context.Context, cmd shared.ReserveInventoryCmd, success bool, reason string) error {
	event := shared.InventoryReservedEvent{
		SagaID:    cmd.SagaID,
		OrderID:   cmd.OrderID,
		ProductID: cmd.ProductID,
		Quantity:  cmd.Quantity,
		Success:   success,
		Reason:    reason,
	}
	if err := h.pub.Publish(ctx, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved, newUUID(), event); err != nil {
		return fmt.Errorf("publish InventoryReservedEvent: %w", err)
	}
	return nil
}

func (h *InventoryCommandHandler) publishReleased(ctx context.Context, cmd shared.ReleaseInventoryCmd, success bool, reason string) error {
	event := shared.InventoryReleasedEvent{
		SagaID:    cmd.SagaID,
		OrderID:   cmd.OrderID,
		ProductID: cmd.ProductID,
		Quantity:  cmd.Quantity,
		Success:   success,
		Reason:    reason,
	}
	if err := h.pub.Publish(ctx, shared.ExchangeEvents, shared.RoutingKeyInventoryReleased, newUUID(), event); err != nil {
		return fmt.Errorf("publish InventoryReleasedEvent: %w", err)
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
