package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/messaging"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/repository"
)

type InventoryCommandHandler struct {
	db                *gorm.DB
	invRepo           *repository.InventoryRepository
	logRepo           *repository.InventoryLogRepository
	pub               *messaging.Publisher
	reserveMaxRetries int
}

func NewInventoryCommandHandler(
	db *gorm.DB,
	invRepo *repository.InventoryRepository,
	logRepo *repository.InventoryLogRepository,
	pub *messaging.Publisher,
	reserveMaxRetries int,
) *InventoryCommandHandler {
	return &InventoryCommandHandler{
		db:                db,
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
		log.Printf("inventory: unknown routing key %s, skipping", msg.RoutingKey)
		return nil
	}
}

var errVersionConflict = errors.New("version conflict")

func (h *InventoryCommandHandler) handleReserve(ctx context.Context, cmd shared.ReserveInventoryCmd) error {
	// Idempotency: check inventory_logs
	exists, err := h.logRepo.ExistsReserve(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("check inventory_log: %w", err)
	}
	if exists {
		log.Printf("inventory: reserve already done for order %d (idempotent)", cmd.OrderID)
		return h.publishReserved(ctx, cmd, true, "")
	}

	for attempt := range h.reserveMaxRetries {
		// Read current row to get version and check stock.
		inv, err := h.invRepo.GetByProductID(ctx, cmd.ProductID)
		if err != nil {
			return fmt.Errorf("get inventory: %w", err)
		}
		if inv.AvailableQty < cmd.Quantity {
			log.Printf("inventory: insufficient stock product=%d available=%d requested=%d",
				cmd.ProductID, inv.AvailableQty, cmd.Quantity)
			return h.publishReserved(ctx, cmd, false, "INSUFFICIENT_STOCK")
		}

		// Optimistic lock: UPDATE only when version still matches.
		err = h.db.Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&model.Inventory{}).
				Where("product_id = ? AND version = ? AND available_qty >= ?",
					cmd.ProductID, inv.Version, cmd.Quantity).
				Updates(map[string]any{
					"available_qty": gorm.Expr("available_qty - ?", cmd.Quantity),
					"reserved_qty":  gorm.Expr("reserved_qty + ?", cmd.Quantity),
					"version":       gorm.Expr("version + 1"),
				})
			if result.Error != nil {
				return fmt.Errorf("update inventory: %w", result.Error)
			}
			if result.RowsAffected == 0 {
				return errVersionConflict
			}
			return tx.Create(&model.InventoryLog{
				ProductID: cmd.ProductID,
				OrderID:   cmd.OrderID,
				Action:    model.InventoryActionReserve,
				Quantity:  cmd.Quantity,
			}).Error
		})
		if err == nil {
			log.Printf("inventory: reserved %d units of product %d for order %d (attempt %d)",
				cmd.Quantity, cmd.ProductID, cmd.OrderID, attempt+1)
			return h.publishReserved(ctx, cmd, true, "")
		}
		if !errors.Is(err, errVersionConflict) {
			return fmt.Errorf("reserve transaction: %w", err)
		}
		log.Printf("inventory: version conflict product=%d attempt=%d/%d, retrying",
			cmd.ProductID, attempt+1, h.reserveMaxRetries)
	}

	// All retries exhausted under contention; surface as insufficient stock to avoid
	// infinite republish loops. Bump INVENTORY_RESERVE_MAX_RETRIES if this fires.
	log.Printf("inventory: optimistic lock failed after %d retries for product %d, treating as insufficient stock",
		h.reserveMaxRetries, cmd.ProductID)
	return h.publishReserved(ctx, cmd, false, "INSUFFICIENT_STOCK")
}

func (h *InventoryCommandHandler) handleRelease(ctx context.Context, cmd shared.ReleaseInventoryCmd) error {
	// Idempotency: check inventory_logs
	exists, err := h.logRepo.ExistsRelease(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("check inventory_log: %w", err)
	}
	if exists {
		log.Printf("inventory: release already done for order %d (idempotent)", cmd.OrderID)
		return h.publishReleased(ctx, cmd, true, "")
	}

	if err := h.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Inventory{}).
			Where("product_id = ?", cmd.ProductID).
			Updates(map[string]any{
				"available_qty": gorm.Expr("available_qty + ?", cmd.Quantity),
				"reserved_qty":  gorm.Expr("GREATEST(reserved_qty - ?, 0)", cmd.Quantity),
				"version":       gorm.Expr("version + 1"),
			})
		if result.Error != nil {
			return fmt.Errorf("update inventory: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("product %d not found", cmd.ProductID)
		}
		return tx.Create(&model.InventoryLog{
			ProductID: cmd.ProductID,
			OrderID:   cmd.OrderID,
			Action:    model.InventoryActionRelease,
			Quantity:  cmd.Quantity,
		}).Error
	}); err != nil {
		log.Printf("inventory: release transaction failed order=%d: %v", cmd.OrderID, err)
		return h.publishReleased(ctx, cmd, false, err.Error())
	}

	log.Printf("inventory: released %d units of product %d for order %d",
		cmd.Quantity, cmd.ProductID, cmd.OrderID)
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
