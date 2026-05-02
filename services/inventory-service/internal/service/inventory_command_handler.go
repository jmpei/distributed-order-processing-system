package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
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
	db      *gorm.DB
	invRepo *repository.InventoryRepository
	logRepo *repository.InventoryLogRepository
	pub     *messaging.Publisher
}

func NewInventoryCommandHandler(
	db *gorm.DB,
	invRepo *repository.InventoryRepository,
	logRepo *repository.InventoryLogRepository,
	pub *messaging.Publisher,
) *InventoryCommandHandler {
	return &InventoryCommandHandler{db: db, invRepo: invRepo, logRepo: logRepo, pub: pub}
}

func (h *InventoryCommandHandler) Handle(ctx context.Context, msg amqp.Delivery) error {
	switch msg.RoutingKey {
	case shared.RoutingKeyInventoryReserve:
		var cmd shared.ReserveInventoryCmd
		if err := json.Unmarshal(msg.Body, &cmd); err != nil {
			return fmt.Errorf("unmarshal ReserveInventoryCmd: %w", err)
		}
		return h.handleReserve(ctx, cmd)
	default:
		log.Printf("inventory: unknown routing key %s, skipping", msg.RoutingKey)
		return nil
	}
}

func (h *InventoryCommandHandler) handleReserve(ctx context.Context, cmd shared.ReserveInventoryCmd) error {
	// Idempotency: check inventory_logs
	exists, err := h.logRepo.ExistsReserve(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("check inventory_log: %w", err)
	}

	if exists {
		log.Printf("inventory: reserve already done for order %d (idempotent)", cmd.OrderID)
		return h.publishReserved(ctx, cmd, true)
	}

	// Reserve in transaction: decrement available, increment reserved, write log
	if err := h.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Inventory{}).
			Where("product_id = ?", cmd.ProductID).
			Updates(map[string]any{
				"available_qty": gorm.Expr("available_qty - ?", cmd.Quantity),
				"reserved_qty":  gorm.Expr("reserved_qty + ?", cmd.Quantity),
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
			Action:    model.InventoryActionReserve,
			Quantity:  cmd.Quantity,
		}).Error
	}); err != nil {
		return fmt.Errorf("reserve transaction: %w", err)
	}

	log.Printf("inventory: reserved %d units of product %d for order %d", cmd.Quantity, cmd.ProductID, cmd.OrderID)
	return h.publishReserved(ctx, cmd, true)
}

func (h *InventoryCommandHandler) publishReserved(ctx context.Context, cmd shared.ReserveInventoryCmd, success bool) error {
	event := shared.InventoryReservedEvent{
		SagaID:    cmd.SagaID,
		OrderID:   cmd.OrderID,
		ProductID: cmd.ProductID,
		Quantity:  cmd.Quantity,
		Success:   success,
	}
	if err := h.pub.Publish(ctx, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved, newUUID(), event); err != nil {
		return fmt.Errorf("publish InventoryReservedEvent: %w", err)
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
