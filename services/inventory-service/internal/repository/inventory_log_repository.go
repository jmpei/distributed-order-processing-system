package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
)

// InventoryLogRepoIface is the surface InventoryCommandHandler depends on; allows mocking.
type InventoryLogRepoIface interface {
	ExistsReserve(ctx context.Context, orderID uint64) (bool, error)
	ExistsRelease(ctx context.Context, orderID uint64) (bool, error)
}

type InventoryLogRepository struct {
	db *gorm.DB
}

func NewInventoryLogRepository(db *gorm.DB) *InventoryLogRepository {
	return &InventoryLogRepository{db: db}
}

// ExistsReserve returns true when a RESERVE log exists for this order (idempotency check).
func (r *InventoryLogRepository) ExistsReserve(ctx context.Context, orderID uint64) (bool, error) {
	return r.existsAction(ctx, orderID, model.InventoryActionReserve)
}

// ExistsRelease returns true when a RELEASE log exists for this order (idempotency check).
func (r *InventoryLogRepository) ExistsRelease(ctx context.Context, orderID uint64) (bool, error) {
	return r.existsAction(ctx, orderID, model.InventoryActionRelease)
}

func (r *InventoryLogRepository) existsAction(ctx context.Context, orderID uint64, action string) (bool, error) {
	var entry model.InventoryLog
	err := r.db.WithContext(ctx).
		Where("order_id = ? AND action = ?", orderID, action).
		First(&entry).Error
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("check inventory_log: %w", err)
}
