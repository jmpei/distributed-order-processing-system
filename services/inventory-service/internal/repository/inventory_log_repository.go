package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
)

type InventoryLogRepository struct {
	db *gorm.DB
}

func NewInventoryLogRepository(db *gorm.DB) *InventoryLogRepository {
	return &InventoryLogRepository{db: db}
}

// ExistsReserve returns true when an RESERVE log exists for this order (idempotency check).
func (r *InventoryLogRepository) ExistsReserve(ctx context.Context, orderID uint64) (bool, error) {
	var log model.InventoryLog
	err := r.db.WithContext(ctx).
		Where("order_id = ? AND action = ?", orderID, model.InventoryActionReserve).
		First(&log).Error
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("check inventory_log: %w", err)
}
