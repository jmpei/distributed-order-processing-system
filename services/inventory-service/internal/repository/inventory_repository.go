package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
)

// InventoryRepoIface is the surface InventoryCommandHandler and InventoryService depend on.
type InventoryRepoIface interface {
	Create(ctx context.Context, inv *model.Inventory) error
	GetByProductID(ctx context.Context, productID uint64) (*model.Inventory, error)
	UpdateAvailableQty(ctx context.Context, productID uint64, qty int) error
	ReserveAtomic(ctx context.Context, productID, orderID uint64, expectedVersion, qty int) (int64, error)
	ReleaseAtomic(ctx context.Context, productID, orderID uint64, qty int) (int64, error)
}

type InventoryRepository struct {
	db *gorm.DB
}

func NewInventoryRepository(db *gorm.DB) *InventoryRepository {
	return &InventoryRepository{db: db}
}

func (r *InventoryRepository) Create(ctx context.Context, inv *model.Inventory) error {
	if err := r.db.WithContext(ctx).Create(inv).Error; err != nil {
		return fmt.Errorf("create inventory: %w", err)
	}
	return nil
}

func (r *InventoryRepository) GetByProductID(ctx context.Context, productID uint64) (*model.Inventory, error) {
	var inv model.Inventory
	if err := r.db.WithContext(ctx).Where("product_id = ?", productID).First(&inv).Error; err != nil {
		return nil, fmt.Errorf("get inventory product %d: %w", productID, err)
	}
	return &inv, nil
}

func (r *InventoryRepository) UpdateAvailableQty(ctx context.Context, productID uint64, qty int) error {
	result := r.db.WithContext(ctx).
		Model(&model.Inventory{}).
		Where("product_id = ?", productID).
		Update("available_qty", qty)
	if result.Error != nil {
		return fmt.Errorf("update available_qty product %d: %w", productID, result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("inventory for product %d not found", productID)
	}
	return nil
}

// ReserveAtomic performs the optimistic-lock UPDATE and inventory_log insert in one transaction.
// Returns RowsAffected of the UPDATE: 0 means version conflict, >0 means reserved.
func (r *InventoryRepository) ReserveAtomic(ctx context.Context, productID, orderID uint64, expectedVersion, qty int) (int64, error) {
	var rowsAffected int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Inventory{}).
			Where("product_id = ? AND version = ? AND available_qty >= ?",
				productID, expectedVersion, qty).
			Updates(map[string]any{
				"available_qty": gorm.Expr("available_qty - ?", qty),
				"reserved_qty":  gorm.Expr("reserved_qty + ?", qty),
				"version":       gorm.Expr("version + 1"),
			})
		if result.Error != nil {
			return fmt.Errorf("update inventory: %w", result.Error)
		}
		rowsAffected = result.RowsAffected
		if rowsAffected == 0 {
			return nil
		}
		return tx.Create(&model.InventoryLog{
			ProductID: productID,
			OrderID:   orderID,
			Action:    model.InventoryActionReserve,
			Quantity:  qty,
		}).Error
	})
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

// ReleaseAtomic refunds available_qty and writes a RELEASE log in one transaction.
func (r *InventoryRepository) ReleaseAtomic(ctx context.Context, productID, orderID uint64, qty int) (int64, error) {
	var rowsAffected int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Inventory{}).
			Where("product_id = ?", productID).
			Updates(map[string]any{
				"available_qty": gorm.Expr("available_qty + ?", qty),
				"reserved_qty":  gorm.Expr("GREATEST(reserved_qty - ?, 0)", qty),
				"version":       gorm.Expr("version + 1"),
			})
		if result.Error != nil {
			return fmt.Errorf("update inventory: %w", result.Error)
		}
		rowsAffected = result.RowsAffected
		if rowsAffected == 0 {
			return nil
		}
		return tx.Create(&model.InventoryLog{
			ProductID: productID,
			OrderID:   orderID,
			Action:    model.InventoryActionRelease,
			Quantity:  qty,
		}).Error
	})
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

// SeedIfNotExists creates the record only when product_id is absent (idempotent).
func (r *InventoryRepository) SeedIfNotExists(ctx context.Context, productID uint64, availableQty int) error {
	inv := model.Inventory{
		ProductID:    productID,
		AvailableQty: availableQty,
	}
	result := r.db.WithContext(ctx).Where("product_id = ?", productID).FirstOrCreate(&inv)
	if result.Error != nil {
		return fmt.Errorf("seed product %d: %w", productID, result.Error)
	}
	return nil
}
