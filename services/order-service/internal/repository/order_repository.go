package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
)

// OrderRepoIface is the surface SagaOrchestrator depends on; allows mocking.
type OrderRepoIface interface {
	Create(ctx context.Context, order *model.Order) error
	GetByID(ctx context.Context, id uint64) (*model.Order, error)
	ListByUserID(ctx context.Context, userID uint64) ([]model.Order, error)
}

type OrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

func (r *OrderRepository) Create(ctx context.Context, order *model.Order) error {
	if err := r.db.WithContext(ctx).Create(order).Error; err != nil {
		return fmt.Errorf("create order: %w", err)
	}
	return nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id uint64) (*model.Order, error) {
	var order model.Order
	if err := r.db.WithContext(ctx).First(&order, id).Error; err != nil {
		return nil, fmt.Errorf("get order %d: %w", id, err)
	}
	return &order, nil
}

func (r *OrderRepository) ListByUserID(ctx context.Context, userID uint64) ([]model.Order, error) {
	var orders []model.Order
	if err := r.db.WithContext(ctx).Where("user_id = ?", userID).Find(&orders).Error; err != nil {
		return nil, fmt.Errorf("list orders user %d: %w", userID, err)
	}
	return orders, nil
}
