package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
)

type SagaRepository struct {
	db *gorm.DB
}

func NewSagaRepository(db *gorm.DB) *SagaRepository {
	return &SagaRepository{db: db}
}

func (r *SagaRepository) Create(ctx context.Context, s *model.SagaState) error {
	if err := r.db.WithContext(ctx).Create(s).Error; err != nil {
		return fmt.Errorf("create saga_state: %w", err)
	}
	return nil
}

func (r *SagaRepository) GetBySagaID(ctx context.Context, sagaID string) (*model.SagaState, error) {
	var s model.SagaState
	if err := r.db.WithContext(ctx).First(&s, "saga_id = ?", sagaID).Error; err != nil {
		return nil, fmt.Errorf("get saga_state %s: %w", sagaID, err)
	}
	return &s, nil
}

func (r *SagaRepository) GetByOrderID(ctx context.Context, orderID uint64) (*model.SagaState, error) {
	var s model.SagaState
	if err := r.db.WithContext(ctx).First(&s, "order_id = ?", orderID).Error; err != nil {
		return nil, fmt.Errorf("get saga_state order %d: %w", orderID, err)
	}
	return &s, nil
}
