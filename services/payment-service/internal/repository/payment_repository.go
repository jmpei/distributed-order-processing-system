package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
)

// PaymentRepoIface is the surface PaymentCommandHandler and PaymentService depend on.
type PaymentRepoIface interface {
	Create(ctx context.Context, p *model.Payment) error
	GetByOrderID(ctx context.Context, orderID uint64) (*model.Payment, error)
	CreatePaymentWithEvent(ctx context.Context, p *model.Payment, eventID string) error
}

type PaymentRepository struct {
	db *gorm.DB
}

func NewPaymentRepository(db *gorm.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) Create(ctx context.Context, p *model.Payment) error {
	if err := r.db.WithContext(ctx).Create(p).Error; err != nil {
		return fmt.Errorf("create payment: %w", err)
	}
	return nil
}

func (r *PaymentRepository) GetByOrderID(ctx context.Context, orderID uint64) (*model.Payment, error) {
	var p model.Payment
	if err := r.db.WithContext(ctx).Where("order_id = ?", orderID).First(&p).Error; err != nil {
		return nil, fmt.Errorf("get payment order %d: %w", orderID, err)
	}
	return &p, nil
}

// CreatePaymentWithEvent inserts the payment row and the processed_event mark in one transaction,
// so dedup state moves atomically with the payment record.
func (r *PaymentRepository) CreatePaymentWithEvent(ctx context.Context, p *model.Payment, eventID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(p).Error; err != nil {
			return fmt.Errorf("create payment: %w", err)
		}
		return MarkProcessed(tx, eventID)
	})
}
