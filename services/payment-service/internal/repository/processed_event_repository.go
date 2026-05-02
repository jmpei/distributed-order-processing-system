package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
)

type ProcessedEventRepository struct {
	db *gorm.DB
}

func NewProcessedEventRepository(db *gorm.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

// IsProcessed returns true when eventID exists in processed_events.
func (r *ProcessedEventRepository) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	var pe model.ProcessedEvent
	err := r.db.WithContext(ctx).First(&pe, "event_id = ?", eventID).Error
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("check processed_event: %w", err)
}

// MarkProcessed inserts eventID into processed_events within the provided tx.
func MarkProcessed(tx *gorm.DB, eventID string) error {
	if err := tx.Create(&model.ProcessedEvent{
		EventID:    eventID,
		ConsumedAt: time.Now().UTC(),
	}).Error; err != nil {
		return fmt.Errorf("mark processed_event: %w", err)
	}
	return nil
}
