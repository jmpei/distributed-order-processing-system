package model

import "time"

type ProcessedEvent struct {
	EventID    string    `gorm:"primaryKey;type:varchar(64)" json:"event_id"`
	ConsumedAt time.Time `gorm:"not null"                    json:"consumed_at"`
}
