package model

import "time"

const (
	InventoryActionReserve = "RESERVE"
	InventoryActionRelease = "RELEASE"
)

type InventoryLog struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	ProductID uint64    `gorm:"not null"                 json:"product_id"`
	OrderID   uint64    `gorm:"not null;uniqueIndex:idx_order_action" json:"order_id"`
	Action    string    `gorm:"type:varchar(16);not null;uniqueIndex:idx_order_action" json:"action"`
	Quantity  int       `gorm:"not null"                 json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}
