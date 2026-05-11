package service

import (
	"context"
	"fmt"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/repository"
)

type InventoryService struct {
	repo repository.InventoryRepoIface
}

func NewInventoryService(repo repository.InventoryRepoIface) *InventoryService {
	return &InventoryService{repo: repo}
}

func (s *InventoryService) GetByProductID(ctx context.Context, productID uint64) (*model.Inventory, error) {
	return s.repo.GetByProductID(ctx, productID)
}

func (s *InventoryService) CreateSKU(ctx context.Context, inv *model.Inventory) error {
	if err := s.repo.Create(ctx, inv); err != nil {
		return fmt.Errorf("create SKU: %w", err)
	}
	return nil
}

func (s *InventoryService) UpdateAvailableQty(ctx context.Context, productID uint64, qty int) error {
	return s.repo.UpdateAvailableQty(ctx, productID, qty)
}
