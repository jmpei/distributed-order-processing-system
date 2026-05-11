package service

import (
	"context"
	"fmt"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
)

type OrderService struct {
	repo repository.OrderRepoIface
}

func NewOrderService(repo repository.OrderRepoIface) *OrderService {
	return &OrderService{repo: repo}
}

func (s *OrderService) CreateOrder(ctx context.Context, order *model.Order) error {
	order.Status = model.OrderStatusPending
	if err := s.repo.Create(ctx, order); err != nil {
		return fmt.Errorf("create order: %w", err)
	}
	return nil
}

func (s *OrderService) GetOrder(ctx context.Context, id uint64) (*model.Order, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *OrderService) ListOrders(ctx context.Context, userID uint64) ([]model.Order, error) {
	return s.repo.ListByUserID(ctx, userID)
}
