package service

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/repository"
)

type PaymentService struct {
	repo repository.PaymentRepoIface
}

func NewPaymentService(repo repository.PaymentRepoIface) *PaymentService {
	return &PaymentService{repo: repo}
}

func (s *PaymentService) CreatePayment(ctx context.Context, p *model.Payment) error {
	p.Status = model.PaymentStatusPending
	p.TransactionID = newUUID()
	if err := s.repo.Create(ctx, p); err != nil {
		return fmt.Errorf("create payment: %w", err)
	}
	return nil
}

func (s *PaymentService) GetByOrderID(ctx context.Context, orderID uint64) (*model.Payment, error) {
	return s.repo.GetByOrderID(ctx, orderID)
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("rand.Read: %v", err)) // crypto/rand failure is unrecoverable
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
