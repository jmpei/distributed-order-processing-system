package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/payment-service/internal/model"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

// ─── Mocks ──────────────────────────────────────────────────────────────────

type mockPaymentRepo struct {
	mock.Mock
}

func (m *mockPaymentRepo) Create(ctx context.Context, p *model.Payment) error {
	return m.Called(ctx, p).Error(0)
}

func (m *mockPaymentRepo) GetByOrderID(ctx context.Context, orderID uint64) (*model.Payment, error) {
	args := m.Called(ctx, orderID)
	p, _ := args.Get(0).(*model.Payment)
	return p, args.Error(1)
}

func (m *mockPaymentRepo) CreatePaymentWithEvent(ctx context.Context, p *model.Payment, eventID string) error {
	args := m.Called(ctx, p, eventID)
	return args.Error(0)
}

type mockEventRepo struct {
	mock.Mock
}

func (m *mockEventRepo) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	args := m.Called(ctx, eventID)
	return args.Bool(0), args.Error(1)
}

type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error {
	args := m.Called(ctx, exchange, routingKey, messageID, payload)
	return args.Error(0)
}

// ─── Test ──────────────────────────────────────────────────────────────────

// TestProcess_Idempotent verifies the recovery re-publish path is a no-op via the
// *backstop*, not the message-id fast path. A recovery re-publish carries a fresh
// msg_id (here "msg-2"), so the processed_events(message_id) fast path never matches
// it — the handler falls through to GetByOrderID and the payments.order_id UNIQUE /
// existing-row check, which makes the second pass take the idempotent branch: no
// second payment row, no second CreatePaymentWithEvent. This is the payment-side
// proof that a re-publish with a new message_id does not double-charge.
func TestProcess_Idempotent(t *testing.T) {
	paymentRepo := &mockPaymentRepo{}
	eventRepo := &mockEventRepo{}
	pub := &mockPublisher{}

	cmd := shared.ProcessPaymentCmd{
		SagaID:  "saga-1",
		OrderID: 42,
		Amount:  199.99,
	}

	// First call: msg_id "msg-1" not seen, no payment row yet, create it.
	eventRepo.On("IsProcessed", mock.Anything, "msg-1").Return(false, nil).Once()
	paymentRepo.On("GetByOrderID", mock.Anything, uint64(42)).
		Return((*model.Payment)(nil), gorm.ErrRecordNotFound).Once()
	paymentRepo.On("CreatePaymentWithEvent", mock.Anything,
		mock.MatchedBy(func(p *model.Payment) bool {
			return p.OrderID == 42 && p.Amount == 199.99 && p.Status == model.PaymentStatusSuccess
		}), "msg-1").Return(nil).Once()

	// Second call: different msg_id, but order 42 already has a payment row.
	eventRepo.On("IsProcessed", mock.Anything, "msg-2").Return(false, nil).Once()
	paymentRepo.On("GetByOrderID", mock.Anything, uint64(42)).
		Return(&model.Payment{
			OrderID:       42,
			Amount:        199.99,
			Status:        model.PaymentStatusSuccess,
			TransactionID: "existing-tx-id",
		}, nil).Once()
	// CreatePaymentWithEvent must NOT be called on the second pass.

	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyPaymentProcessed,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			e, ok := p.(shared.PaymentProcessedEvent)
			return ok && e.OrderID == 42 && e.Success
		})).Return(nil)

	h := NewPaymentCommandHandler(paymentRepo, eventRepo, pub, 0.0)

	assert.NoError(t, h.handleProcess(context.Background(), cmd, "msg-1"))
	assert.NoError(t, h.handleProcess(context.Background(), cmd, "msg-2"))

	// Exactly one payment row created across two deliveries.
	paymentRepo.AssertNumberOfCalls(t, "CreatePaymentWithEvent", 1)
	paymentRepo.AssertNumberOfCalls(t, "GetByOrderID", 2)
	pub.AssertNumberOfCalls(t, "Publish", 2)
}

// TestPaymentService_CRUD covers the simple wrapper used by the HTTP layer.
func TestPaymentService_CRUD(t *testing.T) {
	paymentRepo := &mockPaymentRepo{}

	p := &model.Payment{OrderID: 9001, Amount: 50}
	paymentRepo.On("Create", mock.Anything, mock.MatchedBy(func(arg *model.Payment) bool {
		// CreatePayment sets status=PENDING and generates a transaction_id.
		return arg.OrderID == 9001 && arg.Status == model.PaymentStatusPending && arg.TransactionID != ""
	})).Return(nil).Once()
	paymentRepo.On("GetByOrderID", mock.Anything, uint64(9001)).Return(p, nil).Once()

	svc := NewPaymentService(paymentRepo)
	assert.NoError(t, svc.CreatePayment(context.Background(), p))
	got, err := svc.GetByOrderID(context.Background(), 9001)
	assert.NoError(t, err)
	assert.Equal(t, p, got)

	paymentRepo.AssertExpectations(t)
}
