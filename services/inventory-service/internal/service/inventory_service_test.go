package service

import (
	"context"
	"encoding/json"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/TomatoesSuck/distributed-order-processing/inventory-service/internal/model"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

// ─── Mocks ──────────────────────────────────────────────────────────────────

type mockInvRepo struct {
	mock.Mock
}

func (m *mockInvRepo) Create(ctx context.Context, inv *model.Inventory) error {
	return m.Called(ctx, inv).Error(0)
}

func (m *mockInvRepo) GetByProductID(ctx context.Context, productID uint64) (*model.Inventory, error) {
	args := m.Called(ctx, productID)
	inv, _ := args.Get(0).(*model.Inventory)
	return inv, args.Error(1)
}

func (m *mockInvRepo) UpdateAvailableQty(ctx context.Context, productID uint64, qty int) error {
	return m.Called(ctx, productID, qty).Error(0)
}

func (m *mockInvRepo) ReserveAtomic(ctx context.Context, productID, orderID uint64, expectedVersion, qty int) (int64, error) {
	args := m.Called(ctx, productID, orderID, expectedVersion, qty)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockInvRepo) ReleaseAtomic(ctx context.Context, productID, orderID uint64, qty int) (int64, error) {
	args := m.Called(ctx, productID, orderID, qty)
	return args.Get(0).(int64), args.Error(1)
}

type mockLogRepo struct {
	mock.Mock
}

func (m *mockLogRepo) ExistsReserve(ctx context.Context, orderID uint64) (bool, error) {
	args := m.Called(ctx, orderID)
	return args.Bool(0), args.Error(1)
}

func (m *mockLogRepo) ExistsRelease(ctx context.Context, orderID uint64) (bool, error) {
	args := m.Called(ctx, orderID)
	return args.Bool(0), args.Error(1)
}

type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error {
	args := m.Called(ctx, exchange, routingKey, messageID, payload)
	return args.Error(0)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestReserve_Success(t *testing.T) {
	invRepo := &mockInvRepo{}
	logRepo := &mockLogRepo{}
	pub := &mockPublisher{}

	cmd := shared.ReserveInventoryCmd{
		SagaID: "saga-1", OrderID: 1, ProductID: 1001, Quantity: 2,
	}

	logRepo.On("ExistsReserve", mock.Anything, uint64(1)).Return(false, nil)
	invRepo.On("GetByProductID", mock.Anything, uint64(1001)).
		Return(&model.Inventory{ProductID: 1001, AvailableQty: 10, Version: 5}, nil)
	invRepo.On("ReserveAtomic", mock.Anything, uint64(1001), uint64(1), 5, 2).
		Return(int64(1), nil)
	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved, mock.Anything,
		mock.MatchedBy(func(p any) bool {
			e, ok := p.(shared.InventoryReservedEvent)
			return ok && e.Success && e.OrderID == 1 && e.Quantity == 2
		})).Return(nil)

	h := NewInventoryCommandHandler(invRepo, logRepo, pub, 3)
	err := h.handleReserve(context.Background(), cmd)

	assert.NoError(t, err)
	invRepo.AssertExpectations(t)
	logRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
}

// TestReserve_OptimisticLockRetry verifies the handler retries on RowsAffected==0
// and succeeds when the second attempt finds a non-conflicting version.
func TestReserve_OptimisticLockRetry(t *testing.T) {
	invRepo := &mockInvRepo{}
	logRepo := &mockLogRepo{}
	pub := &mockPublisher{}

	cmd := shared.ReserveInventoryCmd{
		SagaID: "saga-1", OrderID: 2, ProductID: 1001, Quantity: 1,
	}

	logRepo.On("ExistsReserve", mock.Anything, uint64(2)).Return(false, nil)

	// Attempt 1: version=5, conflict → RowsAffected=0.
	invRepo.On("GetByProductID", mock.Anything, uint64(1001)).
		Return(&model.Inventory{ProductID: 1001, AvailableQty: 10, Version: 5}, nil).Once()
	invRepo.On("ReserveAtomic", mock.Anything, uint64(1001), uint64(2), 5, 1).
		Return(int64(0), nil).Once()

	// Attempt 2: version=6 (someone else committed), success → RowsAffected=1.
	invRepo.On("GetByProductID", mock.Anything, uint64(1001)).
		Return(&model.Inventory{ProductID: 1001, AvailableQty: 9, Version: 6}, nil).Once()
	invRepo.On("ReserveAtomic", mock.Anything, uint64(1001), uint64(2), 6, 1).
		Return(int64(1), nil).Once()

	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved, mock.Anything,
		mock.MatchedBy(func(p any) bool {
			e, ok := p.(shared.InventoryReservedEvent)
			return ok && e.Success
		})).Return(nil)

	h := NewInventoryCommandHandler(invRepo, logRepo, pub, 5)
	err := h.handleReserve(context.Background(), cmd)

	assert.NoError(t, err)
	invRepo.AssertNumberOfCalls(t, "GetByProductID", 2)
	invRepo.AssertNumberOfCalls(t, "ReserveAtomic", 2)
	pub.AssertNumberOfCalls(t, "Publish", 1)
}

// TestReserve_Idempotent verifies a second call for the same order is short-circuited
// by ExistsReserve and never touches inventory.
func TestReserve_Idempotent(t *testing.T) {
	invRepo := &mockInvRepo{}
	logRepo := &mockLogRepo{}
	pub := &mockPublisher{}

	cmd := shared.ReserveInventoryCmd{
		SagaID: "saga-1", OrderID: 3, ProductID: 1001, Quantity: 2,
	}

	// First call: not processed yet.
	logRepo.On("ExistsReserve", mock.Anything, uint64(3)).Return(false, nil).Once()
	invRepo.On("GetByProductID", mock.Anything, uint64(1001)).
		Return(&model.Inventory{ProductID: 1001, AvailableQty: 10, Version: 0}, nil).Once()
	invRepo.On("ReserveAtomic", mock.Anything, uint64(1001), uint64(3), 0, 2).
		Return(int64(1), nil).Once()

	// Second call: log says already done.
	logRepo.On("ExistsReserve", mock.Anything, uint64(3)).Return(true, nil).Once()

	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved,
		mock.Anything, mock.Anything).Return(nil)

	h := NewInventoryCommandHandler(invRepo, logRepo, pub, 3)

	assert.NoError(t, h.handleReserve(context.Background(), cmd))
	assert.NoError(t, h.handleReserve(context.Background(), cmd))

	invRepo.AssertNumberOfCalls(t, "GetByProductID", 1)
	invRepo.AssertNumberOfCalls(t, "ReserveAtomic", 1)
	pub.AssertNumberOfCalls(t, "Publish", 2) // once after work, once after idempotent short-circuit
}

// TestRelease_Success covers the compensation path.
func TestRelease_Success(t *testing.T) {
	invRepo := &mockInvRepo{}
	logRepo := &mockLogRepo{}
	pub := &mockPublisher{}

	cmd := shared.ReleaseInventoryCmd{
		SagaID: "saga-1", OrderID: 7, ProductID: 1001, Quantity: 2,
	}

	logRepo.On("ExistsRelease", mock.Anything, uint64(7)).Return(false, nil)
	invRepo.On("ReleaseAtomic", mock.Anything, uint64(1001), uint64(7), 2).Return(int64(1), nil)
	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyInventoryReleased, mock.Anything,
		mock.MatchedBy(func(p any) bool {
			e, ok := p.(shared.InventoryReleasedEvent)
			return ok && e.Success && e.OrderID == 7
		})).Return(nil)

	h := NewInventoryCommandHandler(invRepo, logRepo, pub, 3)
	assert.NoError(t, h.handleRelease(context.Background(), cmd))
	invRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
}

// TestHandle_DispatchRouting covers the consumer entry point: routing key → handler dispatch.
func TestHandle_DispatchRouting(t *testing.T) {
	invRepo := &mockInvRepo{}
	logRepo := &mockLogRepo{}
	pub := &mockPublisher{}

	reserveCmd := shared.ReserveInventoryCmd{SagaID: "s", OrderID: 50, ProductID: 1001, Quantity: 1}
	body, _ := json.Marshal(reserveCmd)
	logRepo.On("ExistsReserve", mock.Anything, uint64(50)).Return(false, nil)
	invRepo.On("GetByProductID", mock.Anything, uint64(1001)).
		Return(&model.Inventory{ProductID: 1001, AvailableQty: 10, Version: 0}, nil)
	invRepo.On("ReserveAtomic", mock.Anything, uint64(1001), uint64(50), 0, 1).Return(int64(1), nil)
	pub.On("Publish", mock.Anything, shared.ExchangeEvents, shared.RoutingKeyInventoryReserved,
		mock.Anything, mock.Anything).Return(nil)

	h := NewInventoryCommandHandler(invRepo, logRepo, pub, 3)
	err := h.Handle(context.Background(), amqp.Delivery{
		RoutingKey: shared.RoutingKeyInventoryReserve,
		Body:       body,
	})
	assert.NoError(t, err)

	// Unknown routing key returns nil (no panic).
	err = h.Handle(context.Background(), amqp.Delivery{RoutingKey: "bogus", Body: []byte("{}")})
	assert.NoError(t, err)
}

// TestInventoryService_CRUD covers the simple passthrough wrapper used by the HTTP layer.
func TestInventoryService_CRUD(t *testing.T) {
	invRepo := &mockInvRepo{}

	want := &model.Inventory{ProductID: 9001, AvailableQty: 50}
	invRepo.On("Create", mock.Anything, want).Return(nil).Once()
	invRepo.On("GetByProductID", mock.Anything, uint64(9001)).Return(want, nil).Once()
	invRepo.On("UpdateAvailableQty", mock.Anything, uint64(9001), 30).Return(nil).Once()

	svc := NewInventoryService(invRepo)
	assert.NoError(t, svc.CreateSKU(context.Background(), want))
	got, err := svc.GetByProductID(context.Background(), 9001)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
	assert.NoError(t, svc.UpdateAvailableQty(context.Background(), 9001, 30))

	invRepo.AssertExpectations(t)
}
