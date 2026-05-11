package service

import (
	"context"
	"encoding/json"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/repository"
	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

// ─── Mocks ──────────────────────────────────────────────────────────────────

type mockSagaRepo struct {
	mock.Mock
}

func (m *mockSagaRepo) Create(ctx context.Context, s *model.SagaState) error {
	return m.Called(ctx, s).Error(0)
}
func (m *mockSagaRepo) GetBySagaID(ctx context.Context, sagaID string) (*model.SagaState, error) {
	args := m.Called(ctx, sagaID)
	s, _ := args.Get(0).(*model.SagaState)
	return s, args.Error(1)
}
func (m *mockSagaRepo) GetByOrderID(ctx context.Context, orderID uint64) (*model.SagaState, error) {
	args := m.Called(ctx, orderID)
	s, _ := args.Get(0).(*model.SagaState)
	return s, args.Error(1)
}
func (m *mockSagaRepo) ListByStatus(ctx context.Context, status string) ([]model.SagaState, error) {
	args := m.Called(ctx, status)
	v, _ := args.Get(0).([]model.SagaState)
	return v, args.Error(1)
}
func (m *mockSagaRepo) List(ctx context.Context) ([]model.SagaState, error) {
	args := m.Called(ctx)
	v, _ := args.Get(0).([]model.SagaState)
	return v, args.Error(1)
}
func (m *mockSagaRepo) CommitInventoryReserved(ctx context.Context, eventID string, event shared.InventoryReservedEvent) (repository.InventoryReservedOutcome, error) {
	args := m.Called(ctx, eventID, event)
	return args.Get(0).(repository.InventoryReservedOutcome), args.Error(1)
}
func (m *mockSagaRepo) CommitPaymentProcessed(ctx context.Context, eventID string, event shared.PaymentProcessedEvent) (repository.PaymentProcessedOutcome, error) {
	args := m.Called(ctx, eventID, event)
	return args.Get(0).(repository.PaymentProcessedOutcome), args.Error(1)
}
func (m *mockSagaRepo) CommitInventoryReleased(ctx context.Context, eventID string, event shared.InventoryReleasedEvent) (repository.InventoryReleasedOutcome, error) {
	args := m.Called(ctx, eventID, event)
	return args.Get(0).(repository.InventoryReleasedOutcome), args.Error(1)
}

type mockOrderRepo struct {
	mock.Mock
}

func (m *mockOrderRepo) Create(ctx context.Context, order *model.Order) error {
	return m.Called(ctx, order).Error(0)
}
func (m *mockOrderRepo) GetByID(ctx context.Context, id uint64) (*model.Order, error) {
	args := m.Called(ctx, id)
	o, _ := args.Get(0).(*model.Order)
	return o, args.Error(1)
}
func (m *mockOrderRepo) ListByUserID(ctx context.Context, userID uint64) ([]model.Order, error) {
	args := m.Called(ctx, userID)
	v, _ := args.Get(0).([]model.Order)
	return v, args.Error(1)
}

type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error {
	return m.Called(ctx, exchange, routingKey, messageID, payload).Error(0)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

// TestRecoverSaga_FromCrashedState verifies that runRecovery reads IN_PROGRESS and
// COMPENSATING sagas from saga_state and re-publishes the right command for each step.
// This simulates the orchestrator booting after a crash mid-saga.
func TestRecoverSaga_FromCrashedState(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	// Two IN_PROGRESS sagas: one stuck at RESERVING_INVENTORY, one at PROCESSING_PAYMENT.
	inProgress := []model.SagaState{
		{SagaID: "saga-1", OrderID: 1, CurrentStep: model.SagaStepReservingInventory, Status: model.SagaStatusInProgress},
		{SagaID: "saga-2", OrderID: 2, CurrentStep: model.SagaStepProcessingPayment, Status: model.SagaStatusInProgress},
	}
	// One COMPENSATING saga at RELEASING_INVENTORY.
	compensating := []model.SagaState{
		{SagaID: "saga-3", OrderID: 3, CurrentStep: model.SagaStepReleasingInventory, Status: model.SagaStatusCompensating},
	}

	sagaRepo.On("ListByStatus", mock.Anything, model.SagaStatusInProgress).Return(inProgress, nil).Once()
	sagaRepo.On("ListByStatus", mock.Anything, model.SagaStatusCompensating).Return(compensating, nil).Once()

	orderRepo.On("GetByID", mock.Anything, uint64(1)).
		Return(&model.Order{BaseModel: model.BaseModel{ID: 1}, ProductID: 1001, Quantity: 2, TotalAmount: 100}, nil).Once()
	orderRepo.On("GetByID", mock.Anything, uint64(2)).
		Return(&model.Order{BaseModel: model.BaseModel{ID: 2}, ProductID: 1001, Quantity: 1, TotalAmount: 50}, nil).Once()
	orderRepo.On("GetByID", mock.Anything, uint64(3)).
		Return(&model.Order{BaseModel: model.BaseModel{ID: 3}, ProductID: 1002, Quantity: 3, TotalAmount: 150}, nil).Once()

	// saga-1: republish ReserveInventoryCmd
	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyInventoryReserve,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ReserveInventoryCmd)
			return ok && c.SagaID == "saga-1" && c.OrderID == 1 && c.Quantity == 2
		})).Return(nil).Once()
	// saga-2: republish ProcessPaymentCmd
	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ProcessPaymentCmd)
			return ok && c.SagaID == "saga-2" && c.OrderID == 2 && c.Amount == 50
		})).Return(nil).Once()
	// saga-3: republish ReleaseInventoryCmd
	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ReleaseInventoryCmd)
			return ok && c.SagaID == "saga-3" && c.OrderID == 3 && c.Quantity == 3
		})).Return(nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)
	o.runRecovery(context.Background())

	sagaRepo.AssertExpectations(t)
	orderRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
	pub.AssertNumberOfCalls(t, "Publish", 3)
}

// TestHandlePaymentFailedEvent_TriggersCompensation verifies that when a payment-failed
// event arrives, onPaymentProcessed publishes a ReleaseInventoryCmd carrying the right
// product_id and quantity for compensation.
func TestHandlePaymentFailedEvent_TriggersCompensation(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	event := shared.PaymentProcessedEvent{
		SagaID:        "saga-99",
		OrderID:       99,
		TransactionID: "tx-99",
		Success:       false,
		Reason:        "SIMULATED_FAILURE",
	}
	eventID := "evt-99"

	// Repo reports compensation needed with the order's product/quantity.
	sagaRepo.On("CommitPaymentProcessed", mock.Anything, eventID, event).
		Return(repository.PaymentProcessedOutcome{
			Skip:       false,
			Compensate: true,
			ProductID:  1001,
			Quantity:   2,
		}, nil).Once()

	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyInventoryRelease,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ReleaseInventoryCmd)
			return ok && c.SagaID == "saga-99" && c.OrderID == 99 && c.ProductID == 1001 && c.Quantity == 2
		})).Return(nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)
	err := o.onPaymentProcessed(context.Background(), event, eventID)

	assert.NoError(t, err)
	sagaRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
}

// TestStartSaga covers the saga kickoff: persist state + publish first command.
func TestStartSaga(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	order := &model.Order{BaseModel: model.BaseModel{ID: 11}, ProductID: 1001, Quantity: 2, TotalAmount: 200}
	sagaRepo.On("Create", mock.Anything, mock.MatchedBy(func(s *model.SagaState) bool {
		return s.OrderID == 11 && s.CurrentStep == model.SagaStepReservingInventory && s.Status == model.SagaStatusInProgress
	})).Return(nil).Once()
	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyInventoryReserve,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ReserveInventoryCmd)
			return ok && c.OrderID == 11 && c.ProductID == 1001 && c.Quantity == 2
		})).Return(nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)
	assert.NoError(t, o.StartSaga(context.Background(), order))
	sagaRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
}

// TestOnInventoryReserved_Success covers the happy-path transition to PROCESSING_PAYMENT.
func TestOnInventoryReserved_Success(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	event := shared.InventoryReservedEvent{
		SagaID: "saga-7", OrderID: 7, ProductID: 1001, Quantity: 2, Success: true,
	}
	sagaRepo.On("CommitInventoryReserved", mock.Anything, "evt-7", event).
		Return(repository.InventoryReservedOutcome{Amount: 200}, nil).Once()
	pub.On("Publish", mock.Anything, shared.ExchangeCommands, shared.RoutingKeyPaymentProcess,
		mock.Anything, mock.MatchedBy(func(p any) bool {
			c, ok := p.(shared.ProcessPaymentCmd)
			return ok && c.SagaID == "saga-7" && c.Amount == 200
		})).Return(nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)
	assert.NoError(t, o.onInventoryReserved(context.Background(), event, "evt-7"))
	sagaRepo.AssertExpectations(t)
	pub.AssertExpectations(t)
}

// TestOnInventoryReleased_Success covers the COMPENSATED terminal transition.
func TestOnInventoryReleased_Success(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	event := shared.InventoryReleasedEvent{SagaID: "saga-8", OrderID: 8, Success: true}
	sagaRepo.On("CommitInventoryReleased", mock.Anything, "evt-8", event).
		Return(repository.InventoryReleasedOutcome{}, nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)
	assert.NoError(t, o.onInventoryReleased(context.Background(), event, "evt-8"))
	sagaRepo.AssertExpectations(t)
	pub.AssertNotCalled(t, "Publish")
}

// TestHandleEvent_DispatchRouting covers the consumer entry point's routing-key switch.
func TestHandleEvent_DispatchRouting(t *testing.T) {
	sagaRepo := &mockSagaRepo{}
	orderRepo := &mockOrderRepo{}
	pub := &mockPublisher{}

	event := shared.PaymentProcessedEvent{SagaID: "s", OrderID: 5, Success: true}
	body, _ := json.Marshal(event)

	sagaRepo.On("CommitPaymentProcessed", mock.Anything, "evt-x", event).
		Return(repository.PaymentProcessedOutcome{}, nil).Once()

	o := NewSagaOrchestrator(sagaRepo, orderRepo, pub)

	// Routing key → onPaymentProcessed.
	err := o.HandleEvent(context.Background(), amqp.Delivery{
		MessageId:  "evt-x",
		RoutingKey: shared.RoutingKeyPaymentProcessed,
		Body:       body,
	})
	assert.NoError(t, err)

	// Missing message_id → silently skipped, no error.
	err = o.HandleEvent(context.Background(), amqp.Delivery{
		RoutingKey: shared.RoutingKeyPaymentProcessed,
		Body:       body,
	})
	assert.NoError(t, err)

	// Unknown routing key → no error.
	err = o.HandleEvent(context.Background(), amqp.Delivery{
		MessageId:  "evt-y",
		RoutingKey: "bogus",
		Body:       []byte("{}"),
	})
	assert.NoError(t, err)
}

// TestOrderService_CRUD covers the simple HTTP-facing wrapper.
func TestOrderService_CRUD(t *testing.T) {
	orderRepo := &mockOrderRepo{}

	o := &model.Order{UserID: 7, ProductID: 1001, Quantity: 2, TotalAmount: 100}
	orderRepo.On("Create", mock.Anything, mock.MatchedBy(func(arg *model.Order) bool {
		// CreateOrder sets status=PENDING before persisting.
		return arg.UserID == 7 && arg.Status == model.OrderStatusPending
	})).Return(nil).Once()
	orderRepo.On("GetByID", mock.Anything, uint64(42)).Return(o, nil).Once()
	orderRepo.On("ListByUserID", mock.Anything, uint64(7)).Return([]model.Order{*o}, nil).Once()

	svc := NewOrderService(orderRepo)
	assert.NoError(t, svc.CreateOrder(context.Background(), o))
	got, err := svc.GetOrder(context.Background(), 42)
	assert.NoError(t, err)
	assert.Equal(t, o, got)
	list, err := svc.ListOrders(context.Background(), 7)
	assert.NoError(t, err)
	assert.Len(t, list, 1)

	orderRepo.AssertExpectations(t)
}
