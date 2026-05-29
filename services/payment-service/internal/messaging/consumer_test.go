package messaging

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/TomatoesSuck/distributed-order-processing/shared/amqpretry"
)

type fakeAck struct {
	acked       bool
	nacked      bool
	nackRequeue bool
}

func (f *fakeAck) Ack(tag uint64, multiple bool) error { f.acked = true; return nil }
func (f *fakeAck) Nack(tag uint64, multiple, requeue bool) error {
	f.nacked = true
	f.nackRequeue = requeue
	return nil
}
func (f *fakeAck) Reject(tag uint64, requeue bool) error { return nil }

type mockRepublisher struct {
	calls []rawCall
	err   error
}
type rawCall struct {
	exchange   string
	routingKey string
	headers    amqp.Table
}

func (m *mockRepublisher) PublishRaw(ctx context.Context, exchange, routingKey, messageID string, body []byte, headers amqp.Table) error {
	m.calls = append(m.calls, rawCall{exchange, routingKey, headers})
	return m.err
}

func newDelivery(rk string, headers amqp.Table, ack *fakeAck) amqp.Delivery {
	return amqp.Delivery{Acknowledger: ack, RoutingKey: rk, Headers: headers, Body: []byte("{}"), DeliveryTag: 1}
}

func TestDispatch_Success_Acks(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{}
	dispatchMsg(context.Background(), newDelivery("payment.process", nil, ack), zap.NewNop(), rep,
		"payment.commands", 5, func(ctx context.Context, m amqp.Delivery) error { return nil })
	assert.True(t, ack.acked)
	assert.Empty(t, rep.calls)
}

func TestDispatch_TransientUnderCap_RepublishesToRetryAndAcks(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{}
	dispatchMsg(context.Background(), newDelivery("payment.process", amqp.Table{amqpretry.HeaderRetryCount: int32(1)}, ack),
		zap.NewNop(), rep, "payment.commands", 5,
		func(ctx context.Context, m amqp.Delivery) error { return errors.New("db blip") })
	assert.True(t, ack.acked)
	if assert.Len(t, rep.calls, 1) {
		assert.Equal(t, amqpretry.RetryExchange, rep.calls[0].exchange)
		assert.Equal(t, "payment.process", rep.calls[0].routingKey)
		assert.Equal(t, int32(2), rep.calls[0].headers[amqpretry.HeaderRetryCount])
	}
}

func TestDispatch_TransientAtCap_RepublishesToDLQ(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{}
	dispatchMsg(context.Background(), newDelivery("payment.process", amqp.Table{amqpretry.HeaderRetryCount: int32(5)}, ack),
		zap.NewNop(), rep, "payment.commands", 5,
		func(ctx context.Context, m amqp.Delivery) error { return errors.New("db blip") })
	assert.True(t, ack.acked)
	if assert.Len(t, rep.calls, 1) {
		assert.Equal(t, "", rep.calls[0].exchange)
		assert.Equal(t, "payment.commands.dlq", rep.calls[0].routingKey)
	}
}

func TestDispatch_Permanent_RepublishesToDLQ(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{}
	dispatchMsg(context.Background(), newDelivery("payment.process", nil, ack),
		zap.NewNop(), rep, "payment.commands", 5,
		func(ctx context.Context, m amqp.Delivery) error { return amqpretry.Permanent(errors.New("bad json")) })
	assert.True(t, ack.acked)
	if assert.Len(t, rep.calls, 1) {
		assert.Equal(t, "payment.commands.dlq", rep.calls[0].routingKey)
	}
}

func TestDispatch_RepublishFails_NacksToBroker(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{err: errors.New("broker down")}
	dispatchMsg(context.Background(), newDelivery("payment.process", nil, ack),
		zap.NewNop(), rep, "payment.commands", 5,
		func(ctx context.Context, m amqp.Delivery) error { return errors.New("db blip") })
	assert.True(t, ack.nacked)
	assert.True(t, ack.nackRequeue)
	assert.False(t, ack.acked)
}

// TestDispatch_DLQRepublishFails_NacksWithoutRequeue covers the dead-letter
// branch's fallback: when republishing to the DLQ fails, the message is
// Nack'd with requeue=false so the broker's own DLX handles it. The requeue
// flag here is the opposite of the retry path's, so it is verified explicitly.
func TestDispatch_DLQRepublishFails_NacksWithoutRequeue(t *testing.T) {
	ack := &fakeAck{}
	rep := &mockRepublisher{err: errors.New("broker down")}
	dispatchMsg(context.Background(),
		newDelivery("payment.process", amqp.Table{amqpretry.HeaderRetryCount: int32(5)}, ack),
		zap.NewNop(), rep, "payment.commands", 5,
		func(ctx context.Context, m amqp.Delivery) error { return errors.New("db blip") })
	assert.True(t, ack.nacked)
	assert.False(t, ack.nackRequeue)
	assert.False(t, ack.acked)
}
