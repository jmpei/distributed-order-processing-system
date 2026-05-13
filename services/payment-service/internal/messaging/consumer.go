package messaging

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"
)

type HandlerFunc func(ctx context.Context, msg amqp.Delivery) error

// StartConsumer starts a goroutine that consumes from queue and calls handler.
// On handler error: nack (requeue=false → dead-letter queue).
// On panic: recover, nack.
// On channel close: reconnect up to 5 times, then panic.
//
// `logger` is the per-service base logger; trace_id and saga_id pulled from
// the AMQP headers are bound to it before each message is dispatched, so
// `observability.LoggerFrom(ctx)` inside the handler emits them automatically.
func StartConsumer(ctx context.Context, mq *MQ, queue string, logger *zap.Logger, handler HandlerFunc) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.With(zap.String("queue", queue))

	ch, msgs, err := openConsumerCh(mq, queue)
	if err != nil {
		return err
	}

	go func() {
		currentCh := ch
		currentMsgs := msgs
		for {
			select {
			case <-ctx.Done():
				currentCh.Close()
				return
			case msg, ok := <-currentMsgs:
				if !ok {
					currentCh.Close()
					logger.Warn("channel closed, reconnecting")
					newCh, newMsgs, err := reconnectConsumer(mq, queue, logger)
					if err != nil {
						panic(fmt.Sprintf("consumer %s reconnect failed: %v", queue, err))
					}
					currentCh = newCh
					currentMsgs = newMsgs
					continue
				}
				dispatchMsg(ctx, msg, logger, handler)
			}
		}
	}()

	return nil
}

func dispatchMsg(parent context.Context, msg amqp.Delivery, logger *zap.Logger, handler HandlerFunc) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic in consumer handler, nacking",
				zap.Any("panic", r),
				zap.String("routing_key", msg.RoutingKey),
			)
			_ = msg.Nack(false, false)
		}
	}()

	// Seed ctx with trace_id + saga_id from message headers so the handler's
	// LoggerFrom(ctx) emits them, and any downstream publish carries them on.
	ctx := observability.ExtractAMQPHeaders(parent, logger, msg.Headers)

	if err := handler(ctx, msg); err != nil {
		observability.LoggerFrom(ctx).Error("handler error, nacking",
			zap.String("routing_key", msg.RoutingKey),
			zap.Error(err),
		)
		_ = msg.Nack(false, false)
		return
	}
	_ = msg.Ack(false)
}

func openConsumerCh(mq *MQ, queue string) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("qos: %w", err)
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("consume %s: %w", queue, err)
	}
	return ch, msgs, nil
}

func reconnectConsumer(mq *MQ, queue string, logger *zap.Logger) (*amqp.Channel, <-chan amqp.Delivery, error) {
	backoff := time.Second
	for i := 1; i <= 5; i++ {
		ch, msgs, err := openConsumerCh(mq, queue)
		if err == nil {
			logger.Info("reconnected", zap.Int("attempt", i))
			return ch, msgs, nil
		}
		logger.Warn("reconnect attempt failed", zap.Int("attempt", i), zap.Error(err))
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, nil, fmt.Errorf("reconnect failed after 5 attempts")
}
