package messaging

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/TomatoesSuck/distributed-order-processing/shared/amqpretry"
	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"
)

type HandlerFunc func(ctx context.Context, msg amqp.Delivery) error

// Republisher forwards a failed delivery to the retry queue or DLQ.
// Satisfied by *Publisher.
type Republisher interface {
	PublishRaw(ctx context.Context, exchange, routingKey, messageID string, body []byte, headers amqp.Table) error
}

// Consumer tunables — kept identical across all three services so capacity
// reasoning ("3 × consumerWorkers concurrent handlers DB-side") stays simple.
// prefetch >= consumerWorkers so workers never starve while waiting on broker.
const (
	consumerWorkers  = 16
	consumerPrefetch = 32
)

// StartConsumer reads from queue and fans deliveries out to a pool of
// consumerWorkers goroutines so handler latency doesn't cap throughput at
// `1 / handler_latency` msg/s. One distributor goroutine owns the AMQP
// delivery channel (incl. reconnect logic); workers pull from a buffered
// in-process chan.
//
// `logger` is the per-service base logger; trace_id and saga_id pulled from
// the AMQP headers are bound to it before each message is dispatched, so
// `observability.LoggerFrom(ctx)` inside the handler emits them automatically.
func StartConsumer(ctx context.Context, mq *MQ, queue string, logger *zap.Logger, handler HandlerFunc, rep Republisher, maxRetries int) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.With(zap.String("queue", queue))

	ch, msgs, err := openConsumerCh(mq, queue)
	if err != nil {
		return err
	}

	work := make(chan amqp.Delivery, consumerWorkers)

	// Worker pool — each goroutine handles one delivery at a time, acks
	// independently. amqp091 channel is goroutine-safe for ack/nack.
	for i := 0; i < consumerWorkers; i++ {
		go func() {
			for msg := range work {
				dispatchMsg(ctx, msg, logger, rep, queue, maxRetries, handler)
			}
		}()
	}

	// Distributor — owns the AMQP delivery channel + reconnect, fans each
	// delivery into `work` so RabbitMQ flow control (via prefetch) backs up
	// here instead of in the workers.
	go func() {
		defer close(work)
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
				work <- msg
			}
		}
	}()

	return nil
}

func dispatchMsg(parent context.Context, msg amqp.Delivery, logger *zap.Logger, rep Republisher, queue string, maxRetries int, handler HandlerFunc) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic in consumer handler, dead-lettering via broker",
				zap.Any("panic", r), zap.String("routing_key", msg.RoutingKey))
			_ = msg.Nack(false, false) // broker DLX → <queue>.dlq
		}
	}()

	ctx := observability.ExtractAMQPHeaders(parent, logger, msg.Headers)
	log := observability.LoggerFrom(ctx)

	err := handler(ctx, msg)
	if err == nil {
		_ = msg.Ack(false)
		return
	}

	retryCount := amqpretry.RetryCount(msg.Headers)
	switch amqpretry.Decide(err, retryCount, maxRetries) {
	case amqpretry.ActionRetry:
		headers := cloneHeaders(msg.Headers)
		headers[amqpretry.HeaderRetryCount] = int32(retryCount + 1)
		log.Warn("handler error, scheduling retry",
			zap.String("routing_key", msg.RoutingKey),
			zap.Int("retry_count", retryCount+1),
			zap.Int("max", maxRetries),
			zap.Error(err))
		if perr := rep.PublishRaw(ctx, amqpretry.RetryExchange, msg.RoutingKey, msg.MessageId, msg.Body, headers); perr != nil {
			log.Error("republish to retry failed, nacking for broker redelivery", zap.Error(perr))
			_ = msg.Nack(false, true)
			return
		}
		_ = msg.Ack(false)
	default: // ActionDeadLetter
		log.Error("handler error, dead-lettering",
			zap.String("routing_key", msg.RoutingKey),
			zap.Int("retry_count", retryCount),
			zap.Bool("permanent", amqpretry.IsPermanent(err)),
			zap.Error(err))
		if perr := rep.PublishRaw(ctx, "", queue+".dlq", msg.MessageId, msg.Body, msg.Headers); perr != nil {
			log.Error("republish to dlq failed, nacking to broker dlx", zap.Error(perr))
			_ = msg.Nack(false, false)
			return
		}
		_ = msg.Ack(false)
	}
}

// cloneHeaders copies an amqp.Table so mutating the retry count never touches
// the original delivery's headers. Returns a fresh empty table for nil input.
func cloneHeaders(h amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range h {
		out[k] = v
	}
	return out
}

func openConsumerCh(mq *MQ, queue string) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := mq.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(consumerPrefetch, 0, false); err != nil {
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
