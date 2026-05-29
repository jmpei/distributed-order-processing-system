package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/TomatoesSuck/distributed-order-processing/shared/observability"
)

// PublisherIface is the surface command handlers depend on; allows mocking.
type PublisherIface interface {
	Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error
}

type Publisher struct {
	mq       *MQ
	logger   *zap.Logger
	mu       sync.Mutex
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
}

func NewPublisher(mq *MQ, logger *zap.Logger) *Publisher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Publisher{mq: mq, logger: logger}
}

// channel returns a confirm-mode channel, opening one if needed. Caller must hold p.mu.
func (p *Publisher) channel() (*amqp.Channel, chan amqp.Confirmation, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, p.confirms, nil
	}
	ch, err := p.mq.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("confirm mode: %w", err)
	}
	p.ch = ch
	p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return p.ch, p.confirms, nil
}

// publishLocked publishes one message on the reused channel and waits for its
// confirm. Caller must hold p.mu. On channel error the channel is dropped so
// the next call reopens.
func (p *Publisher) publishLocked(ctx context.Context, exchange, routingKey, messageID string, body []byte, headers amqp.Table) error {
	ch, confirms, err := p.channel()
	if err != nil {
		return err
	}
	if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
	}); err != nil {
		p.drop()
		return fmt.Errorf("publish: %w", err)
	}
	select {
	case c := <-confirms:
		if !c.Ack {
			return fmt.Errorf("broker nacked message")
		}
		return nil
	case <-time.After(5 * time.Second):
		p.drop()
		return fmt.Errorf("publisher confirm timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drop discards the current channel so the next publish reopens. Caller holds p.mu.
func (p *Publisher) drop() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
		p.confirms = nil
	}
}

func (p *Publisher) Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	headers := observability.InjectAMQPHeaders(ctx, amqp.Table{"message_id": messageID})

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		p.mu.Lock()
		err := p.publishLocked(ctx, exchange, routingKey, messageID, body, headers)
		p.mu.Unlock()
		if err == nil {
			return nil
		}
		lastErr = err
		observability.LoggerFrom(ctx).Warn("publish attempt failed",
			zap.Int("attempt", attempt), zap.String("exchange", exchange),
			zap.String("routing_key", routingKey), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}
	return fmt.Errorf("publish failed after 3 attempts: %w", lastErr)
}

// PublishRaw forwards a pre-marshaled body with explicit headers (used to
// republish a failed delivery to the retry queue or DLQ). Unlike Publish it
// does NOT re-marshal or re-inject trace headers — the caller passes the
// original message's headers so trace_id/saga_id and the retry count survive.
func (p *Publisher) PublishRaw(ctx context.Context, exchange, routingKey, messageID string, body []byte, headers amqp.Table) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishLocked(ctx, exchange, routingKey, messageID, body, headers)
}
