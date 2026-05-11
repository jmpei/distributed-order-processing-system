package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherIface is the surface SagaOrchestrator depends on; allows mocking.
type PublisherIface interface {
	Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error
}

type Publisher struct {
	mq *MQ
}

func NewPublisher(mq *MQ) *Publisher {
	return &Publisher{mq: mq}
}

func (p *Publisher) Publish(ctx context.Context, exchange, routingKey, messageID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if err := p.publishOnce(ctx, exchange, routingKey, messageID, body); err != nil {
			lastErr = err
			log.Printf("publish attempt %d/3 failed (exchange=%s key=%s): %v", attempt, exchange, routingKey, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("publish failed after 3 attempts: %w", lastErr)
}

func (p *Publisher) publishOnce(ctx context.Context, exchange, routingKey, messageID string, body []byte) error {
	ch, err := p.mq.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm mode: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		ContentType:  "application/json",
		Body:         body,
		Headers:      amqp.Table{"message_id": messageID},
	}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	select {
	case c := <-confirms:
		if !c.Ack {
			return fmt.Errorf("broker nacked message")
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("publisher confirm timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}
