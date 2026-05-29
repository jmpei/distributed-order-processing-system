package messaging

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
	"github.com/TomatoesSuck/distributed-order-processing/shared/amqpretry"
)

// Setup declares all exchanges and the queues this service consumes from.
// All declarations are idempotent; safe to call on every startup.
func Setup(mq *MQ) error {
	ch, err := mq.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	// Exchanges
	for _, e := range []struct{ name, kind string }{
		{shared.ExchangeCommands, "direct"},
		{shared.ExchangeEvents, "topic"},
	} {
		if err := ch.ExchangeDeclare(e.name, e.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", e.name, err)
		}
	}

	// order.events queue (this service consumes from it)
	dlqArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": shared.QueueOrderEvents + ".dlq",
	}
	if _, err := ch.QueueDeclare(shared.QueueOrderEvents, true, false, false, false, dlqArgs); err != nil {
		return fmt.Errorf("declare queue %s: %w", shared.QueueOrderEvents, err)
	}
	if _, err := ch.QueueDeclare(shared.QueueOrderEvents+".dlq", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq %s: %w", shared.QueueOrderEvents+".dlq", err)
	}
	if err := ch.QueueBind(shared.QueueOrderEvents, "#", shared.ExchangeEvents, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", shared.QueueOrderEvents, err)
	}

	// Retry exchange (shared by all services; idempotent).
	if err := ch.ExchangeDeclare(amqpretry.RetryExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare retry exchange: %w", err)
	}
	// order.events.retry: hold for RetryTTLMillis, then dead-letter back
	// to saga.events under the message's ORIGINAL routing key (no
	// x-dead-letter-routing-key set, so the original key is preserved).
	retryArgs := amqp.Table{
		"x-message-ttl":          int32(amqpretry.RetryTTLMillis),
		"x-dead-letter-exchange": shared.ExchangeEvents,
	}
	if _, err := ch.QueueDeclare(shared.QueueOrderEvents+".retry", true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}
	for _, key := range []string{shared.RoutingKeyInventoryReserved, shared.RoutingKeyInventoryReleased, shared.RoutingKeyPaymentProcessed} {
		if err := ch.QueueBind(shared.QueueOrderEvents+".retry", key, amqpretry.RetryExchange, false, nil); err != nil {
			return fmt.Errorf("bind retry %s: %w", key, err)
		}
	}

	return nil
}
