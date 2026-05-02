package messaging

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	shared "github.com/TomatoesSuck/distributed-order-processing/shared"
)

func Setup(mq *MQ) error {
	ch, err := mq.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	for _, e := range []struct{ name, kind string }{
		{shared.ExchangeCommands, "direct"},
		{shared.ExchangeEvents, "topic"},
	} {
		if err := ch.ExchangeDeclare(e.name, e.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", e.name, err)
		}
	}

	// payment.commands queue
	dlqArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": shared.QueuePaymentCommands + ".dlq",
	}
	if _, err := ch.QueueDeclare(shared.QueuePaymentCommands, true, false, false, false, dlqArgs); err != nil {
		return fmt.Errorf("declare queue %s: %w", shared.QueuePaymentCommands, err)
	}
	if _, err := ch.QueueDeclare(shared.QueuePaymentCommands+".dlq", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	for _, key := range []string{shared.RoutingKeyPaymentProcess, shared.RoutingKeyPaymentRefund} {
		if err := ch.QueueBind(shared.QueuePaymentCommands, key, shared.ExchangeCommands, false, nil); err != nil {
			return fmt.Errorf("bind %s→%s: %w", key, shared.QueuePaymentCommands, err)
		}
	}

	return nil
}
