package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerFunc func(ctx context.Context, msg amqp.Delivery) error

func StartConsumer(ctx context.Context, mq *MQ, queue string, handler HandlerFunc) error {
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
					log.Printf("consumer %s: channel closed, reconnecting...", queue)
					newCh, newMsgs, err := reconnectConsumer(mq, queue)
					if err != nil {
						panic(fmt.Sprintf("consumer %s reconnect failed: %v", queue, err))
					}
					currentCh = newCh
					currentMsgs = newMsgs
					continue
				}
				dispatchMsg(msg, handler)
			}
		}
	}()

	return nil
}

func dispatchMsg(msg amqp.Delivery, handler HandlerFunc) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in consumer handler, nacking: %v", r)
			_ = msg.Nack(false, false)
		}
	}()

	ctx := context.Background()
	if err := handler(ctx, msg); err != nil {
		log.Printf("handler error, nacking (routing_key=%s): %v", msg.RoutingKey, err)
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

func reconnectConsumer(mq *MQ, queue string) (*amqp.Channel, <-chan amqp.Delivery, error) {
	backoff := time.Second
	for i := 1; i <= 5; i++ {
		ch, msgs, err := openConsumerCh(mq, queue)
		if err == nil {
			log.Printf("consumer %s: reconnected (attempt %d)", queue, i)
			return ch, msgs, nil
		}
		log.Printf("consumer %s: reconnect attempt %d/5 failed: %v", queue, i, err)
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, nil, fmt.Errorf("reconnect failed after 5 attempts")
}
