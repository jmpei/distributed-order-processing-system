package messaging

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MQ struct {
	url  string
	conn *amqp.Connection
	mu   sync.RWMutex
}

func New(url string) (*MQ, error) {
	mq := &MQ{url: url}
	if err := mq.dial(); err != nil {
		return nil, err
	}
	go mq.watch()
	return mq, nil
}

func (mq *MQ) dial() error {
	backoff := time.Second
	for i := 1; i <= 5; i++ {
		conn, err := amqp.Dial(mq.url)
		if err == nil {
			mq.conn = conn
			log.Printf("amqp connected")
			return nil
		}
		log.Printf("amqp connect failed (attempt %d/5): %v, retry in %s", i, err, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("amqp connect failed after 5 attempts")
}

func (mq *MQ) watch() {
	closeCh := mq.conn.NotifyClose(make(chan *amqp.Error, 1))
	amqpErr := <-closeCh
	if amqpErr != nil {
		log.Printf("amqp connection closed: %v, reconnecting...", amqpErr)
	}
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if err := mq.dial(); err != nil {
		panic(fmt.Sprintf("amqp reconnect failed: %v", err))
	}
	go mq.watch()
}

func (mq *MQ) Channel() (*amqp.Channel, error) {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return mq.conn.Channel()
}

func (mq *MQ) Close() {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if mq.conn != nil && !mq.conn.IsClosed() {
		_ = mq.conn.Close()
	}
}
