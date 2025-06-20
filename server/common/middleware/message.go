package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	Body    []byte
	amqpMsg amqp.Delivery
}

func (m *Message) Ack() error {
	if err := m.amqpMsg.Ack(false); err != nil {
		return fmt.Errorf("error acknowledging message: %s", err)
	}
	return nil
}