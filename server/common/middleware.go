package common

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Middleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	queues map[string]*amqp.Queue
}

func NewMiddleware(rabbitUser string, rabbitPass string) (*Middleware, error) {
	conn, err := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPass + "@rabbitmq:5672/")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %s", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %s", err)
	}
	
	return &Middleware{conn: conn, ch: ch}, nil
}

func (m *Middleware) DeclareQueue(name string) error {
	if _, ok := m.queues[name]; ok {
		return fmt.Errorf("queue already exists")
	}
	
	queue, err := m.ch.QueueDeclare(name, false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring queue: %s", err)
	}
	m.queues[name] = &queue
	return nil
}

func (m *Middleware) Close() error {
	if err := m.ch.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %s", err)
	}
	if err := m.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}
	return nil
}

// func (m *Middleware) ReceiveMessage(queueName string, exchange string, routingKey string) error {
// }
