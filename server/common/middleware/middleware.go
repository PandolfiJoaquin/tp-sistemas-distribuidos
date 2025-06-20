package middleware

import (
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Middleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	hc   *HealthCheck
}

func NewMiddleware(rabbitUser string, rabbitPass string, host string) (*Middleware, error) {
	slog.Info("creating middleware", slog.String("dialing", "amqp://"+rabbitUser+":"+rabbitPass+"@"+host+":5672"))

	conn, err := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPass + "@" + host + ":5672")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %s", err)
	}

	hc, err := startHealthCheck()
	if err != nil {
		return nil, fmt.Errorf("failed to start health check: %s", err)
	}

	return &Middleware{conn: conn, ch: ch, hc: hc}, nil
}

func (m *Middleware) GetQueueToSend(name string) (SenderQueue, error) {
	queue, err := m.ch.QueueDeclare(name, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	return NewAmqpQueue(m.ch, queue.Name), nil
}

func (m *Middleware) GetChanToRecv(name string) (<-chan Message, error) {
	queue, err := m.ch.QueueDeclare(name, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	amqpChan, err := m.ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %s", err)
	}

	inboxChan := make(chan Message)
	go func() {
		for msg := range amqpChan {
			inboxChan <- Message{msg.Body, msg}
		}
	}()

	return inboxChan, nil
}

func (m *Middleware) GetQueueWithTopicToSend(exchange, topic string) (SenderQueue, error) {
	if err := m.ch.ExchangeDeclare(exchange, "topic", false, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("error declaring exchange: %s", err)
	}

	q, err := m.ch.QueueDeclare(exchange+"-"+topic, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	if err := m.ch.QueueBind(q.Name, topic, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("error binding queue: %s", err)
	}

	return NewAmqpQueueWithTopic(m.ch, exchange, topic), nil
}

func (m *Middleware) GetFanoutQueueToSend(exchange string) (SenderQueue, error) {
	if err := m.ch.ExchangeDeclare(exchange, "fanout", false, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("error declaring exchange: %s", err)
	}

	return NewAmqpQueueWithFanout(m.ch, exchange), nil
}

func (m *Middleware) GetChanWithTopicToRecv(exchange, topic string) (<-chan Message, error) {
	if err := m.ch.ExchangeDeclare(exchange, "topic", false, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("error declaring exchange: %s", err)
	}

	q, err := m.ch.QueueDeclare(exchange+"-"+topic, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	if err := m.ch.QueueBind(q.Name, topic, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("error binding queue: %s", err)
	}

	amqpChan, err := m.ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %s", err)
	}

	inboxChan := make(chan Message)
	go func() {
		for msg := range amqpChan {
			inboxChan <- Message{msg.Body, msg}
		}
	}()

	return inboxChan, nil
}

func (m *Middleware) GetChanWithFanoutToRecv(exchange, queueName string) (<-chan Message, error) {
	if err := m.ch.ExchangeDeclare(exchange, "fanout", false, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("error declaring exchange: %s", err)
	}

	q, err := m.ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	if err := m.ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		return nil, fmt.Errorf("error binding queue: %s", err)
	}

	amqpChan, err := m.ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %s", err)
	}

	inboxChan := make(chan Message)
	go func() {
		for msg := range amqpChan {
			inboxChan <- Message{msg.Body, msg}
		}
	}()

	return inboxChan, nil
}

func (m *Middleware) Close() error {
	if err := m.ch.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %s", err)
	}
	if err := m.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	if err := m.hc.Stop(); err != nil {
		return fmt.Errorf("failed to close health check: %s", err)
	}

	return nil
}
