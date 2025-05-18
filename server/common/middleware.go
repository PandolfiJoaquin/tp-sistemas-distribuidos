package common

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
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

type Middleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel
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
	return &Middleware{conn: conn, ch: ch}, nil
}

func (m *Middleware) sendToQueue(queueName string, body []byte) error {
	// TODO: Deberia ser publish with context?
	err := m.ch.Publish(
		"",
		queueName, //routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("error sending message: %s", err)
	}
	return nil
}

func (m *Middleware) GetChanToSend(name string) (chan<- []byte, error) {
	queue, err := m.ch.QueueDeclare(name, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue: %s", err)
	}

	chanToSend := make(chan []byte)
	go func() {
		for msg := range chanToSend {

			if err := m.sendToQueue(queue.Name, msg); err != nil {
				//TODO: deberia tener otro canal para devolver el error?
				fmt.Printf("Error sending message: %s", err)
			}
		}
	}()
	return chanToSend, nil
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

func (m *Middleware) GetChanWithTopicToSend(exchange, topic string) (chan<- []byte, error) {
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

	chanToSend := make(chan []byte)
	go func() {
		for msg := range chanToSend {
			if err := m.sendToExchange(exchange, topic, msg); err != nil {
				slog.Error("error sending message", slog.String("error", err.Error()))
			}
		}
	}()
	return chanToSend, nil
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
			//slog.Info("received message", slog.String("topic", topic), slog.String("exchange", exchange))
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

	return nil
}

func (m *Middleware) sendToExchange(exchange string, topic string, msg []byte) error {
	err := m.ch.PublishWithContext(
		context.Background(),
		exchange,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		})
	if err != nil {
		return fmt.Errorf("error sending message: %s", err)
	}
	return nil
}
