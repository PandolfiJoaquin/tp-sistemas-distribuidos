package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderQueue interface {
	Send(body []byte) error
}

type amqpSenderQueue struct {
	ch   *amqp.Channel
	name string
}

func NewAmqpQueue(ch *amqp.Channel, name string) SenderQueue {
	return &amqpSenderQueue{ch: ch, name: name}
}

func (q *amqpSenderQueue) Send(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := q.ch.PublishWithContext(
		ctx,
		"",
		q.name,
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

type amqpSenderQueueWithTopic struct {
	ch       *amqp.Channel
	exchange string
	topic    string
}

func NewAmqpQueueWithTopic(ch *amqp.Channel, exchange, topic string) SenderQueue {
	return &amqpSenderQueueWithTopic{ch: ch, exchange: exchange, topic: topic}
}

func (q *amqpSenderQueueWithTopic) Send(body []byte) error {
	err := q.ch.PublishWithContext(
		context.Background(),
		q.exchange,
		q.topic,
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
