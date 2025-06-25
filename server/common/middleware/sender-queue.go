package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"

	// "math/rand"
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
	if err := ch.Confirm(false); err != nil {
		slog.Error("Failed to enable publisher confirms", slog.String("error", err.Error()))
	}
	return &amqpSenderQueue{ch: ch, name: name}
}

func (q *amqpSenderQueue) Send(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := q.sendMessage(ctx, body); err != nil {
		return err
	}

	// DUPLICATE: 10% probability of resending
	if rand.Float64() < 0.1 {
		slog.Info("Duplicating message due to 10% probability")
		return q.sendMessage(ctx, body)
	}

	return nil
}

func (q *amqpSenderQueue) sendMessage(ctx context.Context, body []byte) error {
	confirmChan, err := q.ch.PublishWithDeferredConfirmWithContext(
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

	if !confirmChan.Wait() {
		return fmt.Errorf("message was not confirmed by RabbitMQ")
	}

	return nil
}

type amqpSenderQueueWithTopic struct {
	ch       *amqp.Channel
	exchange string
	topic    string
}

func NewAmqpQueueWithTopic(ch *amqp.Channel, exchange, topic string) SenderQueue {
	if err := ch.Confirm(false); err != nil {
		slog.Error("Failed to enable publisher confirms", slog.String("error", err.Error()))
	}
	return &amqpSenderQueueWithTopic{ch: ch, exchange: exchange, topic: topic}
}

func (q *amqpSenderQueueWithTopic) Send(body []byte) error {
	if err := q.sendMessage(body); err != nil {
		return err
	}

	// DUPLICATE: 10% probability of resending
	if rand.Float64() < 0.1 {
		slog.Info("Duplicating message due to 10% probability")
		return q.sendMessage(body)
	}

	return nil
}

func (q *amqpSenderQueueWithTopic) sendMessage(body []byte) error {
	confirmChan, err := q.ch.PublishWithDeferredConfirmWithContext(
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

	if !confirmChan.Wait() {
		return fmt.Errorf("message was not confirmed by RabbitMQ")
	}

	return nil
}

type amqpFanoutSenderQueue struct {
	ch       *amqp.Channel
	exchange string
}

func NewAmqpQueueWithFanout(ch *amqp.Channel, exchange string) SenderQueue {
	if err := ch.Confirm(false); err != nil {
		slog.Error("Failed to enable publisher confirms", slog.String("error", err.Error()))
	}
	return &amqpFanoutSenderQueue{ch: ch, exchange: exchange}
}

func (q *amqpFanoutSenderQueue) Send(body []byte) error {
	if err := q.sendMessage(body); err != nil {
		return err
	}

	// DUPLICATE: 10% probability of resending
	if true /*rand.Float64() < 0.1*/ {
		slog.Info("Duplicating message due to 10% probability")
		return q.sendMessage(body)
	}

	return nil
}

func (q *amqpFanoutSenderQueue) sendMessage(body []byte) error {
	confirmChan, err := q.ch.PublishWithDeferredConfirmWithContext(
		context.Background(),
		q.exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		return fmt.Errorf("error sending message: %s", err)
	}

	if !confirmChan.Wait() {
		return fmt.Errorf("message was not confirmed by RabbitMQ")
	}

	return nil
}
