package main

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/", rabbitUser, rabbitPass))
	if err != nil {
		fmt.Println(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	ch, err := conn.Channel()	
	if err != nil {
		fmt.Println(err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	q, err := ch.QueueDeclare("test-queue", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello World!"
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Message sent")

	
}
