package main

import (
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/", rabbitUser, rabbitPass))
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
	}
	defer ch.Close()

	q1YearFilterQueue, err := ch.QueueDeclare("q1-year-filter", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	q1ResultsQueue, err := ch.QueueDeclare("q1-results", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	msgs, err := ch.Consume(q1YearFilterQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}
	
	go func() {
		for d := range msgs {
			sendToQ1Results(ch, q1ResultsQueue, d)
			d.Ack(false)
		}
		}()
		
	forever := make(chan bool)
	<-forever
}

func sendToQ1Results(ch *amqp.Channel, q1ResultsQueue amqp.Queue, d amqp.Delivery) {
	ch.Publish("", q1ResultsQueue.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        d.Body,
	})
}
