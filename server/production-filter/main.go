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
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(ch)

	moviesToFilterQueue, err := ch.QueueDeclare("movies-to-filter-production", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	q1Queue, err := ch.QueueDeclare("q1-year-filter", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	msgsMovies, err := ch.Consume(moviesToFilterQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		for d := range msgsMovies {
			fmt.Printf("Received a message: %s\n", d.Body)
			sendToQ1(ch, q1Queue, d)
			fmt.Println("Message sent to q1.")
			if err := d.Ack(false); err != nil {
				fmt.Printf("Error acknowledging message: %v", err)
			}
		}
	}()

	forever := make(chan bool)
	<-forever
}

func sendToQ1(ch *amqp.Channel, q1Queue amqp.Queue, d amqp.Delivery) {
	if err := ch.Publish("", q1Queue.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        d.Body,
	}); err != nil {
		fmt.Printf("Error publishing message to q1: %v", err)
	}
}
