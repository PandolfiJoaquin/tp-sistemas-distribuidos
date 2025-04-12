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
			sendToQ1(ch, q1Queue, d)
			d.Ack(false)
		}
		}()
		
	forever := make(chan bool)
	<-forever
}

func sendToQ1(ch *amqp.Channel, q1Queue amqp.Queue, d amqp.Delivery) {
	ch.Publish("", q1Queue.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        d.Body,
	})
}
