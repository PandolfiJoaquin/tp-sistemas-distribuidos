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

	q, err := ch.QueueDeclare("test-queue", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}
	
	go func() {
		for d := range msgs {
			fmt.Println("Received a message: ", string(d.Body))
			d.Ack(false)	
		}
		}()
		
	forever := make(chan bool)
	<-forever
}
