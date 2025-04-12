package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Movie struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Year  int    `json:"year"`
	Genre string `json:"genre"`
}

var batch = []Movie{
	{
		ID:    "1",
		Title: "Interstellar",
		Year:  2010,
		Genre: "Space",
	},
	{
		ID:    "2",
		Title: "The Dark Knight",
		Year:  2008,
		Genre: "Action",
	},
	{
		ID:    "3",
		Title: "Rata blanca",
		Year:  2011,
		Genre: "Comedy",
	},
}

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

	movesToFilterQueue, err := ch.QueueDeclare("movies-to-filter-production", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	q1ResultsQueue, err := ch.QueueDeclare("q1-results", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	msgs, err := ch.Consume(q1ResultsQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, err := json.Marshal(batch)
	if err != nil {
		fmt.Printf("Error marshalling batch: %v", err)
		return
	}

	err = ch.PublishWithContext(ctx,
		"",     // exchange
		movesToFilterQueue.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Message sent")

	go func() {
		for d := range msgs {
			var movies []Movie
			err = json.Unmarshal(d.Body, &movies)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Movies: ", movies)
			d.Ack(false)
		}
		}()
		
	forever := make(chan bool)
	<-forever
}
