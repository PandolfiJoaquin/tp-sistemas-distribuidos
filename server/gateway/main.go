package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
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
	//create connection
	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		fmt.Println(err)
	}

	defer func() {
		if err := middleware.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	//create queue to write into (movies-to-filter-production)
	moviesToFilterChan, err := middleware.GetChanToSend("movies-to-filter-production")
	//movesToFilterQueue, err := ch.QueueDeclare("movies-to-filter-production", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	//create queue to read from (q1-results)
	q1ResultsChan, err := middleware.GetChanToRecv("q1-results")
	//q1ResultsQueue, err := ch.QueueDeclare("q1-results", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
	}

	body, err := json.Marshal(batch)
	if err != nil {
		fmt.Printf("Error marshalling batch: %v", err)
		return
	}
	//send message
	moviesToFilterChan <- body
	fmt.Println("Message sent.")
	//read message
	go func() {
		for msg := range q1ResultsChan {
			var movies []Movie
			err = json.Unmarshal(msg.Body, &movies)
			if err != nil {
				fmt.Printf("Error unmarshalling message: %v", err)
			}
			fmt.Println("Movies: ", movies)

			if err := msg.Ack(); err != nil {
				fmt.Printf("Error acknowledging message: %v", err)
			}
		}
	}()

	forever := make(chan bool)
	<-forever
}
