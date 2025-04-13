package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "movies-to-preprocess"
const NEXT_STEP = "q1-results"

var batch = []common.Movie{
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
	{
		ID:    "3",
		Title: "El padrino",
		Year:  1980,
		Genre: "Drama",
	},
}

func main() {
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

	moviesToFilterChan, err := middleware.GetChanToSend(PREVIOUS_STEP)
	if err != nil {
		fmt.Printf("error with channel 'movies-to-preprocess': %v", err)
	}

	q1ResultsChan, err := middleware.GetChanToRecv(NEXT_STEP)
	if err != nil {
		fmt.Println(err)
	}

	body, err := json.Marshal(batch)
	if err != nil {
		fmt.Printf("Error marshalling batch: %v", err)
		return
	}

	moviesToFilterChan <- body
	fmt.Println("Message sent.")

	go processMessages(q1ResultsChan)

	forever := make(chan bool)
	<-forever
}

func processMessages(q1ResultsChan <-chan common.Message) {
	for msg := range q1ResultsChan {
		var movies []common.Movie
		if err := json.Unmarshal(msg.Body, &movies); err != nil {
			fmt.Printf("Error unmarshalling message: %v", err)
		}
		fmt.Println("Movies: ", movies)

		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
	}
	return
}
