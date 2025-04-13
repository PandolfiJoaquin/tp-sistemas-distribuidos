package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

const nextQueue = "filter-year-q1"
const previousQueue = "movies-to-preprocess"

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

	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		fmt.Printf("error with channel %s: %v", previousQueue, err)
	}

	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		fmt.Printf("error with channel '%s': %v", nextQueue, err)
	}

	go start(previousChan, nextChan)

	forever := make(chan bool)
	<-forever
}

func start(moviesToFilterChan <-chan common.Message, nextFilterChan chan<- []byte) {
	for msg := range moviesToFilterChan {
		var movies []common.Movie
		if err := json.Unmarshal(msg.Body, &movies); err != nil {
			fmt.Printf("Error unmarshalling message: %v", err)
			fmt.Printf("message: %v", msg.Body)
		}
		fmt.Printf("Movies passing through preprocessor: %v\n", movies)
		//TODO: filter movies
		fmt.Printf("Movies preprocessed: %v\n", movies)

		response, err := json.Marshal(movies)
		if err != nil {
			fmt.Printf("Error marshalling movies: %v", err)
			continue
		}
		nextFilterChan <- response
		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
	}
}
