package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

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

	moviesToFilterChan, err := middleware.GetChanToRecv("movies-to-filter-production")
	if err != nil {
		fmt.Printf("error with channel 'movies-to-filter-production': %v", err)
	}

	nextFilterChan, err := middleware.GetChanToSend("q1-year-filter")
	if err != nil {
		fmt.Printf("error with channel 'q1-year-filter': %v", err)
	}

	go start(moviesToFilterChan, nextFilterChan)

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
		fmt.Printf("Movies passing through production filter: %s\n", movies)
		//TODO: filter movies
		fmt.Printf("Movies filtered by production: %s\n", movies)

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
