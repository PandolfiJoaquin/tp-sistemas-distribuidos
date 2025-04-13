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

	moviesToFilterChan, err := middleware.GetChanToRecv("q1-year-filter")
	if err != nil {
		fmt.Printf("error with channel 'q1-year-filter': %v", err)
	}

	nextFilterChan, err := middleware.GetChanToSend("q1-results")
	if err != nil {
		fmt.Printf("error with channel 'q1-results': %v", err)
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
		fmt.Printf("Movies passing through year filter: %v\n", movies)
		filteredMovies := common.Filter[common.Movie](movies, func(movie common.Movie) bool { return movie.Year >= 2000 })
		fmt.Printf("Movies filtered by year: %v\n", filteredMovies)

		response, err := json.Marshal(filteredMovies)
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
