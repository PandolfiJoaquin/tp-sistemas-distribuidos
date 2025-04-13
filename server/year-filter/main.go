package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "filter-year-q1"
const NEXT_STEP = "filter-production-q1"

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

	previousChan, err := middleware.GetChanToRecv(PREVIOUS_STEP)
	if err != nil {
		fmt.Printf("error with channel '%s': %v", PREVIOUS_STEP, err)
	}

	nextChan, err := middleware.GetChanToSend(NEXT_STEP)
	if err != nil {
		fmt.Printf("error with channel '%s': %v", NEXT_STEP, err)
	}

	go start(previousChan, nextChan)

	forever := make(chan bool)
	<-forever
}

func start(previousChan <-chan common.Message, nextChan chan<- []byte) {
	for msg := range previousChan {
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
		nextChan <- response
		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
	}
}
