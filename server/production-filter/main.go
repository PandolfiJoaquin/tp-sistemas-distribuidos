package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "filter-production-q1"
const NEXT_STEP = "q1-results"

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

	moviesToFilterChan, err := middleware.GetChanToRecv(PREVIOUS_STEP)
	if err != nil {
		fmt.Printf("error with channel '%s': %v", PREVIOUS_STEP, err)
	}

	nextFilterChan, err := middleware.GetChanToSend(NEXT_STEP)
	if err != nil {
		fmt.Printf("error with channel '%s': %v", NEXT_STEP, err)
	}

	go start(moviesToFilterChan, nextFilterChan)

	forever := make(chan bool)
	<-forever
}

func start(moviesToFilterChan <-chan common.Message, nextFilterChan chan<- []byte) {
	for msg := range moviesToFilterChan {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			fmt.Printf("Error unmarshalling message: %v", err)
			fmt.Printf("message: %v", msg.Body)
		}
		fmt.Printf("Headers: %v", batch.Header)
		fmt.Printf("Movies passing through production filter: %v\n", batch.Movies)
		filteredMovies := common.Filter(batch.Movies, filterByProductionQ1)
		fmt.Printf("Movies filtered by production: %v\n", filteredMovies)
		batch.Movies = filteredMovies
		response, err := json.Marshal(batch)
		if err != nil {
			fmt.Printf("Error marshalling batch: %v", err)
			continue
		}
		nextFilterChan <- response
		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
	}
}

func filterByProductionQ1(movie common.Movie) bool {
	return slices.Contains(movie.ProductionCountries, "Argentina") &&
		slices.Contains(movie.ProductionCountries, "España")
}
