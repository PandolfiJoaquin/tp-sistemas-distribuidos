package main

import (
	"encoding/json"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "movies-to-preprocess"
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

	moviesToFilterChan, err := middleware.GetChanToSend(PREVIOUS_STEP)
	if err != nil {
		fmt.Printf("error with channel 'movies-to-preprocess': %v", err)
	}

	q1ResultsChan, err := middleware.GetChanToRecv(NEXT_STEP)
	if err != nil {
		fmt.Println(err)
	}

	body, err := json.Marshal(common.MockedBatch)
	if err != nil {
		fmt.Printf("Error marshalling batch: %v", err)
		return
	}

	eofBody, err := json.Marshal(common.EOF)
	if err != nil {
		fmt.Printf("Error marshalling EOF: %v", err)
		return
	}

	moviesToFilterChan <- body
	moviesToFilterChan <- eofBody
	fmt.Println("Messages sent.")

	go processMessages(q1ResultsChan)

	forever := make(chan bool)
	<-forever
}

func processMessages(q1ResultsChan <-chan common.Message) {
	for msg := range q1ResultsChan {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			fmt.Printf("Error unmarshalling message: %v", err)
		}
		fmt.Println("Headers: ", batch.Header)
		fmt.Println("Movies: ", batch.Movies)

		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
	}
	return
}
