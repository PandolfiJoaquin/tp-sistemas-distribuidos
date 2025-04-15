package main

import (
	"log/slog"
	"time"

	"tp-sistemas-distribuidos/server/common"
)

const nextQueue = ""
const previousQueue = ""
const Q3ToReduceQueue = "q3-to-reduce"
const ON = 1
const OFF = 0

type Joiner struct {
	rabbitUser string
	rabbitPass string
	middleware *common.Middleware
}

func NewJoiner(rabbitUser, rabbitPass string) *Joiner {
	return &Joiner{rabbitUser: rabbitUser, rabbitPass: rabbitPass}
}

func (j *Joiner) Start() {
	middleware, err := common.NewMiddleware(j.rabbitUser, j.rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return
	}
	j.middleware = middleware

	defer func() {
		if err := middleware.Close(); err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}()

	moviesChan, err := mockMoviesChan()
	if err != nil {
		slog.Error("Error creating mocked movies chann", slog.String("queue", previousQueue), slog.String("error", err.Error()))
		return
	}

	reviewsChan, err := mockReviewsChan()
	if err != nil {
		slog.Error("error creating mocked reviews", slog.String("queue", nextQueue), slog.String("error", err.Error()))
		return
	}

	q3Chan, err := mockQ3Chan()

	go j.start(moviesChan, reviewsChan, q3Chan)

	forever := make(chan bool)
	<-forever
}

func (j *Joiner) start(moviesChan, reviewsChan <-chan common.Message, nextStepChan chan<- []byte) {
	dummyChan := make(<-chan common.Message, 1)
	movies := []<-chan common.Message{dummyChan, moviesChan}
	moviesStatus := ON
	reviews := []<-chan common.Message{dummyChan, reviewsChan}
	reviewsStatus := OFF

	for {
		slog.Info("Receiving messages", slog.Any("moviesStatus:", moviesStatus), slog.Any("reviewsStatus", reviewsStatus))
		select {
		case _ = <-movies[moviesStatus]:
			slog.Info("Received all movies. starting to pop reviews")
			time.Sleep(2 * time.Second)
			reviewsStatus = ON
			continue
		case _ = <-reviews[reviewsStatus]:
			slog.Info("Processing reviews")
			time.Sleep(2 * time.Second)
			nextStepChan <- []byte("Reduce this")
			continue
		}
	}

}

//func (j *Joiner) processMessages(moviesToPreprocessChan <-chan common.Message, nextStepChan chan<- []byte) {
//	for msg := range moviesToPreprocessChan {
//		var batch common.Batch
//		if err := json.Unmarshal(msg.Body, &batch); err != nil {
//			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
//			continue
//		}
//
//		slog.Info("processing batch", slog.Any("headers", batch.Header), slog.Any("movies", batch.Movies))
//
//		preprocessBatch := batch
//		if !batch.IsEof() {
//			preprocessBatch = j.preprocessBatch(batch)
//			slog.Info("movies left after preprocessing", slog.Any("movies", preprocessBatch.Movies))
//		}
//
//		response, err := json.Marshal(preprocessBatch)
//		if err != nil {
//			slog.Error("error marshalling batch", slog.String("error", err.Error()))
//			continue
//		}
//
//		nextStepChan <- response
//		if err := msg.Ack(); err != nil {
//			slog.Error("error acknowledging message", slog.String("error", err.Error()))
//		}
//	}
//}

func mockMoviesChan() (<-chan common.Message, error) {
	moviesChan := make(chan common.Message, 1)
	go func() {
		time.Sleep(5 * time.Second)
		moviesChan <- common.Message{}
	}()
	return moviesChan, nil
}

func mockReviewsChan() (<-chan common.Message, error) {
	reviewsChan := make(chan common.Message, 1)
	go func() {
		time.Sleep(10 * time.Second)
		reviewsChan <- common.Message{}
	}()
	return reviewsChan, nil
}

func mockQ3Chan() (chan<- []byte, error) {
	moviesChan := make(chan []byte, 1)
	go func() {
		time.Sleep(15 * time.Second)
		response := <-moviesChan
		slog.Info("Mock received message from Joiner", slog.Any("message", string(response)))
	}()
	return moviesChan, nil

}
