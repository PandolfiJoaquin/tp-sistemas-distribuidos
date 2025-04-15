package main

import (
	"log/slog"

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
			continue
		case _ = <-reviews[reviewsStatus]:
			continue
		}
	}

}

func (j *Joiner) preprocessBatch(batch common.Batch) common.Batch {
	return batch //TODO: do preprocessing
}

func mockMoviesChan() (<-chan common.Message, error) {
	moviesChan := make(chan common.Message, 1)
	return moviesChan, nil
}

func mockReviewsChan() (<-chan common.Message, error) {
	reviewsChan := make(chan common.Message, 1)
	return reviewsChan, nil
}

func mockQ3Chan() (chan<- []byte, error) {
	moviesChan := make(chan []byte, 1)
	return moviesChan, nil

}
