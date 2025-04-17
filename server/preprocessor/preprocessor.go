package main

import (
	"encoding/json"
	"log/slog"

	"tp-sistemas-distribuidos/server/common"
)

const nextQueue = "filter-year-q1"
const previousQueue = "movies-to-preprocess"

type Preprocessor struct {
	rabbitUser string
	rabbitPass string
	middleware *common.Middleware
}

func NewPreprocessor(rabbitUser, rabbitPass string) *Preprocessor {
	return &Preprocessor{rabbitUser: rabbitUser, rabbitPass: rabbitPass}
}

func (p *Preprocessor) Start() {
	middleware, err := common.NewMiddleware(p.rabbitUser, p.rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return
	}
	p.middleware = middleware

	defer func() {
		if err := middleware.Close(); err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}()

	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", previousQueue), slog.String("error", err.Error()))
		return
	}

	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", nextQueue), slog.String("error", err.Error()))
		return
	}

	go p.processMessages(previousChan, nextChan)

	forever := make(chan bool)
	<-forever
}

func (p *Preprocessor) processMessages(moviesToPreprocessChan <-chan common.Message, nextStepChan chan<- []byte) {
	for msg := range moviesToPreprocessChan {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		slog.Info("processing batch", slog.Any("headers", batch.Header), slog.Any("movies", batch.Movies))

		preprocessBatch := batch
		if !batch.IsEof() {
			preprocessBatch = p.preprocessBatch(batch)
			slog.Info("movies left after preprocessing", slog.Any("movies", preprocessBatch.Movies))
		} else {
			slog.Info("EOF received")
		}

		response, err := json.Marshal(preprocessBatch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			continue
		}

		nextStepChan <- response
		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
	}
}

func (p *Preprocessor) preprocessBatch(batch common.Batch) common.Batch {
	return batch //TODO: do preprocessing
}
