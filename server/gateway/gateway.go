package main

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "movies-to-preprocess"
const NEXT_STEP = "q1-results"

type Gateway struct {
	rabbitUser string
	rabbitPass string
	middleware *common.Middleware
}

func NewGateway(rabbitUser, rabbitPass string) *Gateway {
	return &Gateway{rabbitUser: rabbitUser, rabbitPass: rabbitPass}
}

func (g *Gateway) Start() {
	middleware, err := common.NewMiddleware(g.rabbitUser, g.rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return
	}

	defer func() {
		if err := middleware.Close(); err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}()

	g.middleware = middleware

	moviesToFilterChan, err := middleware.GetChanToSend(PREVIOUS_STEP)
	if err != nil {
		slog.Error(fmt.Sprintf("error with channel '%s'", PREVIOUS_STEP), slog.String("error", err.Error()))
		return
	}

	q1ResultsChan, err := middleware.GetChanToRecv(NEXT_STEP)
	if err != nil {
		slog.Error(fmt.Sprintf("error with channel '%s'", NEXT_STEP), slog.String("error", err.Error()))
		return
	}

	body, err := json.Marshal(common.MockedBatch)
	if err != nil {
		slog.Error("error marshalling batch", slog.String("error", err.Error()))
		return
	}

	eofBody, err := json.Marshal(common.EOF)
	if err != nil {
		slog.Error("error marshalling EOF", slog.String("error", err.Error()))
		return
	}

	moviesToFilterChan <- body
	moviesToFilterChan <- eofBody
	slog.Info("Messages sent.")

	go g.processMessages(q1ResultsChan)

	forever := make(chan bool)
	<-forever
}

func (g *Gateway) processMessages(q1ResultsChan <-chan common.Message) {
	for msg := range q1ResultsChan {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
		}

		if batch.IsEof() {
			slog.Info("EOF received")
		} else {
			slog.Info("Got movies", slog.Any("headers", batch.Header), slog.Any("movies", batch.Movies))
		}

		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
	}
}
