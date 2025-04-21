package main

import (
	"encoding/json"
	"fmt"
	cdipaoloSentiment "github.com/cdipaolo/sentiment"
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

const (
	previousQueue = "sentiment-analyzer"
	nextQueue     = "q5-to-reduce"
)

type Analyzer struct {
	middleware *common.Middleware
	model      cdipaoloSentiment.Models
}

func NewAnalyzer(rabbitUser, rabbitPass string) (*Analyzer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	model, err := cdipaoloSentiment.Restore()
	if err != nil {
		slog.Error("Error restoring sentiment analyzer cdipaoloSentiment", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error restoring sentiment analyzer cdipaoloSentiment: %w", err)
	}

	return &Analyzer{middleware: middleware, model: model}, nil
}

func (a *Analyzer) Start() {
	previousChan, err := a.middleware.GetChanToRecv(previousQueue)
	if err != nil {
		slog.Error("Error creating channel", slog.String("queue", previousQueue), slog.String("error", err.Error()))
		return
	}

	nextChan, err := a.middleware.GetChanToSend(nextQueue)
	if err != nil {
		slog.Error("error creating channel", slog.String("queue", nextQueue), slog.String("error", err.Error()))
		return
	}

	go a.run(previousChan, nextChan)

	forever := make(chan bool)
	<-forever

}

func (a *Analyzer) run(previousChan <-chan common.Message, nextChan chan<- []byte) {
	for msg := range previousChan {
		a.processMessage(msg, nextChan)
	}
}

func (a *Analyzer) processMessage(msg common.Message, nextChan chan<- []byte) {
	defer func() {
		err := msg.Ack()
		if err != nil {
			slog.Error("Error acknowledging message", slog.String("error", err.Error()))
			return
		}
	}()

	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		slog.Error("Error unmarshalling message", slog.String("error", err.Error()))
		return

	}
	slog.Debug("Received message", slog.String("message", string(msg.Body)))

	batchWithSentiment := a.analyzeSentiment(batch)
	serializedBatch, err := json.Marshal(batchWithSentiment)
	if err != nil {
		slog.Error("Error marshalling response", slog.String("error", err.Error()))
		return
	}
	nextChan <- serializedBatch
}

func (a *Analyzer) Stop() error {
	err := a.middleware.Close()
	if err != nil {
		slog.Error("Error closing middleware", slog.String("error", err.Error()))
		return fmt.Errorf("error closing analyzer: %w", err)
	}
	return nil
}

func (a *Analyzer) analyzeSentiment(batch common.Batch[common.Movie]) common.Batch[common.MovieWithSentiment] {
	moviesWithSentiment := common.Map(batch.Data, a.analyzeSentimentOfMovie)
	return common.Batch[common.MovieWithSentiment]{
		Header: batch.Header,
		Data:   moviesWithSentiment,
	}
}

func (a *Analyzer) analyzeSentimentOfMovie(movie common.Movie) common.MovieWithSentiment {
	return common.MovieWithSentiment{
		Movie:     movie,
		Sentiment: a.calculateSentiment(movie),
	}
}

func (a *Analyzer) calculateSentiment(movie common.Movie) common.Sentiment {
	sentiment := a.model.SentimentAnalysis(movie.Overview, cdipaoloSentiment.English).Score
	if sentiment == 0 {
		return common.Negative
	}
	return common.Positive
}
