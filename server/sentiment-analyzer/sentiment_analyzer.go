package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"tp-sistemas-distribuidos/server/common"

	cdipaoloSentiment "github.com/cdipaolo/sentiment"
)

const (
	previousQueue = "sentiment-analyzer"
	rabbitHost    = "rabbitmq"
	nextQueue     = "q5-to-reduce"
)

type Analyzer struct {
	middleware *common.Middleware
	model      cdipaoloSentiment.Models
}

func NewAnalyzer(rabbitUser, rabbitPass string) (*Analyzer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
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
	defer a.stop()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

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

	a.run(ctx, previousChan, nextChan)
}

func (a *Analyzer) run(ctx context.Context, previousChan <-chan common.Message, nextChan chan<- []byte) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping sentiment analyzer")
			return
		case msg := <-previousChan:
			if err := a.processMessage(msg, nextChan); err != nil {
				slog.Error("Error processing message", slog.String("error", err.Error()))
				continue // TODO: ack?
			}
			if err := msg.Ack(); err != nil {
				slog.Error("Error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (a *Analyzer) processMessage(msg common.Message, nextChan chan<- []byte) error {
	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling message: %v", err)

	}
	slog.Debug("Received message", slog.String("message", string(msg.Body)))

	batchWithSentiment := a.analyzeSentiment(batch)
	serializedBatch, err := json.Marshal(batchWithSentiment)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}
	nextChan <- serializedBatch
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

func (a *Analyzer) stop() {
	err := a.middleware.Close()
	if err != nil {
		slog.Error("Error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("sentiment analyzer stopped")
}
