package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"tp-sistemas-distribuidos/server/common"
)

const (
	previousQueueQuery1     = "filter-year-q1"
	previousQueueQuery3And4 = "filter-year-q3q4"
	nextQueueQuery1         = "filter-production-q1"
	nextQueueQuery3And4     = "filter-production-q3q4"
)

type YearFilter struct {
	middleware       *common.Middleware
	query1Connection connection
	query3Connection connection
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewYearFilter(rabbitUser, rabbitPass string) (*YearFilter, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	query1Connection, err := initializeConnection(middleware, previousQueueQuery1, nextQueueQuery1)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	query3And4Connection, err := initializeConnection(middleware, previousQueueQuery3And4, nextQueueQuery3And4)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &YearFilter{middleware: middleware, query1Connection: query1Connection, query3Connection: query3And4Connection}, nil
}

func initializeConnection(middleware *common.Middleware, previousQueue string, nextQueue string) (connection, error) {
	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}

	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to send: %w", nextQueue, err)
	}
	return connection{previousChan, nextChan}, nil
}

func (f *YearFilter) Start() {
	defer f.stop()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	f.start(ctx)
}

func (f *YearFilter) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping year filter")
			return
		case msg := <-f.query1Connection.ChanToRecv:
			if err := f.processQueryMessage(f.query1Connection.ChanToSend, msg, f.year2000sFilter); err != nil {
				slog.Error("error processing q1 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging q1 message", slog.String("error", err.Error()))
			}
		case msg := <-f.query3Connection.ChanToRecv:
			if err := f.processQueryMessage(f.query3Connection.ChanToSend, msg, f.yearAfter2000sFilter); err != nil {
				slog.Error("error processing q3/q4 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging q3/q4 message", slog.String("error", err.Error()))
			}
		}
	}
}

func (f *YearFilter) processQueryMessage(chanToSend chan<- []byte, msg common.Message, filterFunc func(common.Movie) bool) error {
	batch, err := f.filterMessage(msg, filterFunc)
	if err != nil {
		return fmt.Errorf("error filtering message: %w", err)
	}
	if err := f.sendBatch(chanToSend, batch); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
}

func (f *YearFilter) filterMessage(msg common.Message, filterFunc func(common.Movie) bool) (common.Batch[common.Movie], error) {
	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return common.Batch[common.Movie]{}, fmt.Errorf("error unmarshalling message: %w", err)
	}

	filteredMovies := batch.Data
	if !batch.IsEof() {
		filteredMovies = common.Filter(batch.Data, filterFunc)
	}

	batch.Data = filteredMovies
	return batch, nil
}

func (f *YearFilter) sendBatch(chanToSend chan<- []byte, batch common.Batch[common.Movie]) error {
	response, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}
	chanToSend <- response
	return nil
}

func (f *YearFilter) year2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000 && movie.Year < 2010
}

func (f *YearFilter) yearAfter2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000
}

func (f *YearFilter) stop() {
	if err := f.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("year filter stopped")
}
