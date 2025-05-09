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

type queuesNames struct {
	previousQueue string
	nextQueue     string
}

var queriesQueues = map[int]queuesNames{
	1: {previousQueue: "filter-year-q1", nextQueue: "filter-production-q1"},
	3: {previousQueue: "filter-year-q3q4", nextQueue: "filter-production-q3q4"},
}

type YearFilter struct {
	middleware  *common.Middleware
	connections map[int]connection
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

	connections, err := initializeConnections(middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &YearFilter{middleware: middleware, connections: connections}, nil
}

func initializeConnection(middleware *common.Middleware, previousQueue, nextQueue string) (connection, error) {
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

func initializeConnections(middleware *common.Middleware) (map[int]connection, error) {
	connections := make(map[int]connection)
	for queryNum, queuesNames := range queriesQueues {
		connection, err := initializeConnection(middleware, queuesNames.previousQueue, queuesNames.nextQueue)
		if err != nil {
			return nil, fmt.Errorf("error initializing connection for %s: %w", queuesNames.previousQueue, err)
		}
		connections[queryNum] = connection
	}
	return connections, nil
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
		case msg := <-f.connections[1].ChanToRecv:
			if err := f.processQueryMessage(1, msg, f.year2000sFilter); err != nil {
				slog.Error("error processing q1 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging q1 message", slog.String("error", err.Error()))
			}
		case msg := <-f.connections[3].ChanToRecv:
			if err := f.processQueryMessage(3, msg, f.yearAfter2000sFilter); err != nil {
				slog.Error("error processing q3/q4 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging q3/q4 message", slog.String("error", err.Error()))
			}
		}
	}
}

func (f *YearFilter) processQueryMessage(queryNum int, msg common.Message, filterFunc func(common.Movie) bool) error {
	batch, err := f.filterMessage(msg, filterFunc)
	if err != nil {
		return fmt.Errorf("error filtering message: %w", err)
	}
	if err := f.sendBatch(queryNum, batch); err != nil {
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

func (f *YearFilter) sendBatch(queryNum int, batch common.Batch[common.Movie]) error {
	response, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}
	f.connections[queryNum].ChanToSend <- response
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
