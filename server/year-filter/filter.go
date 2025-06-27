package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/middleware"
)

const (
	rabbitHost              = "rabbitmq"
	previousQueueQuery1     = "filter-year-q1"
	previousQueueQuery3And4 = "filter-year-q3q4"
	nextQueueQuery1         = "filter-production-q1"
	nextQueueQuery3And4     = "filter-production-q3q4"
	heartbeatInterval       = 1 * time.Second
)

type YearFilter struct {
	m                *middleware.Middleware
	query1Connection connection
	query3Connection connection
}

type connection struct {
	ChanToRecv <-chan middleware.Message
	ChanToSend middleware.SenderQueue
}

func NewYearFilter(rabbitUser, rabbitPass string) (*YearFilter, error) {
	m, err := middleware.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	query1Connection, err := initializeConnection(m, previousQueueQuery1, nextQueueQuery1)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	query3And4Connection, err := initializeConnection(m, previousQueueQuery3And4, nextQueueQuery3And4)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &YearFilter{m: m, query1Connection: query1Connection, query3Connection: query3And4Connection}, nil
}

func initializeConnection(m *middleware.Middleware, previousQueue string, nextQueue string) (connection, error) {
	previousChan, err := m.GetChanToRecv(previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}

	nextChan, err := m.GetQueueToSend(nextQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to send: %w", nextQueue, err)
	}
	return connection{previousChan, nextChan}, nil
}

func (f *YearFilter) Start() {
	slog.Info("starting year filter")
	defer f.stop()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	f.start(ctx)
}

func (f *YearFilter) start(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping year filter")
			return
		case <-ticker.C:
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
		f.m.SendHeartbeat()
	}
}

func (f *YearFilter) processQueryMessage(chanToSend middleware.SenderQueue, msg middleware.Message, filterFunc func(common.Movie) bool) error {
	batch, err := f.filterMessage(msg, filterFunc)
	if err != nil {
		return fmt.Errorf("error filtering message: %w", err)
	}
	if err := f.sendBatch(chanToSend, batch); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
}

func (f *YearFilter) filterMessage(msg middleware.Message, filterFunc func(common.Movie) bool) (common.Batch[common.Movie], error) {
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

func (f *YearFilter) sendBatch(chanToSend middleware.SenderQueue, batch common.Batch[common.Movie]) error {
	response, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}
	if err := chanToSend.Send(response); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
}

func (f *YearFilter) year2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000 && movie.Year < 2010
}

func (f *YearFilter) yearAfter2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000
}

func (f *YearFilter) stop() {
	if err := f.m.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("year filter stopped")
}
