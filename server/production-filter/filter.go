package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"slices"
	"syscall"

	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

const (
	rabbitHost          = "rabbitmq"
	previousQueueQuery1 = "filter-production-q1"
	previousQueueQuery2 = "filter-production-q2"
	previousQueueQuery3 = "filter-production-q3q4"
	nextQueueQuery1     = "q1-results"
	nextQueueQuery2     = "q2-to-reduce"
	topic               = "movies-to-join-%d"
	moviesExchange      = "movies-exchange"
)

type ProductionFilter struct {
	middleware              *common.Middleware
	query1Connection        connection
	query2Connection        connection
	query3ShardsConnections shardConnection
}

type shardConnection struct {
	previousChan <-chan common.Message
	nextChan     map[int]chan<- []byte
	shards       int
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewProductionFilter(rabbitUser, rabbitPass string, shards int) (*ProductionFilter, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	query1Connection, err := initializeConnection(middleware, previousQueueQuery1, nextQueueQuery1)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 1 connection: %w", err)
	}

	query2Connection, err := initializeConnection(middleware, previousQueueQuery2, nextQueueQuery2)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 2 connection: %w", err)
	}

	query3ShardsConnections, err := initializeShardsConnections(middleware, previousQueueQuery3, shards)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 3 shards connections: %w", err)
	}

	return &ProductionFilter{middleware: middleware,
		query1Connection:        query1Connection,
		query2Connection:        query2Connection,
		query3ShardsConnections: query3ShardsConnections,
	}, nil
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

func initializeShardsConnections(middleware *common.Middleware, previousQueue string, shards int) (shardConnection, error) {
	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return shardConnection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}

	nextChan := make(map[int]chan<- []byte)
	for i := 1; i <= shards; i++ {
		nextChan[i], err = middleware.GetChanWithTopicToSend(moviesExchange, fmt.Sprintf(topic, i))
		if err != nil {
			return shardConnection{}, fmt.Errorf("error getting channel %s to receive: %w", fmt.Sprintf(topic, i), err)
		}
	}

	return shardConnection{previousChan, nextChan, shards}, nil
}

func (f *ProductionFilter) Start() {
	defer f.stop()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	f.start(ctx)
}

func (f *ProductionFilter) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping production filter")
			return
		case msg := <-f.query1Connection.ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ1)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			} else {
				if err := f.sendBatch(f.query1Connection.ChanToSend, batch); err != nil {
					slog.Error("error sending batch", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.query2Connection.ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ2)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			} else {
				if err := f.sendBatch(f.query2Connection.ChanToSend, batch); err != nil {
					slog.Error("error sending batch", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.query3ShardsConnections.previousChan:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ3)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			} else {
				if err := f.sendBatchToShards(f.query3ShardsConnections, batch); err != nil {
					slog.Error("error sending batch to shards", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (f *ProductionFilter) processQueryMessage(msg common.Message, filterFunc func(common.Movie) bool) (common.Batch[common.Movie], error) {
	batch, err := f.filterMessage(msg, filterFunc)
	if err != nil {
		return common.Batch[common.Movie]{}, fmt.Errorf("error filtering message: %w", err)
	}
	return batch, nil
}

func (f *ProductionFilter) filterMessage(msg common.Message, filterFunc func(common.Movie) bool) (common.Batch[common.Movie], error) {
	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return common.Batch[common.Movie]{}, fmt.Errorf("error unmarshalling message: %w", err)
	}

	filteredMovies := batch.Data
	if !batch.IsEof() {
		filteredMovies = common.Filter(batch.Data, filterFunc)
		slog.Debug("movies left after filtering by year", slog.Any("movies", filteredMovies))
	}

	batch.Data = filteredMovies
	return batch, nil
}

func (f *ProductionFilter) sendBatch(chanToSend chan<- []byte, batch common.Batch[common.Movie]) error {
	response, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}
	chanToSend <- response
	return nil
}

func (f *ProductionFilter) sendBatchToShards(conn shardConnection, batch common.Batch[common.Movie]) error {
	movies := make([][]common.Movie, conn.shards)
	for _, movie := range batch.Data {
		shard := common.GetShard(movie.ID, conn.shards)
		movies[shard-1] = append(movies[shard-1], movie)
	}

	for i, moviesData := range movies {
		shard := i + 1
		currentBatch := batch
		currentBatch.Data = moviesData
		if err := f.sendBatch(conn.nextChan[shard], currentBatch); err != nil {
			slog.Error("error sending batch", slog.String("error", err.Error()))
		}
	}
	return nil
}
func (f *ProductionFilter) filterByProductionQ1(movie common.Movie) bool {
	arg := pkg.Country{Code: "AR", Name: "Argentina"}
	esp := pkg.Country{Code: "ES", Name: "Spain"}
	return slices.Contains(movie.ProductionCountries, arg) &&
		slices.Contains(movie.ProductionCountries, esp)
}

func (f *ProductionFilter) filterByProductionQ2(movie common.Movie) bool {
	return len(movie.ProductionCountries) == 1 && movie.Budget > 0
}

func (f *ProductionFilter) filterByProductionQ3(movie common.Movie) bool {
	arg := pkg.Country{Code: "AR", Name: "Argentina"}
	return slices.Contains(movie.ProductionCountries, arg)
}

func (f *ProductionFilter) stop() {
	if err := f.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("production filter stopped")
}
