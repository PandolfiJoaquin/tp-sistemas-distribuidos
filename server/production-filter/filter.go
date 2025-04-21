package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

const (
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
	shards                  int
}

type shardConnection struct {
	previousChan <-chan common.Message
	nextChan     map[int]chan<- []byte
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewProductionFilter(rabbitUser, rabbitPass string, shards int) (*ProductionFilter, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	// connections, err := initializeConnections(middleware, queriesQueues)
	// if err != nil {
	// return nil, fmt.Errorf("error initializing connections: %w", err)
	// }

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
		shards:                  shards,
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
		//nextChan[i], err = middleware.GetChanToSend(fmt.Sprintf(topic, i))
		if err != nil {
			return shardConnection{}, fmt.Errorf("error getting channel %s to receive: %w", fmt.Sprintf(topic, i), err)
		}
	}

	return shardConnection{previousChan, nextChan}, nil
}

func (f *ProductionFilter) Start() {
	go f.start()

	forever := make(chan bool)
	<-forever
}

func (f *ProductionFilter) start() {
	for {
		select {
		case msg := <-f.query1Connection.ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ1)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := f.sendBatch(f.query1Connection.ChanToSend, batch); err != nil {
				slog.Error("error sending batch", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.query2Connection.ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ2)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := f.sendBatch(f.query2Connection.ChanToSend, batch); err != nil {
				slog.Error("error sending batch", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.query3ShardsConnections.previousChan:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ3)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}

			movies := make([][]common.Movie, f.shards)
			for _, movie := range batch.Data {
				shard := common.GetShard(movie.ID, f.shards)
				movies[shard-1] = append(movies[shard-1], movie)
			}

			for i, moviesData := range movies {
				shard := i + 1
				currentBatch := batch
				currentBatch.Data = moviesData
				slog.Info("sending batch", slog.Int("shard", shard))
				if err := f.sendBatch(f.query3ShardsConnections.nextChan[shard], currentBatch); err != nil {
					slog.Error("error sending batch", slog.String("error", err.Error()))
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

func (f *ProductionFilter) Stop() error {
	if err := f.middleware.Close(); err != nil {
		return fmt.Errorf("error closing middleware: %w", err)
	}
	return nil
}
