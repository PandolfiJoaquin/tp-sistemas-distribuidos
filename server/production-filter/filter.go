package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

type queuesNames struct {
	previousQueue string
	nextQueues    map[int]string
}

var queriesQueues = map[int]queuesNames{
	1: {previousQueue: "filter-production-q1", nextQueues: map[int]string{1: "q1-results"}},
	2: {previousQueue: "filter-production-q2", nextQueues: map[int]string{1: "q2-to-reduce"}},
	3: {previousQueue: "filter-production-q3q4", nextQueues: map[int]string{1: "movies-to-join-1", 2: "movies-to-join-2", 3: "movies-to-join-3"}}, // TODO: change to shards
}

type ProductionFilter struct {
	middleware  *common.Middleware
	connections map[int]connection
	shards      int
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend map[int]chan<- []byte
}

func NewProductionFilter(rabbitUser, rabbitPass string, shards int) (*ProductionFilter, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connections, err := initializeConnections(middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &ProductionFilter{middleware: middleware, connections: connections, shards: shards}, nil
}

func initializeConnection(middleware *common.Middleware, previousQueue string, nextQueues map[int]string) (connection, error) {
	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}

	nextChans := make(map[int]chan<- []byte)
	for queryNum, nextQueue := range nextQueues {
		nextChan, err := middleware.GetChanToSend(nextQueue)
		if err != nil {
			return connection{}, fmt.Errorf("error getting channel %s to send: %w", nextQueue, err)
		}
		nextChans[queryNum] = nextChan
	}
	return connection{previousChan, nextChans}, nil
}

func initializeConnections(middleware *common.Middleware) (map[int]connection, error) {
	connections := make(map[int]connection)
	for queryNum, queuesNames := range queriesQueues {
		connection, err := initializeConnection(middleware, queuesNames.previousQueue, queuesNames.nextQueues)
		if err != nil {
			return nil, fmt.Errorf("error initializing connection for %s: %w", queuesNames.previousQueue, err)
		}
		connections[queryNum] = connection
	}
	return connections, nil
}

func (f *ProductionFilter) Start() {
	go f.start()

	forever := make(chan bool)
	<-forever
}

func (f *ProductionFilter) start() {
	for {
		select {
		case msg := <-f.connections[1].ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ1)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := f.sendBatch(f.connections[1].ChanToSend[1], batch); err != nil {
				slog.Error("error sending batch", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.connections[2].ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ2)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := f.sendBatch(f.connections[2].ChanToSend[1], batch); err != nil {
				slog.Error("error sending batch", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.connections[3].ChanToRecv:
			batch, err := f.processQueryMessage(msg, f.filterByProductionQ3)
			if err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}

			movies := make([][]common.Movie, f.shards)
			for _, movie := range batch.Data {
				shard := common.GetShard(movie.ID, f.shards)
				movies[shard] = append(movies[shard], movie)
			}

			for shard, movies := range movies {
				currentBatch := batch
				currentBatch.Data = movies
				if err := f.sendBatch(f.connections[3].ChanToSend[shard], currentBatch); err != nil {
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
		slog.Info("movies left after filtering by year", slog.Any("movies", filteredMovies))
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
