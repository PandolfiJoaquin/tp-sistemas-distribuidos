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
	nextQueue     string
}

var queriesQueues = map[int]queuesNames{
	1: {previousQueue: "filter-production-q1", nextQueue: "q1-results"},
	2: {previousQueue: "filter-production-q2", nextQueue: "q2-to-reduce"},
	3: {previousQueue: "filter-production-q3q4", nextQueue: "movies-to-join"}, // TODO: change to shards
}

type ProductionFilter struct {
	middleware  *common.Middleware
	connections map[int]connection
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewProductionFilter(rabbitUser, rabbitPass string) (*ProductionFilter, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connections, err := initializeConnections(middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &ProductionFilter{middleware: middleware, connections: connections}, nil
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

func (f *ProductionFilter) Start() {
	go f.start()

	forever := make(chan bool)
	<-forever
}

func (f *ProductionFilter) start() {
	for {
		select {
		case msg := <-f.connections[1].ChanToRecv:
			if err := f.processQueryMessage(1, msg, f.filterByProductionQ1); err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.connections[2].ChanToRecv:
			if err := f.processQueryMessage(2, msg, f.filterByProductionQ2); err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		case msg := <-f.connections[3].ChanToRecv:
			if err := f.processQueryMessage(3, msg, f.filterByProductionQ3); err != nil {
				slog.Error("error processing query message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (f *ProductionFilter) processQueryMessage(queryNum int, msg common.Message, filterFunc func(common.Movie) bool) error {
	batch, err := f.filterMessage(msg, filterFunc)
	if err != nil {
		return fmt.Errorf("error filtering message: %w", err)
	}
	if err := f.sendBatch(queryNum, batch); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
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

func (f *ProductionFilter) sendBatch(queryNum int, batch common.Batch[common.Movie]) error {
	response, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}
	f.connections[queryNum].ChanToSend <- response
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
