package main

import (
	"encoding/json"
	"log/slog"
	"slices"

	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "filter-production-q1"
const NEXT_STEP = "movies-to-join"
// const NEXT_STEP = "q2-to-reduce"

type ProductionFilter struct {
	rabbitUser string
	rabbitPass string
	middleware *common.Middleware
}

func NewProductionFilter(rabbitUser, rabbitPass string) *ProductionFilter {
	return &ProductionFilter{rabbitUser: rabbitUser, rabbitPass: rabbitPass}
}

func (f *ProductionFilter) Start() {
	middleware, err := common.NewMiddleware(f.rabbitUser, f.rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return
	}
	f.middleware = middleware

	defer func() {
		if err := middleware.Close(); err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}()

	moviesToFilterChan, err := middleware.GetChanToRecv(PREVIOUS_STEP)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", PREVIOUS_STEP), slog.String("error", err.Error()))
		return
	}

	nextFilterChan, err := middleware.GetChanToSend(NEXT_STEP)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", NEXT_STEP), slog.String("error", err.Error()))
		return
	}

	go f.processMessages(moviesToFilterChan, nextFilterChan)

	forever := make(chan bool)
	<-forever
}

func (f *ProductionFilter) processMessages(moviesToFilterChan <-chan common.Message, nextFilterChan chan<- []byte) {
	for msg := range moviesToFilterChan {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}
		//slog.Info("processing batch", slog.Any("headers", batch.Header), slog.Any("movies", batch.Movies))

		filteredMovies := batch.Movies
		if !batch.IsEof() {
			filteredMovies = common.Filter(batch.Movies, f.filterByProductionQ3)
			slog.Info("movies left after filtering by production", slog.Any("movies", filteredMovies))
		}

		batch.Movies = filteredMovies
		response, err := json.Marshal(batch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			continue
		}

		nextFilterChan <- response
		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
	}
}

func (f *ProductionFilter) filterByProductionQ1(movie common.Movie) bool {
	arg := pkg.Country{Code: "AR", Name: "Argentina"}
	esp := pkg.Country{Code: "ES", Name: "Spain"}
	return slices.Contains(movie.ProductionCountries, arg) &&
		slices.Contains(movie.ProductionCountries, esp)
}

func (f *ProductionFilter) filterByProductionQ2(movie common.Movie) bool {
	return len(movie.ProductionCountries) == 1
}

func (f *ProductionFilter) filterByProductionQ3(movie common.Movie) bool {
	arg := pkg.Country{Code: "AR", Name: "Argentina"}
	return slices.Contains(movie.ProductionCountries, arg)
}

