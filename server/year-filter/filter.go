package main

import (
	"encoding/json"
	"log/slog"

	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "filter-year-q1"
const NEXT_STEP = "filter-production-q1"

type YearFilter struct {
	rabbitUser string
	rabbitPass string
	middleware *common.Middleware
}

func NewYearFilter(rabbitUser, rabbitPass string) *YearFilter {
	return &YearFilter{rabbitUser: rabbitUser, rabbitPass: rabbitPass}
}

func (f *YearFilter) Start() {
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

	previousChan, err := middleware.GetChanToRecv(PREVIOUS_STEP)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", PREVIOUS_STEP), slog.String("error", err.Error()))
		return
	}

	nextChan, err := middleware.GetChanToSend(NEXT_STEP)
	if err != nil {
		slog.Error("error with channel", slog.String("queue", NEXT_STEP), slog.String("error", err.Error()))
		return
	}

	go f.processMessages(previousChan, nextChan)

	forever := make(chan bool)
	<-forever
}

func (f *YearFilter) processMessages(previousChan <-chan common.Message, nextChan chan<- []byte) {
	for msg := range previousChan {
		var batch common.MoviesBatch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		filteredMovies := batch.Movies
		if !batch.IsEof() {
			filteredMovies = common.Filter(batch.Movies, f.year2000sFilter)
			slog.Info("movies left after filtering by year", slog.Any("movies", filteredMovies))
		}

		batch.Movies = filteredMovies
		response, err := json.Marshal(batch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			continue
		}

		nextChan <- response
		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
	}
}

func (f *YearFilter) year2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000 && movie.Year < 2010
}

func (f *YearFilter) yearAfter2000sFilter(movie common.Movie) bool {
	return movie.Year >= 2000
}
