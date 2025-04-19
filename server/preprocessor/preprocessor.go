package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"pkg/models"
	"strconv"
	"tp-sistemas-distribuidos/server/common"
)

const nextQueue = "filter-year-q1"
const previousQueue = "movies-to-preprocess"

type PreprocessorConfig struct {
	RabbitUser string
	RabbitPass string
}

type Preprocessor struct {
	config                 PreprocessorConfig
	middleware             *common.Middleware
	moviesToPreprocessChan <-chan common.Message
	nextStepChan           chan<- []byte
}

func NewPreprocessor(rabbitUser, rabbitPass string) *Preprocessor {
	config := PreprocessorConfig{
		RabbitUser: rabbitUser,
		RabbitPass: rabbitPass,
	}

	Preprocessor := &Preprocessor{
		config: config,
	}

	err := Preprocessor.middlewareSetup()
	if err != nil {
		slog.Error("error setting up middleware", slog.String("error", err.Error()))
		return nil
	}

	return Preprocessor

}

func (p *Preprocessor) middlewareSetup() error {
	// Setup middleware connection
	middleware, err := common.NewMiddleware(p.config.RabbitUser, p.config.RabbitPass)
	if err != nil {
		return fmt.Errorf("error creating middleware: %s", err)
	}

	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return fmt.Errorf("error getting channel to receive: %s", err)
	}

	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		return fmt.Errorf("error getting channel to send: %s", err)
	}

	p.middleware = middleware
	p.moviesToPreprocessChan = previousChan
	p.nextStepChan = nextChan
	return nil
}

func (p *Preprocessor) close() {
	if err := p.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
}

func (p *Preprocessor) Start() {
	go p.processMessages()
	defer p.close()

	forever := make(chan bool)
	<-forever
}

func (p *Preprocessor) processMessages() {
	for msg := range p.moviesToPreprocessChan {

		var batch models.RawMovieBatch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		preprocessBatch := common.Batch{
			Header: common.Header{
				Weight:      batch.Header.Weight,
				TotalWeight: batch.Header.TotalWeight,
			},
		}

		if !batch.IsEof() {
			preprocessBatch = p.preprocessBatch(batch)
		} else {
			slog.Info("EOF received")
		}

		response, err := json.Marshal(preprocessBatch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			continue
		}

		p.nextStepChan <- response
		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
	}
}

func (p *Preprocessor) preprocessBatch(batch models.RawMovieBatch) common.Batch {
	movies := make([]common.Movie, 0)

	for _, movie := range batch.Movies {
		id := strconv.Itoa(int(movie.ID))
		title := movie.Title
		year := movie.ReleaseDate.Year()
		genres := make([]string, len(movie.Genres))
		for i, genre := range movie.Genres {
			genres[i] = genre.Name
		}
		countries := make([]string, len(movie.ProductionCountries))
		for i, country := range movie.ProductionCountries {
			countries[i] = country.Code
		}

		movies = append(movies, common.Movie{
			ID:                  id,
			Title:               title,
			Year:                year,
			Genres:              genres,
			ProductionCountries: countries,
		})
	}

	res := common.Batch{
		Header: common.Header{
			Weight:      uint32(len(movies)),
			TotalWeight: -1,
		},
		Movies: movies,
	}

	return res
}
