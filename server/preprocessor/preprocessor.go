package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"pkg/models"
	"strconv"
	"tp-sistemas-distribuidos/server/common"
)

const toPreProcess = "to-preprocess"
const filterMoviesQ1 = "filter-year-q1"
const filterMoviesQ2 = "filter-production-q2"
const filterMovieQ3Q4 = "filter-year-q3q4"
const reviewsQueue = "reviews-to-join"

type PreprocessorConfig struct {
	RabbitUser string
	RabbitPass string
}

type Preprocessor struct {
	config        PreprocessorConfig
	middleware    *common.Middleware
	toProcessChan <-chan common.Message
	reviewsChan   chan<- []byte
	moviesChans   []chan<- []byte
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

	reviewsToJoin, err := middleware.GetChanToSend(reviewsQueue)
	if err != nil {
		return fmt.Errorf("error getting channel to send reviews: %s", err)
	}

	moviesChans := make([]chan<- []byte, 0)
	moviesToFilterQ1, err := middleware.GetChanToSend(filterMoviesQ1)
	if err != nil {
		return fmt.Errorf("error getting channel to send movies: %s", err)
	}

	moviesToFilterQ2, err := middleware.GetChanToSend(filterMoviesQ2)
	if err != nil {
		return fmt.Errorf("error getting channel to send movies: %s", err)
	}

	moviesToFilterQ3Q4, err := middleware.GetChanToSend(filterMovieQ3Q4)
	if err != nil {
		return fmt.Errorf("error getting channel to send movies: %s", err)
	}

	moviesChans = append(moviesChans, moviesToFilterQ1)
	moviesChans = append(moviesChans, moviesToFilterQ2)
	moviesChans = append(moviesChans, moviesToFilterQ3Q4)

	toProcess, err := middleware.GetChanToRecv(toPreProcess)
	if err != nil {
		return fmt.Errorf("error getting channel to receive: %s", err)
	}

	p.middleware = middleware
	p.toProcessChan = toProcess
	p.reviewsChan = reviewsToJoin
	p.moviesChans = moviesChans

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

	// TODO: Handle shutdown gracefully
	forever := make(chan bool)
	<-forever
}

func (p *Preprocessor) processMessages() {
	for msg := range p.toProcessChan {
		var batch common.ToProcessMsg

		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			return
		}

		if err := p.preprocessBatch(batch); err != nil {
			slog.Error("error preprocessing batch", slog.String("error", err.Error()))
			return
		}

		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
			return
		}

	}
}

func (p *Preprocessor) preprocessBatch(batch common.ToProcessMsg) error {
	var (
		header  common.Header
		payload interface{}
		outCh   []chan<- []byte
	)

	switch batch.Type {
	case "movies":
		var mb models.RawMovieBatch
		if err := json.Unmarshal(batch.Body, &mb); err != nil {
			return fmt.Errorf("error unmarshalling movies batch: %w", err)
		}

		header = common.Header{
			Weight:      mb.Header.Weight,
			TotalWeight: mb.Header.TotalWeight,
		}

		if mb.IsEof() {
			payload = common.Batch[common.Movie]{Header: header}
		} else {
			payload = p.preprocessMovies(mb)
		}

		slog.Info("preprocessing Movies", slog.String("batch", string(batch.Body)))

		for _, moviesChan := range p.moviesChans {
			outCh = append(outCh, moviesChan)
		}

	case "reviews":
		var rb models.RawReviewBatch
		if err := json.Unmarshal(batch.Body, &rb); err != nil {
			return fmt.Errorf("error unmarshalling reviews batch: %w", err)
		}

		header = common.Header{
			Weight:      rb.Header.Weight,
			TotalWeight: rb.Header.TotalWeight,
		}

		if rb.IsEof() {
			payload = common.ReviewsBatch{Header: header}
		} else {
			payload = p.preprocessReviews(rb)
		}

		slog.Info("preprocessing reviews", slog.String("batch", string(batch.Body)))

		outCh = append(outCh, p.reviewsChan)

	default:
		return fmt.Errorf("unknown batch type %q", batch.Type)
	}

	resp, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling %s payload: %w", batch.Type, err)
	}

	for _, ch := range outCh {
		ch <- resp
	}

	return nil
}

func (p *Preprocessor) preprocessMovies(batch models.RawMovieBatch) common.Batch[common.Movie] {
	movies := make([]common.Movie, 0)

	for _, movie := range batch.Movies {
		id := strconv.Itoa(int(movie.ID))
		title := movie.Title
		year := movie.ReleaseDate.Year()

		movies = append(movies, common.Movie{
			ID:                  id,
			Title:               title,
			Year:                year,
			Genres:              movie.Genres,
			ProductionCountries: movie.ProductionCountries,
			Budget:              movie.Budget,
		})
	}

	res := common.Batch[common.Movie]{
		Header: common.Header{
			Weight:      uint32(len(movies)),
			TotalWeight: -1,
		},
		Data: movies,
	}

	return res
}

func (p *Preprocessor) preprocessReviews(batch models.RawReviewBatch) common.ReviewsBatch {
	reviews := make([]common.ReviewToJoin, 0)

	for _, review := range batch.Reviews {
		id := review.UserID
		movieID := review.MovieID
		rating := review.Rating

		reviews = append(reviews, common.ReviewToJoin{
			ID:      id,
			MovieID: movieID,
			Rating:  rating,
		})
	}

	res := common.ReviewsBatch{
		Header: common.Header{
			Weight:      uint32(len(reviews)),
			TotalWeight: -1,
		},
		Reviews: reviews,
	}

	return res
}
