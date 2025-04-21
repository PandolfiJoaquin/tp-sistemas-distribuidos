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
const AnalyzerQueue = "sentiment-analyzer"
const reviewsQueue = "reviews-to-join"

// TODO: credits queue
//const CreditsQueue =

type PreprocessorConfig struct {
	RabbitUser string
	RabbitPass string
}

type Preprocessor struct {
	config        PreprocessorConfig
	middleware    *common.Middleware
	toProcessChan <-chan common.Message
	reviewsChan   chan<- []byte

	moviesChans []chan<- []byte
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

	moviesToAnalyze, err := middleware.GetChanToSend(AnalyzerQueue)
	if err != nil {
		return fmt.Errorf("error getting channel to send movies: %s", err)
	}

	moviesChans = append(moviesChans, moviesToFilterQ1)
	moviesChans = append(moviesChans, moviesToFilterQ2)
	moviesChans = append(moviesChans, moviesToFilterQ3Q4)
	moviesChans = append(moviesChans, moviesToAnalyze)

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
		payload interface{}
		outCh   []chan<- []byte
	)

	switch batch.Type {
	case "movies":
		var mb models.RawMovieBatch
		if err := json.Unmarshal(batch.Body, &mb); err != nil {
			return fmt.Errorf("error unmarshalling movies batch: %w", err)
		}
		if mb.IsEof() {
			payload = makeEOFBatch[common.Movie](mb.Header.TotalWeight)
		} else {
			payload = p.preprocessMovies(mb)
		}

		slog.Info("preprocessing movies ", slog.String("batch size: ", strconv.Itoa(int(mb.Header.Weight))))

		for _, moviesChan := range p.moviesChans {
			outCh = append(outCh, moviesChan)
		}

	case "reviews":
		var rb models.RawReviewBatch
		if err := json.Unmarshal(batch.Body, &rb); err != nil {
			return fmt.Errorf("error unmarshalling reviews batch: %w", err)
		}

		if rb.IsEof() {
			payload = makeEOFBatch[common.Review](rb.Header.TotalWeight)
		} else {
			payload = p.preprocessReviews(rb)
		}

		slog.Info("preprocessing reviews", slog.String("batch size: ", strconv.Itoa(int(rb.Header.Weight))))

		outCh = append(outCh, p.reviewsChan)

	case "credits":
		var cb models.RawCreditBatch
		if err := json.Unmarshal(batch.Body, &cb); err != nil {
			return fmt.Errorf("error unmarshalling credits batch: %w", err)
		}

		slog.Info("preprocessing credits", slog.String("batch size: ", strconv.Itoa(int(cb.Header.Weight))))
		if cb.IsEof() {
			payload = makeEOFBatch[common.Credit](cb.Header.TotalWeight)
		} else {
			payload = p.preprocessCredits(cb)
		}

		// TODO: Add the channel to send credits

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
			Overview:            movie.Overview,
		})
	}

	res := makeBatchMsg[common.Movie](uint32(len(movies)), movies)

	return res
}

func (p *Preprocessor) preprocessReviews(batch models.RawReviewBatch) common.Batch[common.Review] {
	reviews := make([]common.Review, 0)

	for _, review := range batch.Reviews {
		id := review.UserID
		movieID := review.MovieID
		rating := review.Rating

		reviews = append(reviews, common.Review{
			ID:      id,
			MovieID: movieID,
			Rating:  rating,
		})
	}

	res := makeBatchMsg[common.Review](uint32(len(reviews)), reviews)

	return res
}

func (p *Preprocessor) preprocessCredits(batch models.RawCreditBatch) common.Batch[common.Credit] {
	credits := make([]common.Credit, 0)

	for _, credit := range batch.Credits {
		movieID := credit.MovieId
		actors := make([]common.Actor, 0)

		for _, actor := range credit.Cast {
			actors = append(actors, common.Actor{
				ActorID: strconv.Itoa(actor.ID),
				Name:    actor.Name,
			})
		}

		credits = append(credits, common.Credit{
			MovieId: movieID,
			Actors:  actors,
		})
	}

	res := makeBatchMsg[common.Credit](uint32(len(credits)), credits)

	return res
}

func makeEOFBatch[T any](totalWeight int32) common.Batch[T] {
	return common.Batch[T]{
		Header: common.Header{
			Weight:      0,
			TotalWeight: totalWeight,
		},
		Data: []T{},
	}
}

func makeBatchMsg[T any](weight uint32, data []T) common.Batch[T] {
	return common.Batch[T]{
		Header: common.Header{
			Weight:      weight,
			TotalWeight: -1,
		},
		Data: data,
	}
}
