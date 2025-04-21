package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"pkg/models"
	"strconv"
	"tp-sistemas-distribuidos/server/common"
)

const (
	toPreProcess    = "to-preprocess"
	filterMoviesQ1  = "filter-year-q1"
	filterMoviesQ2  = "filter-production-q2"
	filterMovieQ3Q4 = "filter-year-q3q4"
	AnalyzerQueue   = "sentiment-analyzer"
	reviewsTopic    = "reviews-to-join-%d"
	reviewsExchange = "reviews-exchange"
	creditsTopic    = ""
	creditsExchange = ""
)

type PreprocessorConfig struct {
	RabbitUser string
	RabbitPass string
}

type Preprocessor struct {
	config           PreprocessorConfig
	middleware       *common.Middleware
	toProcessChan    <-chan common.Message
	shards           int
	reviewsChans     map[int]chan<- []byte
	moviesChans      []chan<- []byte
	pesoTotalQuePaso int
}

func NewPreprocessor(rabbitUser string, rabbitPass string, shards int) *Preprocessor {
	config := PreprocessorConfig{
		RabbitUser: rabbitUser,
		RabbitPass: rabbitPass,
	}

	Preprocessor := &Preprocessor{
		config:           config,
		reviewsChans:     map[int]chan<- []byte{},
		shards:           shards,
		pesoTotalQuePaso: 0,
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

	for i := range p.shards {
		shard := i + 1
		slog.Info("Creating channel to send reviews", slog.Int("shard", shard))
		//nextChan[i], err = middleware.GetChanWithTopicToSend(moviesExchange, fmt.Sprintf(topic, i))
		p.reviewsChans[shard], err = middleware.GetChanWithTopicToSend(reviewsExchange, fmt.Sprintf(reviewsTopic, shard))
		if err != nil {
			return fmt.Errorf("error getting channel to send reviews: %s", err)
		}

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
		payload any
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

		outCh = append(outCh, p.moviesChans...)

	case "reviews":
		var rb models.RawReviewBatch
		if err := json.Unmarshal(batch.Body, &rb); err != nil {
			return fmt.Errorf("error unmarshalling reviews batch: %w", err)
		}

		// TODO: CAMBIAR ESTE CODIGO HORRIPILANTE
		if rb.IsEof() {
			slog.Info("EOF reviews", slog.Any("total weight que paso ", p.pesoTotalQuePaso))
			eofReviews := makeEOFBatch[common.Review](rb.Header.TotalWeight)
			bytesEof, err := json.Marshal(eofReviews)
			if err != nil {
				return fmt.Errorf("error marshaling EOF reviews: %w", err)
			}
			for i := 0; i < p.shards; i++ {
				p.reviewsChans[i+1] <- bytesEof
			}
		} else {
			reviews := p.preprocessReviews(rb)
			p.pesoTotalQuePaso += int(reviews.Weight)
			reviewsDivided := divideReviews(reviews, p.shards)

			//slog.Info("dividing reviews", slog.Int("shards", p.shards), slog.String("divided length: ", strconv.Itoa(len(reviewsDivided.Data))))
			for i, reviewsShard := range reviewsDivided {
				slog.Info("Sending reviews")
				bytesReview, err := json.Marshal(reviewsShard)
				if err != nil {
					return fmt.Errorf("error marshaling reviews: %w", err)
				}
				p.reviewsChans[i+1] <- bytesReview
			}
		}

		slog.Debug("preprocessing reviews", slog.String("batch size: ", strconv.Itoa(int(rb.Header.Weight))))

	case "credits":
		var cb models.RawCreditBatch
		if err := json.Unmarshal(batch.Body, &cb); err != nil {
			return fmt.Errorf("error unmarshalling credits batch: %w", err)
		}

		slog.Debug("preprocessing credits", slog.String("batch size: ", strconv.Itoa(int(cb.Header.Weight))))
		if cb.IsEof() {
			payload = makeEOFBatch[common.Credit](cb.Header.TotalWeight)
		} else {
			payload = p.preprocessCredits(cb)
		}

		// TODO: Add the channel to send credits

	default:
		return fmt.Errorf("unknown batch type %q", batch.Type)
	}

	if payload == nil {
		return nil
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
			Revenue:             movie.Revenue,
		})
	}

	res := makeBatchMsg[common.Movie](batch.Header.Weight, movies)

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
	res := makeBatchMsg[common.Review](uint32(len(batch.Reviews)), reviews)

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

	res := makeBatchMsg[common.Credit](batch.Header.Weight, credits)

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

func divideReviews(batch common.Batch[common.Review], numberOfShards int) []common.Batch[common.Review] {
	reviewsShards := make([][]common.Review, numberOfShards)

	for i := range numberOfShards {
		reviewsShards[i] = common.Filter(batch.Data, func(review common.Review) bool {
			return common.GetShard(review.MovieID, numberOfShards) == i+1
		})
	}
	slog.Info("len reviews", slog.Any("reviews len", len(batch.Data)))
	if len(reviewsShards[0]) != len(batch.Data) {
		slog.Error("error dividing reviews")
	}
	return common.Map(reviewsShards, func(reviews []common.Review) common.Batch[common.Review] {
		header := batch.Header
		header.Weight = uint32(len(reviews))
		return common.Batch[common.Review]{
			Header: header,
			Data:   reviews,
		}
	})

}
