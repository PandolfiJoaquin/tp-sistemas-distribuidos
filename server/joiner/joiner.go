package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
)

const (
	moviesExchange  = "movies-exchange"
	moviestopic     = "movies-to-join-%d"
	reviewsExchange = "reviews-exchange"
	reviewsTopic    = "reviews-to-join-%d"
	creditExchange  = "credits-exchange"
	creditTopic     = "credits-to-join-%d"
)

const q3ToReduceQueue = "q3-to-reduce"
const q4ToReduceQueue = "q4-to-reduce"

type Joiner struct {
	joinerId   int
	middleware *common.Middleware
	session    *JoinerSession //para varios clientes convertir esto en un mapa [ClientId]JoinerSession
}

func NewJoiner(joinerId int, rabbitUser, rabbitPass string) (*Joiner, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return nil, err
	}

	return &Joiner{
		joinerId:   joinerId,
		middleware: middleware,
		session:    NewJoinerSession(),
	}, nil
}

func (j *Joiner) Start() {
	defer j.stop()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	moviesChan, err := j.middleware.GetChanWithTopicToRecv(moviesExchange, fmt.Sprintf(moviestopic, j.joinerId))
	//moviesChan, err := j.middleware.GetChanToRecv(fmt.Sprintf(moviesToJoinWithTopic, j.joinerId))
	if err != nil {
		slog.Error("Error creating channel", slog.String("queue", moviesExchange), slog.String("error", err.Error()))
		return
	}

	reviewsChan, err := j.middleware.GetChanWithTopicToRecv(reviewsExchange, fmt.Sprintf(reviewsTopic, j.joinerId))
	if err != nil {
		slog.Error("Error creating channel", slog.String("queue", reviewsExchange), slog.String("error", err.Error()))
		return
	}

	creditChan, err := j.middleware.GetChanWithTopicToRecv(creditExchange, fmt.Sprintf(creditTopic, j.joinerId))
	if err != nil {
		slog.Error("Error creating channel", slog.String("queue", creditExchange), slog.String("error", err.Error()))
		return
	}

	q3ToReduce, err := j.middleware.GetChanToSend(q3ToReduceQueue)
	if err != nil {
		slog.Error("error creating channel", slog.String("queue", q3ToReduceQueue), slog.String("error", err.Error()))
		return
	}

	q4ToReduce, err := j.middleware.GetChanToSend(q4ToReduceQueue)
	if err != nil {
		slog.Error("error creating channel", slog.String("queue", q4ToReduceQueue), slog.String("error", err.Error()))
		return
	}

	j.run(ctx, moviesChan, reviewsChan, creditChan, q3ToReduce, q4ToReduce)
}

func (j *Joiner) run(
	ctx context.Context,
	_moviesChan, _reviewsChan, _creditChan <-chan common.Message,
	q3ToReduce, q4ToReduce chan<- []byte,
) {
	dummyChan := make(<-chan common.Message)
	movies := _moviesChan
	reviews := dummyChan
	credits := dummyChan
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping joiner")
			return
		case msg := <-movies:
			var batch common.Batch[common.Movie]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			j.session.SaveMovies(batch)
			if j.session.AllMoviesReceived() {
				slog.Info("Received all movies. starting to pop reviews")
				reviews = _reviewsChan
				credits = _creditChan
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			continue

		case msg := <-reviews:
			var batch common.Batch[common.Review]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			j.session.NotifyReview(batch.Header)
			reviewXMovies := j.join(batch.Data)
			reviewsXMoviesBatch := common.Batch[common.MovieReview]{
				Header: batch.Header,
				Data:   reviewXMovies,
			}

			response, err := json.Marshal(reviewsXMoviesBatch)
			if err != nil {
				slog.Error("error marshalling batch", slog.String("error", err.Error()))
				continue
			}
			q3ToReduce <- response

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			continue
		case msg := <-credits:
			var batch common.Batch[common.Credit]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			j.session.NotifyCredit(batch.Header)

			actors := j.filterCredits(batch.Data)
			actorsBatch := common.Batch[common.Credit]{
				Header: batch.Header,
				Data:   actors,
			}

			response, err := json.Marshal(actorsBatch)
			if err != nil {
				slog.Error("error marshalling batch", slog.String("error", err.Error()))
				continue
			}
			q4ToReduce <- response

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			continue
		}

	}
}

func (j *Joiner) join(reviews []common.Review) []common.MovieReview {
	joinedReviews := common.Map(reviews, j.joinReview)
	return common.Flatten(joinedReviews)
}

func (j *Joiner) joinReview(r common.Review) []common.MovieReview {

	movies := j.session.GetMovies()
	moviesForReview := common.Filter(movies, func(m common.Movie) bool { return m.ID == r.MovieID })
	reviewXMovies := common.Map(moviesForReview, func(m common.Movie) common.MovieReview {
		return common.MovieReview{
			MovieID: m.ID,
			Title:   m.Title,
			Rating:  r.Rating,
		}
	})
	return reviewXMovies
}

func (j *Joiner) stop() {
	if err := j.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("joiner stopped")
}

func (j *Joiner) filterCredits(data []common.Credit) []common.Credit {
	movies := j.session.GetMovies()
	movieIds := common.Map(movies, func(m common.Movie) string { return m.ID })
	ids := make(map[string]bool)
	for _, id := range movieIds {
		ids[id] = true
	}
	actors := common.Filter(data, func(c common.Credit) bool {
		return ids[c.MovieId]
	})
	return actors
}
