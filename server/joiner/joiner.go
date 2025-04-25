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
	q3ToReduceQueue = "q3-to-reduce"
	q4ToReduceQueue = "q4-to-reduce"
)

type Joiner struct {
	joinerId   int
	middleware *common.Middleware
	sessions   map[common.ClientId]*JoinerSession //para varios clientes convertir esto en un mapa [ClientId]JoinerSession
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
		sessions:   map[common.ClientId]*JoinerSession{},
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
		clientId := common.ClientId("") //TODO: Esto se deberia tomar del header del mensaje
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
			session := j.getSession(clientId)
			session.SaveMovies(batch)
			if session.AllMoviesReceived() {
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
			session := j.getSession(clientId)
			session.NotifyReview(batch.Header)
			reviewXMovies := session.join(batch.Data)
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
			j.cleanSession(clientId)
			continue
		case msg := <-credits:
			var batch common.Batch[common.Credit]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			session := j.getSession(clientId)
			session.NotifyCredit(batch.Header)

			actors := session.filterCredits(batch.Data)
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
			j.cleanSession(clientId)
			continue
		}

	}
}

func (j *Joiner) getSession(clientId common.ClientId) *JoinerSession {
	if _, ok := j.sessions[clientId]; !ok {
		slog.Info("New client detected. creating session", slog.String("clientId", string(clientId)))
		j.sessions[clientId] = NewJoinerSession()
	}
	return j.sessions[clientId]
}

func (j *Joiner) cleanSession(id common.ClientId) {
	if j.sessions[id].IsDone() {
		slog.Info("Done for client", slog.String("clientId", string(id)))
		delete(j.sessions, id)
		slog.Info("Successfully deleted session", slog.String("clientId", string(id)))
	}
}

func (j *Joiner) stop() {
	if err := j.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("joiner stopped")
}
