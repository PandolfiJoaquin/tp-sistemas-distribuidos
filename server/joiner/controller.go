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

type JoinerController struct {
	joinerId            int
	middleware          *common.Middleware
	sessions            map[common.ClientId]*JoinerService
	storedReviewBatches map[common.ClientId][]common.Batch[common.Review]
}

func NewJoinerController(joinerId int, rabbitUser, rabbitPass string) (*JoinerController, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return nil, err
	}

	return &JoinerController{
		joinerId:            joinerId,
		middleware:          middleware,
		sessions:            map[common.ClientId]*JoinerService{},
		storedReviewBatches: map[common.ClientId][]common.Batch[common.Review]{},
	}, nil
}

func (j *JoinerController) Start() {
	defer j.stop()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	moviesChan, err := j.middleware.GetChanWithTopicToRecv(moviesExchange, fmt.Sprintf(moviestopic, j.joinerId))
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

func (j *JoinerController) joinReviewBatch(clientId common.ClientId, batch common.Batch[common.Review], q3ToReduce chan<- []byte) {
	session := j.getSession(clientId)
	session.NotifyReview(batch.Header)

	reviewXMovies := session.Join(batch.Data)
	reviewsXMoviesBatch := common.Batch[common.MovieReview]{
		Header: batch.Header,
		Data:   reviewXMovies,
	}

	response, err := json.Marshal(reviewsXMoviesBatch)
	if err != nil {
		slog.Error("error marshalling batch", slog.String("error", err.Error()))
	}
	q3ToReduce <- response
}

func (j *JoinerController) storeReviewBatch(clientId common.ClientId, batch common.Batch[common.Review]) {
	j.storedReviewBatches[clientId] = append(j.storedReviewBatches[clientId], batch)
}

func (j *JoinerController) joinStoredReviewBatches(clientId common.ClientId, q3ToReduce chan<- []byte) {
	slog.Info("joining stored review batches", slog.String("clientId", string(clientId)))
	batches := j.storedReviewBatches[clientId]
	j.storedReviewBatches[clientId] = []common.Batch[common.Review]{}
	for _, batch := range batches {
		j.joinReviewBatch(clientId, batch, q3ToReduce)
		j.cleanSession(clientId)
	}
}

func (j *JoinerController) run(
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
			clientId := batch.GetClientId()
			session := j.getSession(clientId)
			session.SaveMovies(batch)
			if session.AllMoviesReceived() {
				slog.Info("Received all movies. starting to pop reviews")
				reviews = _reviewsChan
				credits = _creditChan
				go j.joinStoredReviewBatches(clientId, q3ToReduce) // Joins all reviews stored
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}

		case msg := <-reviews:
			var batch common.Batch[common.Review]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			clientId := batch.GetClientId()
			session := j.getSession(clientId)

			if !session.AllMoviesReceived() {
				slog.Info("Not all movies received. storing in memory")
				j.storeReviewBatch(clientId, batch)
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			j.joinReviewBatch(clientId, batch, q3ToReduce)
			
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			j.cleanSession(clientId)
			
		case msg := <-credits:
			var batch common.Batch[common.Credit]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			clientId := batch.GetClientId()
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
		}

	}
}

func (j *JoinerController) getSession(clientId common.ClientId) *JoinerService {
	if _, ok := j.sessions[clientId]; !ok {
		slog.Info("New client detected. creating session", slog.String("clientId", string(clientId)))
		j.sessions[clientId] = NewJoinerService()
	}
	return j.sessions[clientId]
}

func (j *JoinerController) cleanSession(id common.ClientId) {
	if j.sessions[id].IsDone() {
		slog.Info("Done for client", slog.String("clientId", string(id)))
		delete(j.sessions, id)
		slog.Info("Successfully deleted session", slog.String("clientId", string(id)))
	}
}

func (j *JoinerController) stop() {
	if err := j.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("joiner stopped")
}
