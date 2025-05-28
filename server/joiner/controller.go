package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/persistency"
)

const (
	rabbitHost = "rabbitmq"
	//rabbitHost      = "127.0.0.1"
	moviesExchange       = "movies-exchange"
	moviestopic          = "movies-to-join-%d"
	reviewsExchange      = "reviews-exchange"
	reviewsTopic         = "reviews-to-join-%d"
	creditExchange       = "credits-exchange"
	creditTopic          = "credits-to-join-%d"
	q3ToReduceQueue      = "q3-to-reduce"
	q4ToReduceQueue      = "q4-to-reduce"
	maxTransactionsOnLog = 50
)

type LogOperations string

const ( //TODO: Optimize encoding
	UpdateMoviesWeightsOp     LogOperations = "UpdateMoviesWeights"
	SaveMoviesOp                            = "SaveMovies"
	UpdateReviewsWeightsOp                  = "UpdateReviewsWeights"
	StoreReviewBatchOp                      = "StoreReviewBatch"
	JoinStoredReviewBatchesOp               = "JoinStoredReviewBatches"
	UpdateCreditsWeightsOp                  = "UpdateCreditsWeights"
	ExorciseSessionOp                       = "ExorciseSession"
)

type JoinerController struct {
	joinerId            int
	middleware          *common.Middleware
	Sessions            map[string]*JoinerSession                `json:"sessions"`
	StoredReviewBatches map[string][]common.Batch[common.Review] `json:"storedReviewBatches"`
	transactionsOnLog   int
}

func NewJoinerController(joinerId int, rabbitUser, rabbitPass string) (*JoinerController, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return nil, err
	}

	controller := &JoinerController{
		joinerId:            joinerId,
		middleware:          middleware,
		Sessions:            make(map[string]*JoinerSession),
		StoredReviewBatches: make(map[string][]common.Batch[common.Review]),
	}
	recoveredData, err := persistency.Recover()
	if err != nil {
		slog.Error("error recovering persistency", slog.String("error", err.Error()))
	}

	if len(recoveredData) != 0 {
		if err = json.Unmarshal(recoveredData, controller); err != nil {
			slog.Error("error unmarshalling persistency", slog.String("error", err.Error()))
		}
	}

	return controller, nil
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

func (j *JoinerController) joinReviewBatch(clientId string, batch common.Batch[common.Review], q3ToReduce chan<- []byte) {
	session := j.getSession(clientId)
	session.UpdateReviewsWeights(batch.Header)

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

func (j *JoinerController) storeReviewBatch(clientId string, batch common.Batch[common.Review]) {
	j.StoredReviewBatches[clientId] = append(j.StoredReviewBatches[clientId], batch)
}

func (j *JoinerController) joinStoredReviewBatches(clientId string, q3ToReduce chan<- []byte) {
	slog.Info("joining stored review batches", slog.String("clientId", clientId))
	batches := j.StoredReviewBatches[clientId]
	j.StoredReviewBatches[clientId] = []common.Batch[common.Review]{}
	for _, batch := range batches {
		j.joinReviewBatch(clientId, batch, q3ToReduce)
		j.exorciseSession(clientId)
	}
}

func (j *JoinerController) save(transaction persistency.Transaction) {
	jsonData, err := json.Marshal(j)
	if err != nil {
		slog.Error("error marshalling internal state", slog.String("error", err.Error()))
		return
	}

	if err := persistency.Commit(transaction); err != nil {
		slog.Error("error committing transaction", slog.String("error", err.Error()))
		return
	}
	j.transactionsOnLog++

	if j.transactionsOnLog == maxTransactionsOnLog {
		if err = persistency.SaveCheckpoint(jsonData); err != nil {
			slog.Error("error saving checkpoint", slog.String("error", err.Error()))
			return
		}
		j.transactionsOnLog = 0
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
			transaction := persistency.NewTransaction()
			var batch common.Batch[common.Movie]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			clientId := batch.GetClientID()
			session := j.getSession(clientId)

			session.UpdateMoviesWeights(batch.Header)
			transaction.Do(string(UpdateMoviesWeightsOp), batch.Header.ToString())

			session.SaveMovies(batch.Data)
			transaction.Do(string(SaveMoviesOp), batch.Data.ToString())

			if session.AllMoviesReceived() {
				slog.Info("Received all movies. starting to pop reviews")
				reviews = _reviewsChan
				credits = _creditChan
				j.joinStoredReviewBatches(clientId, q3ToReduce) // Joins all reviews stored
			}

			j.save(transaction)

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}

		case msg := <-reviews:
			var batch common.Batch[common.Review]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			clientId := batch.GetClientID()
			session := j.getSession(clientId)

			if !session.AllMoviesReceived() {
				j.storeReviewBatch(clientId, batch)
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			j.joinReviewBatch(clientId, batch, q3ToReduce)

			j.exorciseSession(clientId)

			j.save()

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}

		case msg := <-credits:
			var batch common.Batch[common.Credit]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			clientId := batch.GetClientID()
			session := j.getSession(clientId)
			session.UpdateCreditsWeights(batch.Header)

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

			j.exorciseSession(clientId)

			j.save()

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (j *JoinerController) getSession(clientId string) *JoinerSession {
	if _, ok := j.Sessions[clientId]; !ok {
		slog.Info("New client detected. creating session", slog.String("clientId", string(clientId)))
		j.Sessions[clientId] = NewJoinerSession()
	}
	return j.Sessions[clientId]
}

// if the session is done, delete it
func (j *JoinerController) exorciseSession(id string) {
	if j.Sessions[id].IsDone() {
		slog.Info("Done for client", slog.String("clientId", id))
		delete(j.Sessions, id)
		slog.Info("Successfully deleted session", slog.String("clientId", id))
	}
}

func (j *JoinerController) stop() {
	if err := j.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("joiner stopped")
}
