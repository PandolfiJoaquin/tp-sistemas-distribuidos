package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

const reviewsToJoinQueue = "reviews-to-join"
const joinersTopic = "joiner-shards"
const moviesToJoinWithTopic = "movies-to-join-%d"

const nextStep = "q3-to-reduce"

type Joiner struct {
	joinerId       int
	middleware     *common.Middleware
	moviesReceived []common.Batch[common.Movie]
}

func NewJoiner(joinerId int, rabbitUser, rabbitPass string) (*Joiner, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return nil, err
	}
	return &Joiner{
		joinerId:       joinerId,
		middleware:     middleware,
		moviesReceived: []common.Batch[common.Movie]{},
	}, nil
}

func (j *Joiner) Start() {
	// moviesChan, err := j.middleware.GetChanWithTopicToRecv(joinersTopic, fmt.Sprintf(moviesToJoinWithTopic, j.joinerId))
	moviesChan, err := j.middleware.GetChanToRecv(fmt.Sprintf(moviesToJoinWithTopic, j.joinerId))
	if err != nil {
		slog.Error("Error creating channel", slog.String("queue", fmt.Sprintf(moviesToJoinWithTopic, j.joinerId)), slog.String("error", err.Error()))
		return
	}

	q3Chan, err := j.middleware.GetChanToSend(nextStep)
	if err != nil {
		slog.Error("error creating channel", slog.String("queue", nextStep), slog.String("error", err.Error()))
		return
	}

	go j.run(moviesChan, q3Chan)

	forever := make(chan bool)
	<-forever
}

func (j *Joiner) run(_moviesChan <-chan common.Message, nextStepChan chan<- []byte) {
	dummyChan := make(<-chan common.Message)
	movies := _moviesChan
	reviews := dummyChan

	for {
		select {
		case msg := <-movies:
			var batch common.Batch[common.Movie]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			slog.Info("Received message from movies", slog.Any("batch", batch))
			j.saveBatch(batch)
			if j.allMoviesReceived() {
				slog.Info("Received all movies. starting to pop reviews")
				var err error
				reviews, err = j.middleware.GetChanToRecv(reviewsToJoinQueue)
				if err != nil {
					slog.Error("error creating channel", slog.String("queue", reviewsToJoinQueue), slog.String("error", err.Error()))
					return
				}

			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			continue

		case msg := <-reviews:
			slog.Info("Received message from reviews")
			var batch common.Batch[common.Review]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

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
			slog.Info("Sending msgs: ", slog.Any("response", reviewsXMoviesBatch))
			nextStepChan <- response

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

	movies := j.getMovies()
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

func (j *Joiner) getMovies() []common.Movie {
	//TODO: movies shouldn't be stored in batches. when loading from disk this would change
	return common.Flatten(common.Map(j.moviesReceived, func(batch common.Batch[common.Movie]) []common.Movie { return batch.Data }))
}

func (j *Joiner) saveBatch(batch common.Batch[common.Movie]) {
	j.moviesReceived = append(j.moviesReceived, batch)
}

// allMoviesReceived checks if it has all necessary movies to run joining with reviews
func (j *Joiner) allMoviesReceived() bool {
	eofBatch := common.First(j.moviesReceived, func(b common.Batch[common.Movie]) bool { return b.IsEof() })
	if eofBatch == nil {
		return false
	}

	totalWeight := int(eofBatch.Header.TotalWeight)
	weightsReceived := common.Map(j.moviesReceived, func(b common.Batch[common.Movie]) uint32 { return b.Header.Weight })
	totalWeightReceived := common.Sum(weightsReceived)

	if totalWeightReceived > totalWeight {
		slog.Error("total weight received is greater than total weight")
		return false
	}
	return totalWeightReceived == totalWeight

}

func (j *Joiner) Close() error {
	if err := j.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
		return err
	}
	return nil
}
