package main

import (
	"encoding/json"
	"log/slog"
	"time"

	"tp-sistemas-distribuidos/server/common"
)

const nextQueue = ""
const previousQueue = ""
const Q3ToReduceQueue = "q3-to-reduce"
const ON = 1
const OFF = 0

// ReviewToJoin represents a review that should be join with a movie
type ReviewToJoin struct {
	ID      string `json:"id"`
	MovieID string `json:"movie_id"`
	Rating  uint32 `json:"rating"`
}

// ReviewsBatch represents a batch of reviews
type ReviewsBatch struct {
	Header  common.Header  `json:"header"`
	Reviews []ReviewToJoin `json:"reviews"`
}

// ReviewXMovies represents a review joined with a movie
type ReviewXMovies struct {
	MovieID string `json:"movie_id"`
	Title   string `json:"title"`
	Rating  uint32 `json:"rating"`
}

// ReviewsXMoviesBatch represents a batch of reviews joined with movies
type ReviewsXMoviesBatch struct {
	Header         common.Header   `json:"header"`
	ReviewsXMovies []ReviewXMovies `json:"reviews_x_movies"`
}
type Joiner struct {
	rabbitUser     string
	rabbitPass     string
	middleware     *common.Middleware
	moviesReceived []common.Batch
}

func NewJoiner(rabbitUser, rabbitPass string) *Joiner {
	return &Joiner{
		rabbitUser:     rabbitUser,
		rabbitPass:     rabbitPass,
		moviesReceived: []common.Batch{},
	}
}

func (j *Joiner) Start() {
	middleware, err := common.NewMiddleware(j.rabbitUser, j.rabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return
	}
	j.middleware = middleware

	defer func() {
		if err := middleware.Close(); err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}()

	moviesChan, err := mockMoviesChan()
	if err != nil {
		slog.Error("Error creating mocked movies", slog.String("queue", previousQueue), slog.String("error", err.Error()))
		return
	}

	reviewsChan, err := mockReviewsChan()
	if err != nil {
		slog.Error("error creating mocked reviews", slog.String("queue", nextQueue), slog.String("error", err.Error()))
		return
	}

	q3Chan, err := mockQ3Chan()
	if err != nil {
		slog.Error("error creating mocked q3 to send to", slog.String("queue", Q3ToReduceQueue), slog.String("error", err.Error()))
		return
	}

	go j.start(moviesChan, reviewsChan, q3Chan)

	forever := make(chan bool)
	<-forever
}

func (j *Joiner) start(moviesChan, reviewsChan <-chan common.Message, nextStepChan chan<- []byte) {
	dummyChan := make(<-chan common.Message, 1)
	movies := []<-chan common.Message{dummyChan, moviesChan}
	moviesStatus := ON
	reviews := []<-chan common.Message{dummyChan, reviewsChan}
	reviewsStatus := OFF

	for {
		slog.Info("Receiving messages", slog.Any("moviesStatus:", moviesStatus), slog.Any("reviewsStatus", reviewsStatus))
		select {
		case msg := <-movies[moviesStatus]:
			slog.Info("Received message from movies")
			var batch common.Batch
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			j.saveBatch(batch)
			if j.allMoviesReceived() {
				slog.Info("Received all movies. starting to pop reviews")
				reviewsStatus = ON
			}
			//TODO: Uncomment this when using middleware
			//if err := msg.Ack(); err != nil {
			//	slog.Error("error acknowledging message", slog.String("error", err.Error()))
			//}
			continue

		case msg := <-reviews[reviewsStatus]:
			slog.Info("Received message from reviews")
			var batch ReviewsBatch
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			reviewXMovies := j.join(batch.Reviews)
			reviewsXMoviesBatch := ReviewsXMoviesBatch{
				Header:         batch.Header,
				ReviewsXMovies: reviewXMovies,
			}

			response, err := json.Marshal(reviewsXMoviesBatch)
			if err != nil {
				slog.Error("error marshalling batch", slog.String("error", err.Error()))
				continue
			}
			nextStepChan <- response

			//TODO: Uncomment this when using middleware
			//if err := msg.Ack(); err != nil {
			//	slog.Error("error acknowledging message", slog.String("error", err.Error()))
			//}
			continue
		}
	}
}

func (j *Joiner) join(reviews []ReviewToJoin) []ReviewXMovies {
	joinedReviews := common.Map(reviews, j.joinReview)
	return common.Flatten(joinedReviews)
}

func (j *Joiner) joinReview(r ReviewToJoin) []ReviewXMovies {

	movies := j.getMovies()
	moviesForReview := common.Filter(movies, func(m common.Movie) bool { return m.ID == r.MovieID })
	reviewXMovies := common.Map(moviesForReview, func(m common.Movie) ReviewXMovies {
		return ReviewXMovies{
			MovieID: m.ID,
			Title:   m.Title,
			Rating:  r.Rating,
		}
	})
	return reviewXMovies
}

func (j *Joiner) getMovies() []common.Movie {
	//TODO: movies shouldn't be stored in batches. when loading from disk this would change
	return common.Flatten(common.Map(j.moviesReceived, func(batch common.Batch) []common.Movie { return batch.Movies }))
}

func (j *Joiner) saveBatch(batch common.Batch) {
	j.moviesReceived = append(j.moviesReceived, batch)
}

// allMoviesReceived checks if it has all necessary movies to start joining with reviews
func (j *Joiner) allMoviesReceived() bool {
	eofBatch := common.First(j.moviesReceived, func(b common.Batch) bool { return b.IsEof() })
	if eofBatch == nil {
		return false
	}

	totalWeight := int(eofBatch.Header.TotalWeight)
	weightsReceived := common.Map(j.moviesReceived, func(b common.Batch) uint32 { return b.Header.Weight })
	totalWeightReceived := common.Sum(weightsReceived)

	if totalWeightReceived > totalWeight {
		slog.Error("total weight received is greater than total weight")
		return false
	}
	return totalWeightReceived == totalWeight

}

func mockMoviesChan() (<-chan common.Message, error) {
	moviesChan := make(chan common.Message, 1)
	go func() {
		batch := common.MockedBatch
		batchSerialized, err := json.Marshal(batch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			return
		}

		eofSerialized, err := json.Marshal(common.EOF)
		if err != nil {
			slog.Error("error marshalling EOF", slog.String("error", err.Error()))
			return
		}
		time.Sleep(5 * time.Second)
		moviesChan <- common.Message{Body: batchSerialized}
		moviesChan <- common.Message{Body: eofSerialized}
		slog.Info("[mockMovieSource] Sent movies")
	}()
	return moviesChan, nil
}

func mockReviewsChan() (<-chan common.Message, error) {
	reviewsChan := make(chan common.Message, 1)
	go func() {
		time.Sleep(10 * time.Second)
		batch := ReviewsBatch{Reviews: []ReviewToJoin{
			{ID: "1", MovieID: "1", Rating: 5},
			{ID: "2", MovieID: "1", Rating: 4},
			{ID: "3", MovieID: "2", Rating: 3},
		}}
		serializedBatch, err := json.Marshal(batch)
		if err != nil {
			slog.Error("error marshalling batch", slog.String("error", err.Error()))
			return
		}

		reviewsChan <- common.Message{
			Body: serializedBatch,
		}
	}()
	return reviewsChan, nil
}

func mockQ3Chan() (chan<- []byte, error) {
	moviesChan := make(chan []byte, 1)
	go func() {
		time.Sleep(15 * time.Second)
		response := <-moviesChan
		slog.Info("[Mock] received message from Joiner", slog.Any("message", string(response)))
	}()
	return moviesChan, nil

}
