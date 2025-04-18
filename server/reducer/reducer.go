package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

const (
	previousQueueQuery2 = "q2-to-reduce"
	previousQueueQuery3 = "q3-to-reduce"
	previousQueueQuery4 = "q4-to-reduce"
	previousQueueQuery5 = "q5-to-reduce"

	nextQueueQuery2 = "q2-to-final-reduce"
	nextQueueQuery3 = "q3-to-final-reduce"
	nextQueueQuery4 = "q4-to-final-reduce"
	nextQueueQuery5 = "q5-to-final-reduce"
)

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

func (b *ReviewsXMoviesBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}

type Reducer struct {
	middleware  *common.Middleware
	connections map[string]connection
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewReducer(rabbitUser, rabbitPass string) (*Reducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connections, err := initializeConnections(middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &Reducer{middleware: middleware, connections: connections}, nil
}

func initializeConnection(middleware *common.Middleware, previousQueue, nextQueue string) (connection, error) {
	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}
	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to send: %w", nextQueue, err)
	}
	return connection{previousChan, nextChan}, nil
}

func initializeConnections(middleware *common.Middleware) (map[string]connection, error) {
	connectionQ2, err := initializeConnection(middleware, previousQueueQuery2, nextQueueQuery2)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery2, err)
	}
	connectionQ3, err := initializeConnection(middleware, previousQueueQuery3, nextQueueQuery3)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery3, err)
	}
	connectionQ4, err := initializeConnection(middleware, previousQueueQuery4, nextQueueQuery4)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery4, err)
	}
	connectionQ5, err := initializeConnection(middleware, previousQueueQuery5, nextQueueQuery5)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery5, err)
	}

	connections := map[string]connection{
		previousQueueQuery2: connectionQ2,
		previousQueueQuery3: connectionQ3,
		previousQueueQuery4: connectionQ4,
		previousQueueQuery5: connectionQ5,
	}

	return connections, nil
}

func (r *Reducer) Start() {
	go r.startReceiving()
	forever := make(chan bool)
	<-forever
}

func (r *Reducer) startReceiving() {
	for {
		select {
		case msg := <-r.connections[previousQueueQuery2].ChanToRecv:
			if err := r.processQuery2Message(msg); err != nil {
				slog.Error("error processing query2 message", slog.String("error", err.Error())) //TODO: Ack?
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query2 message", slog.String("error", err.Error()))
			}
		case msg := <-r.connections[previousQueueQuery3].ChanToRecv:
			if err := r.processQuery3Message(msg); err != nil {
				slog.Error("error processing query3 message", slog.String("error", err.Error())) //TODO: Ack?
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query3 message", slog.String("error", err.Error()))
			}
			// case msg := <-r.connections[previousQueueQuery4].ChanToRecv:
			// 	if err := processQueryMessage(msg, r.reduceAndSendQ4); err != nil {
			// 		slog.Error("error processing query4 message", slog.String("error", err.Error()))
			// 	}
			// case msg := <-r.connections[previousQueueQuery5].ChanToRecv:
			// 	if err := processQueryMessage(msg, r.reduceAndSendQ5); err != nil {
			// 		slog.Error("error processing query5 message", slog.String("error", err.Error()))
			// 	}
		}
	}
}

func (r *Reducer) processQuery2Message(msg common.Message) error {
	var batch common.Batch
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 2 message: %w", err)
	}

	response := msg.Body
	if !batch.IsEof() {
		reduced, err := r.reduceQ2(batch)
		if err != nil {
			return fmt.Errorf("error processing query2 message: %w", err)
		}
		response, err = json.Marshal(reduced)
		if err != nil {
			return fmt.Errorf("error marshalling response: %w", err)
		}
	}

	r.connections[nextQueueQuery2].ChanToSend <- response
	return nil
}

func (r *Reducer) processQuery3Message(msg common.Message) error {
	var batch ReviewsXMoviesBatch
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 3 message: %w", err)
	}

	response := msg.Body
	if !batch.IsEof() {
		reduced, err := r.reduceQ3(batch)
		if err != nil {
			return fmt.Errorf("error processing query3 message: %w", err)
		}
		response, err = json.Marshal(reduced)
		if err != nil {
			return fmt.Errorf("error marshalling response: %w", err)
		}
	}

	r.connections[nextQueueQuery3].ChanToSend <- response
	return nil
}

func (r *Reducer) reduceQ2(batch common.Batch) (common.CoutriesBudgetMsg, error) {
	// me llega un mensaje de peliculas y tengo que reducirlo a un map con cada entrada (pais, $$), me viene filtrado
	countries := make(common.CoutriesBudgetMsg)
	for _, movie := range batch.Movies {
		if len(movie.ProductionCountries) > 1 {
			return nil, fmt.Errorf("movie has more than 1 production country for query 2, movie: %v", movie)
		}
		countries[movie.ProductionCountries[0]] += movie.Budget
	}

	return countries, nil
}

func (r *Reducer) reduceQ3(batch ReviewsXMoviesBatch) (common.MoviesAvgRatingMsg, error) {
	// me llega un mensaje de peliculasXReviews y tengo que reducirlo a un map con cada entrada (peli, sum(ratings), cant_reviews), me viene filtrado
	movieRatings := make(common.MoviesAvgRatingMsg)
	for _, movieRating := range batch.ReviewsXMovies {
		if previousRating, ok := movieRatings[movieRating.MovieID]; !ok {
			movieRatings[movieRating.MovieID] = common.RatingAvg{RatingSum: movieRating.Rating, RatingCount: 1}
		} else {
			movieRatings[movieRating.MovieID] = common.RatingAvg{RatingSum: previousRating.RatingSum + movieRating.Rating, RatingCount: previousRating.RatingCount + 1}
		}
	}
	return movieRatings, nil
}

// func (r *Reducer) reduceAndSendQ4(batch common.Batch) error {
// 	// me llega un mensaje de peliculasXactores y tengo que reducirlo a un map con cada entrada (actor, cant_pelis), me viene filtrado
// 	return nil
// }

// func (r *Reducer) reduceAndSendQ5(batch common.Batch) error {
// 	// me llega un mensaje de peliculas+overview y tengo que reducirlo a un struct con 2 campos, positivo y negativo,
// 	// dentro de cada uno tengo sum(tasa(ingreso/presupuesto)) y cant_pelis, me viene filtrado
// 	return nil
// }

func (r *Reducer) Stop() error {
	return r.middleware.Close()
}
