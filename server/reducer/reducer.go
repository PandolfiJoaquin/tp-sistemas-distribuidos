package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

type queuesNames struct {
	previousQueue string
	nextQueue     string
}

var queriesQueues = map[int]queuesNames{
	2: {previousQueue: "q2-to-reduce", nextQueue: "q2-to-final-reduce"},
	3: {previousQueue: "q3-to-reduce", nextQueue: "q3-to-final-reduce"},
	4: {previousQueue: "q4-to-reduce", nextQueue: "q4-to-final-reduce"},
	5: {previousQueue: "q5-to-reduce", nextQueue: "q5-to-final-reduce"},
}

type Reducer struct {
	middleware  *common.Middleware
	connections map[int]connection
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

func initializeConnections(middleware *common.Middleware) (map[int]connection, error) {
	connections := make(map[int]connection)
	for queryNum, queuesNames := range queriesQueues {
		connection, err := initializeConnection(middleware, queuesNames.previousQueue, queuesNames.nextQueue)
		if err != nil {
			return nil, fmt.Errorf("error initializing connection for %s: %w", queuesNames.previousQueue, err)
		}
		connections[queryNum] = connection
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
		case msg := <-r.connections[2].ChanToRecv:
			slog.Info("received message from query 2", slog.String("message", string(msg.Body)))
			if err := r.processQuery2Message(msg); err != nil {
				slog.Error("error processing query2 message", slog.String("error", err.Error())) //TODO: Ack?
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query2 message", slog.String("error", err.Error()))
			}
			slog.Info("acknowledged query2 message")
		case msg := <-r.connections[3].ChanToRecv:
			slog.Info("received message from query 3", slog.String("message", string(msg.Body)))
			if err := r.processQuery3Message(msg); err != nil {
				slog.Error("error processing query3 message", slog.String("error", err.Error())) //TODO: Ack?
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query3 message", slog.String("error", err.Error()))
			}
			// case msg := <-r.connections[4].ChanToRecv:
			// 	if err := processQueryMessage(msg, r.reduceAndSendQ4); err != nil {
			// 		slog.Error("error processing query4 message", slog.String("error", err.Error()))
			// 	}
			// case msg := <-r.connections[5].ChanToRecv:
			// 	if err := processQueryMessage(msg, r.reduceAndSendQ5); err != nil {
			// 		slog.Error("error processing query5 message", slog.String("error", err.Error()))
			// 	}
		}
	}
}

func (r *Reducer) processQuery2Message(msg common.Message) error {
	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 2 message: %w", err)
	}
	reduced, err := r.reduceQ2(batch)
	if err != nil {
		return fmt.Errorf("error processing query2 message: %w", err)
	}
	response, err := json.Marshal(reduced)
	if err != nil {
		return fmt.Errorf("error marshalling response: %w", err)
	}
	r.connections[2].ChanToSend <- response
	return nil
}

func (r *Reducer) processQuery3Message(msg common.Message) error {
	var batch common.Batch[common.MovieReview]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 3 message: %w", err)
	}

	reduced, err := r.reduceQ3(batch)
	if err != nil {
		return fmt.Errorf("error processing query3 message: %w", err)
	}
	response, err := json.Marshal(reduced)
	if err != nil {
		return fmt.Errorf("error marshalling response: %w", err)
	}

	r.connections[3].ChanToSend <- response
	return nil
}

func (r *Reducer) reduceQ2(batch common.Batch[common.Movie]) (common.CoutriesBudgetMsg, error) {
	// me llega un mensaje de peliculas y tengo que reducirlo a un map con cada entrada (pais, $$), me viene filtrado
	countries := make(map[pkg.Country]uint64)
	for _, movie := range batch.Data {
		if len(movie.ProductionCountries) > 1 {
			return common.CoutriesBudgetMsg{}, fmt.Errorf("movie has more than 1 production country for query 2, movie: %v", movie)
		}
		countries[movie.ProductionCountries[0]] += movie.Budget
	}

	countriesList := make([]common.CountryBudget, 0, len(countries))
	for country, budget := range countries {
		countriesList = append(countriesList, common.CountryBudget{Country: country, Budget: budget})
	}

	return common.CoutriesBudgetMsg{
		Header:    batch.Header,
		Countries: countriesList,
	}, nil
}

func (r *Reducer) reduceQ3(batch common.Batch[common.MovieReview]) (common.MoviesAvgRatingMsg, error) {
	// me llega un mensaje de peliculasXReviews y tengo que reducirlo a un map con cada entrada (peli, sum(ratings), cant_reviews), me viene filtrado
	movieRatings := make(map[string]common.MovieAvgRating)
	for _, movieRating := range batch.Data {
		if previousRating, ok := movieRatings[movieRating.MovieID]; !ok {
			movieRatings[movieRating.MovieID] = common.MovieAvgRating{MovieID: movieRating.MovieID, RatingSum: movieRating.Rating, RatingCount: 1}
		} else {
			movieRatings[movieRating.MovieID] = common.MovieAvgRating{MovieID: movieRating.MovieID, RatingSum: previousRating.RatingSum + movieRating.Rating, RatingCount: previousRating.RatingCount + 1}
		}
	}

	moviesRatingsList := make([]common.MovieAvgRating, 0, len(movieRatings))
	for _, movieRating := range movieRatings {
		moviesRatingsList = append(moviesRatingsList, movieRating)
	}

	return common.MoviesAvgRatingMsg{
		Header:        batch.Header,
		MoviesRatings: moviesRatingsList,
	}, nil
}

func (r *Reducer) Stop() error {
	return r.middleware.Close()
}
