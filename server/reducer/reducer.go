package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	pkg "pkg/models"
	"syscall"
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
	defer r.close()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	r.startReceiving(ctx)
}

func (r *Reducer) startReceiving(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping")
			return
		case msg := <-r.connections[2].ChanToRecv:
			if err := r.processQuery2Message(msg); err != nil {
				slog.Error("error processing query2 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query2 message", slog.String("error", err.Error()))
			}
		case msg := <-r.connections[3].ChanToRecv:
			if err := r.processQuery3Message(msg); err != nil {
				slog.Error("error processing query3 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query3 message", slog.String("error", err.Error()))
			}
		case msg := <-r.connections[4].ChanToRecv:
			if err := r.processQuery4Message(msg); err != nil {
				slog.Error("error processing query4 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query4 message", slog.String("error", err.Error()))
			}
		case msg := <-r.connections[5].ChanToRecv:
			if err := r.processQuery5Message(msg); err != nil {
				slog.Error("error processing query5 message", slog.String("error", err.Error()))
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query5 message", slog.String("error", err.Error()))
			}
		}
	}
}

// TODO: Futuro refactor: poner esta func y sacar las 5 de processQueryXMessage (no lo hago porque no puedo testear que funcione el codigo)

// func processMessage[T any](msg common.Message, reduceFunc func(common.Batch[T]) (any, error), sendChan chan<- []byte) error {
// 	var batch common.Batch[T]
// 	if err := json.Unmarshal(msg.Body, &batch); err != nil {
// 		return fmt.Errorf("error unmarshalling message: %w", err)
// 	}

// 	reduced, err := reduceFunc(batch)
// 	if err != nil {
// 		return fmt.Errorf("error reducing message: %w", err)
// 	}

// 	response, err := json.Marshal(reduced)
// 	if err != nil {
// 		return fmt.Errorf("error marshalling response: %w", err)
// 	}

// 	sendChan <- response
// 	return nil
// }

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
	if reduced.IsEof() {
		slog.Info("EOF received for q3 in reducer")
	}
	r.connections[3].ChanToSend <- response
	return nil
}

func (r *Reducer) processQuery4Message(msg common.Message) error {
	var batch common.Batch[common.Credit]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 4 message: %w", err)
	}

	reduced, err := r.reduceQ4(batch)
	if err != nil {
		return fmt.Errorf("error processing query4 message: %w", err)
	}
	response, err := json.Marshal(reduced)
	if err != nil {
		return fmt.Errorf("error marshalling response: %w", err)
	}

	r.connections[4].ChanToSend <- response
	return nil
}

func (r *Reducer) processQuery5Message(msg common.Message) error {
	var batch common.Batch[common.MovieWithSentimentOverview]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		return fmt.Errorf("error unmarshalling query 5 message: %w", err)
	}

	reduced, err := r.reduceQ5(batch)
	if err != nil {
		return fmt.Errorf("error processing query5 message: %w", err)
	}
	response, err := json.Marshal(reduced)
	if err != nil {
		return fmt.Errorf("error marshalling response: %w", err)
	}

	r.connections[5].ChanToSend <- response
	return nil
}

func (r *Reducer) reduceQ2(batch common.Batch[common.Movie]) (common.Batch[common.CountryBudget], error) {
	// me llega un mensaje de peliculas y tengo que reducirlo a un map con cada entrada (pais, $$), me viene filtrado
	countries := make(map[pkg.Country]uint64)
	for _, movie := range batch.Data {
		if len(movie.ProductionCountries) > 1 {
			return common.Batch[common.CountryBudget]{}, fmt.Errorf("movie has more than 1 production country for query 2, movie: %v", movie)
		}
		countries[movie.ProductionCountries[0]] += movie.Budget
	}

	countriesList := make([]common.CountryBudget, 0, len(countries))
	for country, budget := range countries {
		countriesList = append(countriesList, common.CountryBudget{Country: country, Budget: budget})
	}

	return common.Batch[common.CountryBudget]{
		Header: batch.Header,
		Data:   countriesList,
	}, nil
}

func (r *Reducer) reduceQ3(batch common.Batch[common.MovieReview]) (common.Batch[common.MovieAvgRating], error) {
	// me llega un mensaje de peliculasXReviews y tengo que reducirlo a un map con cada entrada (peli, sum(ratings), cant_reviews), me viene filtrado
	movieRatings := make(map[string]common.MovieAvgRating)
	for _, movieRating := range batch.Data {
		if currentRating, ok := movieRatings[movieRating.MovieID]; !ok {
			movieRatings[movieRating.MovieID] = common.MovieAvgRating{
				MovieID:     movieRating.MovieID,
				Title:       movieRating.Title,
				RatingSum:   movieRating.Rating,
				RatingCount: 1,
			}
		} else {
			currentRating.RatingSum += movieRating.Rating
			currentRating.RatingCount += 1
			movieRatings[movieRating.MovieID] = currentRating
		}
	}

	moviesRatingsList := make([]common.MovieAvgRating, 0, len(movieRatings))
	for _, movieRating := range movieRatings {
		moviesRatingsList = append(moviesRatingsList, movieRating)
	}

	return common.Batch[common.MovieAvgRating]{
		Header: batch.Header,
		Data:   moviesRatingsList,
	}, nil
}

func (r *Reducer) reduceQ4(batch common.Batch[common.Credit]) (common.Batch[common.ActorMoviesAmount], error) {
	// me llega un mensaje de Credit y tengo que reducirlo a un map con cada entrada (actor, sum(pelis)), me viene filtrado
	actorMovies := make(map[string]common.ActorMoviesAmount)
	for _, credit := range batch.Data {
		for _, actor := range credit.Actors {
			if currentMoviesAmount, ok := actorMovies[actor.ActorID]; !ok {
				actorMovies[actor.ActorID] = common.ActorMoviesAmount{
					ActorID:      actor.ActorID,
					ActorName:    actor.Name,
					MoviesAmount: 1,
				}
			} else {
				currentMoviesAmount.MoviesAmount += 1
				actorMovies[actor.ActorID] = currentMoviesAmount
			}
		}
	}

	actorMoviesList := make([]common.ActorMoviesAmount, 0, len(actorMovies))
	for _, actorMoviesAmount := range actorMovies {
		actorMoviesList = append(actorMoviesList, actorMoviesAmount)
	}

	return common.Batch[common.ActorMoviesAmount]{
		Header: batch.Header,
		Data:   actorMoviesList,
	}, nil
}

func (r *Reducer) reduceQ5(batch common.Batch[common.MovieWithSentimentOverview]) (common.Batch[common.SentimentProfitRatioAccumulator], error) {
	// me llega un mensaje de MovieWithSentimentOverview y tengo que reducirlo a un AvgMovieProfitRatioBySentiment, me viene filtrado
	positiveProfitRatio := common.ProfitRatioAccumulator{}
	negativeProfitRatio := common.ProfitRatioAccumulator{}
	for _, movieWithSentiment := range batch.Data {
		if movieWithSentiment.Revenue == 0 || movieWithSentiment.Budget == 0 {
			continue
		}

		profitRatio := float64(movieWithSentiment.Revenue) / float64(movieWithSentiment.Budget)
		if movieWithSentiment.Sentiment == common.Positive {
			positiveProfitRatio.ProfitRatioSum += profitRatio
			positiveProfitRatio.ProfitRatioCount++
		} else {
			negativeProfitRatio.ProfitRatioSum += profitRatio
			negativeProfitRatio.ProfitRatioCount++
		}

		if profitRatio > 10000 {
			slog.Debug("ALOT profit ratio", slog.String("movie_id", movieWithSentiment.ID), slog.Any("revenue", movieWithSentiment.Revenue), slog.Any("budget", movieWithSentiment.Budget), slog.Float64("profit_ratio", profitRatio))
		}

	}

	return common.Batch[common.SentimentProfitRatioAccumulator]{
		Header: batch.Header,
		Data: []common.SentimentProfitRatioAccumulator{
			{
				PositiveProfitRatio: positiveProfitRatio,
				NegativeProfitRatio: negativeProfitRatio,
			},
		},
	}, nil
}

func (r *Reducer) close() {
	if err := r.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("reducer stopped")
}
