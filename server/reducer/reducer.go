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

// type queuesNames struct {
// 	previousQueue string
// 	nextQueue     string
// }

// var queriesQueues = map[int]queuesNames{
// 	2: {previousQueue: "q2-to-reduce", nextQueue: "q2-to-final-reduce"},
// 	3: {previousQueue: "q3-to-reduce", nextQueue: "q3-to-final-reduce"},
// 	4: {previousQueue: "q4-to-reduce", nextQueue: "q4-to-final-reduce"},
// 	5: {previousQueue: "q5-to-reduce", nextQueue: "q5-to-final-reduce"},
// }

const (
	rabbitHost      = "rabbitmq"
	previousQueueQ2 = "q2-to-reduce"
	nextQueueQ2     = "q2-to-final-reduce"
	previousQueueQ3 = "q3-to-reduce"
	nextQueueQ3     = "q3-to-final-reduce"
	previousQueueQ4 = "q4-to-reduce"
	nextQueueQ4     = "q4-to-final-reduce"
	previousQueueQ5 = "q5-to-reduce"
	nextQueueQ5     = "q5-to-final-reduce"
)

type Reducer struct {
	middleware       *common.Middleware
	query2Connection connection
	query3Connection connection
	query4Connection connection
	query5Connection connection
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewReducer(rabbitUser, rabbitPass string) (*Reducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	query2Connection, err := initializeConnection(middleware, previousQueueQ2, nextQueueQ2)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 2 connection: %w", err)
	}

	query3Connection, err := initializeConnection(middleware, previousQueueQ3, nextQueueQ3)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 3 connection: %w", err)
	}

	query4Connection, err := initializeConnection(middleware, previousQueueQ4, nextQueueQ4)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 4 connection: %w", err)
	}

	query5Connection, err := initializeConnection(middleware, previousQueueQ5, nextQueueQ5)
	if err != nil {
		return nil, fmt.Errorf("error initializing query 5 connection: %w", err)
	}

	return &Reducer{
		middleware:       middleware,
		query2Connection: query2Connection,
		query3Connection: query3Connection,
		query4Connection: query4Connection,
		query5Connection: query5Connection,
	}, nil
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
		case msg := <-r.query2Connection.ChanToRecv:
			reduced, err := reduceMessage(msg, r.reduceQ2)
			if err != nil {
				slog.Error("error processing query2 message", slog.String("error", err.Error()))
			} else {
				if err := sendResponse(reduced, r.query2Connection.ChanToSend); err != nil {
					slog.Error("error sending response", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query2 message", slog.String("error", err.Error()))
			}
		case msg := <-r.query3Connection.ChanToRecv:
			reduced, err := reduceMessage(msg, r.reduceQ3)
			if err != nil {
				slog.Error("error processing query3 message", slog.String("error", err.Error()))
			} else {
				if err := sendResponse(reduced, r.query3Connection.ChanToSend); err != nil {
					slog.Error("error sending response", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query3 message", slog.String("error", err.Error()))
			}
		case msg := <-r.query4Connection.ChanToRecv:
			reduced, err := reduceMessage(msg, r.reduceQ4)
			if err != nil {
				slog.Error("error processing query4 message", slog.String("error", err.Error()))
			} else {
				if err := sendResponse(reduced, r.query4Connection.ChanToSend); err != nil {
					slog.Error("error sending response", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query4 message", slog.String("error", err.Error()))
			}
		case msg := <-r.query5Connection.ChanToRecv:
			reduced, err := reduceMessage(msg, r.reduceQ5)
			if err != nil {
				slog.Error("error processing query5 message", slog.String("error", err.Error()))
			} else {
				if err := sendResponse(reduced, r.query5Connection.ChanToSend); err != nil {
					slog.Error("error sending response", slog.String("error", err.Error()))
				}
			}
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging query5 message", slog.String("error", err.Error()))
			}
		}
	}
}

func reduceMessage[T any, R any](msg common.Message, reduceFunc func(common.Batch[T]) (R, error)) (R, error) {
	var batch common.Batch[T]
	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		var zero R
		return zero, fmt.Errorf("error unmarshalling message: %w", err)
	}

	reduced, err := reduceFunc(batch)
	if err != nil {
		var zero R
		return zero, fmt.Errorf("error reducing message: %w", err)
	}

	return reduced, nil
}

func sendResponse[T any](response T, sendChan chan<- []byte) error {
	responseBytes, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("error marshalling response: %w", err)
	}
	sendChan <- responseBytes
	return nil
}

func (r *Reducer) reduceQ2(batch common.Batch[common.Movie]) (common.Batch[common.CountryBudget], error) {
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

func (r *Reducer) reduceQ5(batch common.Batch[common.MovieWithSentiment]) (common.Batch[common.SentimentProfitRatioAccumulator], error) {
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
