package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"sort"
	"syscall"
	"tp-sistemas-distribuidos/server/common"

	pkg "pkg/models"
)

type queuesNames struct {
	previousQueue string
	nextQueue     string
}

var queriesQueues = map[int]queuesNames{
	2: {previousQueue: "q2-to-final-reduce", nextQueue: "q2-results"},
	3: {previousQueue: "q3-to-final-reduce", nextQueue: "q3-results"},
	4: {previousQueue: "q4-to-final-reduce", nextQueue: "q4-results"},
	5: {previousQueue: "q5-to-final-reduce", nextQueue: "q5-results"},
}

type FinalReducer struct {
	middleware  *common.Middleware
	connection  connection
	queryNum    int
	amtOfShards int
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewFinalReducer(queryNum int, rabbitUser, rabbitPass string, amtOfShards int) (*FinalReducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connection, err := initializeConnectionForQuery(queryNum, middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for query %d: %w", queryNum, err)
	}

	return &FinalReducer{
		middleware:  middleware,
		connection:  connection,
		queryNum:    queryNum,
		amtOfShards: amtOfShards,
	}, nil
}

func initializeConnectionForQuery(queryNum int, middleware *common.Middleware) (connection, error) {
	queuesNames := queriesQueues[queryNum]
	if queuesNames.previousQueue == "" || queuesNames.nextQueue == "" {
		return connection{}, fmt.Errorf("query number %d not found", queryNum)
	}

	previousChan, err := middleware.GetChanToRecv(queuesNames.previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", queuesNames.previousQueue, err)
	}

	nextChan, err := middleware.GetChanToSend(queuesNames.nextQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to send: %w", queuesNames.nextQueue, err)
	}

	return connection{previousChan, nextChan}, nil
}

func (r *FinalReducer) Start() {
	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if r.queryNum == 2 {
		slog.Info("starting final reducer for query 2")
		r.startReceivingQ2(ctx)
	} else if r.queryNum == 3 {
		slog.Info("starting final reducer for query 3")
		r.startReceivingQ3(ctx)
	} else if r.queryNum == 4 {
		slog.Info("starting final reducer for query 4")
		r.startReceivingQ4(ctx)
	} else if r.queryNum == 5 {
		slog.Info("starting final reducer for query 5")
		r.startReceivingQ5(ctx)
	} else {
		slog.Error("query number not found", slog.Int("query number", r.queryNum))
		return
	}

	slog.Info("final reducer finalized")
}

func (r *FinalReducer) startReceivingQ2(ctx context.Context) {
	countries := make(map[pkg.Country]uint64)
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 2")
			return
		case msg := <-r.connection.ChanToRecv:
			var batch common.Batch[common.CountryBudget]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			// TODO: de aca para abajo se podria cambiar por una func y que el resto del codigo sea para todas las querys
			for _, countryBudget := range batch.Data {
				countries[countryBudget.Country] += countryBudget.Budget
			}

			currentWeight += batch.Header.Weight

			if batch.IsEof() {
				eofWeight = int32(batch.Header.TotalWeight)
			}

			if int32(currentWeight) == eofWeight && eofWeight != 0 {
				top5Countries := calculateTop5Countries(countries)
				response, err := json.Marshal(top5Countries)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query2 final response")
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (r *FinalReducer) startReceivingQ3(ctx context.Context) {
	movies := make(map[string]common.MovieAvgRating)
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 3")
			return
		case msg := <-r.connection.ChanToRecv:
			var batch common.Batch[common.MovieAvgRating]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			for _, movieRating := range batch.Data {
				if currentRating, ok := movies[movieRating.MovieID]; !ok {
					movies[movieRating.MovieID] = movieRating
				} else {
					currentRating.RatingSum += movieRating.RatingSum
					currentRating.RatingCount += movieRating.RatingCount
					movies[movieRating.MovieID] = currentRating
				}
			}

			currentWeight += batch.Header.Weight

			if batch.IsEof() {
				if eofWeight != 0 {
					if err := msg.Ack(); err != nil {
						return
					}
					continue
				}
				eofWeight = batch.Header.TotalWeight * int32(r.amtOfShards)
			}
			if int32(currentWeight) > eofWeight && eofWeight != 0 {
				slog.Error("current weight is greater than eof weight", slog.Any("current weight", int32(currentWeight)), slog.Any("eof weight", eofWeight))
			}

			if int32(currentWeight) == eofWeight {
				bestAndWorstMovies := calculateBestAndWorstMovie(movies)
				response, err := json.Marshal(bestAndWorstMovies)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query3 final response", slog.String("best movie id", bestAndWorstMovies.BestMovie.ID), slog.String("worst movie id", bestAndWorstMovies.WorstMovie.ID))

			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (r *FinalReducer) startReceivingQ4(ctx context.Context) {
	actorMovies := make(map[string]common.ActorMoviesAmount)
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 4")
			return
		case msg := <-r.connection.ChanToRecv:
			var batch common.Batch[common.ActorMoviesAmount]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			// TODO: de aca para abajo se podria cambiar por una func y que el resto del codigo sea para todas las querys
			for _, actorMoviesAmount := range batch.Data {
				if currentMoviesAmount, ok := actorMovies[actorMoviesAmount.ActorID]; !ok {
					actorMovies[actorMoviesAmount.ActorID] = actorMoviesAmount
				} else {
					currentMoviesAmount.MoviesAmount += actorMoviesAmount.MoviesAmount
					actorMovies[actorMoviesAmount.ActorID] = currentMoviesAmount
				}
			}

			currentWeight += batch.Header.Weight

			if batch.IsEof() {
				if eofWeight != 0 {
					if err := msg.Ack(); err != nil {
						return
					}
					continue
				}
				eofWeight = batch.Header.TotalWeight * int32(r.amtOfShards)
			}

			if int32(currentWeight) == eofWeight && eofWeight != 0 {
				top10Actors := calculateTop10Actors(actorMovies)
				response, err := json.Marshal(top10Actors)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query4 final response", slog.Any("top10 actors", top10Actors))
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func (r *FinalReducer) startReceivingQ5(ctx context.Context) {
	sentimentProfitRatios := common.SentimentProfitRatioAccumulator{}
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 5")
			return
		case msg := <-r.connection.ChanToRecv:
			var batch common.Batch[common.SentimentProfitRatioAccumulator]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			// TODO: de aca para abajo se podria cambiar por una func y que el resto del codigo sea para todas las querys
			for _, sentimentProfitRatio := range batch.Data {
				sentimentProfitRatios.PositiveProfitRatio.ProfitRatioSum += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum
				sentimentProfitRatios.PositiveProfitRatio.ProfitRatioCount += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount
				sentimentProfitRatios.NegativeProfitRatio.ProfitRatioSum += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioSum
				sentimentProfitRatios.NegativeProfitRatio.ProfitRatioCount += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioCount

				if sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum > 10000 {
					slog.Debug("ALOT positive sentiment profit ratio", slog.Any("count", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount), slog.Any("sum", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum))
				}
			}

			currentWeight += batch.Header.Weight
			slog.Info("current weight", slog.Any("current weight", currentWeight), slog.Any("eof weight", eofWeight))
			if batch.IsEof() {
				eofWeight = int32(batch.Header.TotalWeight)
				slog.Info("eof weight", slog.Any("eof weight", eofWeight))
			}

			if int32(currentWeight) == eofWeight && eofWeight != 0 {
				sentimentProfitRatioAverage := calculateSentimentProfitRatioAverage(sentimentProfitRatios)
				response, err := json.Marshal(sentimentProfitRatioAverage)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query5 final response", slog.Float64("positive avg profit ratio", sentimentProfitRatioAverage.PositiveAvgProfitRatio), slog.Float64("negative avg profit ratio", sentimentProfitRatioAverage.NegativeAvgProfitRatio))
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}
}

func calculateTop5Countries(countries map[pkg.Country]uint64) common.Top5Countries {
	if len(countries) == 0 {
		slog.Warn("countries count is 0, returning empty top 5 countries")
		return common.Top5Countries{}
	}

	counts := make([]common.CountryBudget, 0, len(countries))
	for country, budget := range countries {
		counts = append(counts, common.CountryBudget{Country: country, Budget: budget})
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].Budget > counts[j].Budget
	})

	if len(counts) < 5 {
		slog.Warn("countries count is less than 5, repeating last country")
		for len(counts) < 5 {
			last := counts[len(counts)-1]
			counts = append(counts, last)
		}
	}

	return common.Top5Countries{Countries: counts[:5]}
}

func calculateBestAndWorstMovie(movies map[string]common.MovieAvgRating) common.BestAndWorstMovies {
	bestMovie := ""
	worstMovie := ""
	bestRatingAvg := 0.0
	worstRatingAvg := 0.0
	for movie, rating := range movies {
		ratingAvg := float64(rating.RatingSum) / float64(rating.RatingCount)
		if ratingAvg > bestRatingAvg {
			bestMovie = movie
			bestRatingAvg = ratingAvg
		}
		if ratingAvg < worstRatingAvg || worstRatingAvg == 0.0 {
			worstMovie = movie
			worstRatingAvg = ratingAvg
		}
	}

	if bestMovie == "" || worstMovie == "" {
		slog.Warn("best or worst movie is empty")
	}

	bestMovieWithTitle := common.MovieWithTitle{ID: bestMovie, Title: movies[bestMovie].Title}
	worstMovieWithTitle := common.MovieWithTitle{ID: worstMovie, Title: movies[worstMovie].Title}
	return common.BestAndWorstMovies{BestMovie: bestMovieWithTitle, WorstMovie: worstMovieWithTitle}
}

func calculateTop10Actors(actors map[string]common.ActorMoviesAmount) common.Top10Actors {
	if len(actors) == 0 {
		slog.Warn("actors count is 0, returning empty top 10 actors")
		return common.Top10Actors{}
	}

	actorsSlice := make([]common.ActorMoviesAmount, 0, len(actors))
	for _, actor := range actors {
		actorsSlice = append(actorsSlice, actor)
	}

	sort.Slice(actorsSlice, func(i, j int) bool {
		return actorsSlice[i].MoviesAmount > actorsSlice[j].MoviesAmount
	})

	if len(actorsSlice) < 10 {
		slog.Warn("actors count is less than 10, repeating last actor")
		for len(actorsSlice) < 10 {
			last := actorsSlice[len(actorsSlice)-1]
			actorsSlice = append(actorsSlice, last)
		}
	}
	return common.Top10Actors{TopActors: actorsSlice[:10]}
}

func calculateSentimentProfitRatioAverage(sentimentProfitRatios common.SentimentProfitRatioAccumulator) common.SentimentProfitRatioAverage {
	positiveAvg := -1.0
	if sentimentProfitRatios.PositiveProfitRatio.ProfitRatioCount > 0 {
		positiveAvg = sentimentProfitRatios.PositiveProfitRatio.ProfitRatioSum / float64(sentimentProfitRatios.PositiveProfitRatio.ProfitRatioCount)
	} else {
		slog.Warn("positive profit ratio count is 0, returning -1")
	}

	negativeAvg := -1.0
	if sentimentProfitRatios.NegativeProfitRatio.ProfitRatioCount > 0 {
		negativeAvg = sentimentProfitRatios.NegativeProfitRatio.ProfitRatioSum / float64(sentimentProfitRatios.NegativeProfitRatio.ProfitRatioCount)
	} else {
		slog.Warn("negative profit ratio count is 0, returning -1")
	}

	return common.SentimentProfitRatioAverage{
		PositiveAvgProfitRatio: positiveAvg,
		NegativeAvgProfitRatio: negativeAvg,
	}
}

func (r *FinalReducer) Stop() error {
	return nil
}
