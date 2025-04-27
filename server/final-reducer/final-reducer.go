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
	middleware   *common.Middleware
	connection   connection
	queryNum     int
	joinerShards int
	sessions     map[string]*ClientSession
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
<<<<<<< HEAD
		middleware:   middleware,
		connection:   connection,
		queryNum:     queryNum,
		joinerShards: amtOfShards,
		sessions:     make(map[string]*ClientSession),
=======
		middleware:  middleware,
		connection:  connection,
		queryNum:    queryNum,
		joinerShards: amtOfShards,
		sessions:    make(map[string]*ClientSession),
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
	}, nil
}

func initializeConnectionForQuery(queryNum int, middleware *common.Middleware) (connection, error) {
	queuesNames, ok := queriesQueues[queryNum]
	if !ok {
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
	defer r.stop()

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
}

<<<<<<< HEAD
func startReceiving[T any](ctx context.Context, chanToRecv <-chan common.Message, sessions map[string]*ClientSession, finishAndSendBatch func(clientId string), processBatch func(batch common.Batch[T])) error {
=======
func (r *FinalReducer) startReceivingQ2(ctx context.Context) {
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-chanToRecv:
			var batch common.Batch[T]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

<<<<<<< HEAD
			if batch.Header.GetClientID() == "" {
				slog.Warn("client id is empty, using 1")
				batch.Header.ClientID = "1"
			}

			processBatch(batch)

			clientID := batch.GetClientID()
			sessions[clientID].AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				slog.Info("setting eof weight", slog.String("client id", clientID), slog.Any("eof weight", int32(batch.Header.TotalWeight)))
				sessions[clientID].SetEofWeight(int32(batch.Header.TotalWeight))
=======
			clientID := batch.Header.GetClientID()
			if _, ok := r.sessions[clientID]; !ok {
				r.sessions[clientID] = NewClientSession(clientID, 1)
				r.sessions[clientID].SetData(make(map[pkg.Country]uint64))
			}
			countries := r.sessions[clientID].GetData().(map[pkg.Country]uint64)

			// TODO: de aca para abajo se podria cambiar por una func y que el resto del codigo sea para todas las querys
			for _, countryBudget := range batch.Data {
				countries[countryBudget.Country] += countryBudget.Budget
			}

			r.sessions[clientID].SetData(countries)

			r.sessions[clientID].AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				r.sessions[clientID].SetEofWeight(int32(batch.Header.TotalWeight))
			}

			if r.sessions[clientID].IsFinished() {
				top5Countries := calculateTop5Countries(countries)
				response, err := json.Marshal(top5Countries)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query2 final response")
				delete(r.sessions, clientID)
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}

			if sessions[clientID].IsFinished() {
				slog.Info("finishing and sending batch", slog.String("client id", clientID), slog.String("message weight", fmt.Sprintf("%d", batch.Header.Weight)))
				finishAndSendBatch(clientID)
			}
		}
	}
}

func (r *FinalReducer) startReceivingQ2(ctx context.Context) {
	err := startReceiving(ctx, r.connection.ChanToRecv, r.sessions, r.finishAndSendBatchForQuery2, func(batch common.Batch[common.CountryBudget]) {
		clientID := batch.Header.GetClientID()
		if _, ok := r.sessions[clientID]; !ok {
			r.sessions[clientID] = NewClientSession(clientID, 1)
			r.sessions[clientID].SetData(make(map[pkg.Country]uint64))
		}

		countries, ok := r.sessions[clientID].GetData().(map[pkg.Country]uint64)
		if !ok {
			slog.Error("error getting data", slog.String("error", "data is not of required type: map[pkg.Country]uint64"))
			return
		}

		for _, countryBudget := range batch.Data {
			countries[countryBudget.Country] += countryBudget.Budget
		}
	})

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ3(ctx context.Context) {
<<<<<<< HEAD
	err := startReceiving(ctx, r.connection.ChanToRecv, r.sessions, r.finishAndSendBatchForQuery3, func(batch common.Batch[common.MovieAvgRating]) {
		clientID := batch.Header.GetClientID()
		if _, ok := r.sessions[clientID]; !ok {
			r.sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
			r.sessions[clientID].SetData(make(map[string]common.MovieAvgRating))
		}

		movies, ok := r.sessions[clientID].GetData().(map[string]common.MovieAvgRating)
		if !ok {
			slog.Error("error getting data", slog.String("error", "data is not of required type: map[string]common.MovieAvgRating"))
=======
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 3")
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			return
		}

<<<<<<< HEAD
		for _, movieRating := range batch.Data {
			if currentRating, ok := movies[movieRating.MovieID]; !ok {
				movies[movieRating.MovieID] = movieRating
			} else {
				currentRating.RatingSum += movieRating.RatingSum
				currentRating.RatingCount += movieRating.RatingCount
				movies[movieRating.MovieID] = currentRating
=======
			clientID := batch.Header.GetClientID()
			if _, ok := r.sessions[clientID]; !ok {
				r.sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
				r.sessions[clientID].SetData(make(map[string]common.MovieAvgRating))
			}
			movies := r.sessions[clientID].GetData().(map[string]common.MovieAvgRating)

			for _, movieRating := range batch.Data {
				if currentRating, ok := movies[movieRating.MovieID]; !ok {
					movies[movieRating.MovieID] = movieRating
				} else {
					currentRating.RatingSum += movieRating.RatingSum
					currentRating.RatingCount += movieRating.RatingCount
					movies[movieRating.MovieID] = currentRating
				}
			}

			r.sessions[clientID].SetData(movies)

			r.sessions[clientID].AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				if r.sessions[clientID].GetEofWeight() != 0 {
					if err := msg.Ack(); err != nil {
						return
					}
					continue
				}
				r.sessions[clientID].SetEofWeight(int32(batch.Header.TotalWeight))
			}

			if r.sessions[clientID].IsFinished() {
				bestAndWorstMovies := calculateBestAndWorstMovie(movies)
				response, err := json.Marshal(bestAndWorstMovies)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query3 final response", slog.String("best movie id", bestAndWorstMovies.BestMovie.MovieID), slog.String("worst movie id", bestAndWorstMovies.WorstMovie.MovieID))
				delete(r.sessions, clientID)
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			}
		}
	})

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ4(ctx context.Context) {
<<<<<<< HEAD
	err := startReceiving(ctx, r.connection.ChanToRecv, r.sessions, r.finishAndSendBatchForQuery4, func(batch common.Batch[common.ActorMoviesAmount]) {
		clientID := batch.Header.GetClientID()
		if _, ok := r.sessions[clientID]; !ok {
			r.sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
			r.sessions[clientID].SetData(make(map[string]common.ActorMoviesAmount))
		}

		actorMovies, ok := r.sessions[clientID].GetData().(map[string]common.ActorMoviesAmount)
		if !ok {
			slog.Error("error getting data", slog.String("error", "data is not of required type: map[string]common.ActorMoviesAmount"))
			return
		}

		for _, actorMoviesAmount := range batch.Data {
			if currentMoviesAmount, ok := actorMovies[actorMoviesAmount.ActorID]; !ok {
				actorMovies[actorMoviesAmount.ActorID] = actorMoviesAmount
			} else {
				currentMoviesAmount.MoviesAmount += actorMoviesAmount.MoviesAmount
				actorMovies[actorMoviesAmount.ActorID] = currentMoviesAmount
=======
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 4")
			return
		case msg := <-r.connection.ChanToRecv:
			var batch common.Batch[common.ActorMoviesAmount]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			clientID := batch.Header.GetClientID()
			if _, ok := r.sessions[clientID]; !ok {
				r.sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
				r.sessions[clientID].SetData(make(map[string]common.ActorMoviesAmount))
			}
			actorMovies := r.sessions[clientID].GetData().(map[string]common.ActorMoviesAmount)

			// TODO: de aca para abajo se podria cambiar por una func y que el resto del codigo sea para todas las querys
			for _, actorMoviesAmount := range batch.Data {
				if currentMoviesAmount, ok := actorMovies[actorMoviesAmount.ActorID]; !ok {
					actorMovies[actorMoviesAmount.ActorID] = actorMoviesAmount
				} else {
					currentMoviesAmount.MoviesAmount += actorMoviesAmount.MoviesAmount
					actorMovies[actorMoviesAmount.ActorID] = currentMoviesAmount
				}
			}

			r.sessions[clientID].SetData(actorMovies)

			r.sessions[clientID].AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				if r.sessions[clientID].GetEofWeight() != 0 {
					if err := msg.Ack(); err != nil {
						return
					}
					continue
				}
				r.sessions[clientID].SetEofWeight(int32(batch.Header.TotalWeight))
			}

			if r.sessions[clientID].IsFinished() {
				top10Actors := calculateTop10Actors(actorMovies)
				response, err := json.Marshal(top10Actors)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query4 final response", slog.Any("top10 actors", top10Actors))
				delete(r.sessions, clientID)
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			}
		}
	})

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ5(ctx context.Context) {
<<<<<<< HEAD
	//TODO: add sessions here instead of in the struct and use generics
	err := startReceiving(ctx, r.connection.ChanToRecv, r.sessions, r.finishAndSendBatchForQuery5, func(batch common.Batch[common.SentimentProfitRatioAccumulator]) {
		clientID := batch.Header.GetClientID()
		if _, ok := r.sessions[clientID]; !ok {
			r.sessions[clientID] = NewClientSession(clientID, 1)
			r.sessions[clientID].SetData(common.SentimentProfitRatioAccumulator{})
		}

		sentimentProfitRatios, ok := r.sessions[clientID].GetData().(common.SentimentProfitRatioAccumulator)
		if !ok {
			slog.Error("error getting data", slog.String("error", "data is not of required type: common.SentimentProfitRatioAccumulator"))
=======
	for {
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping final reducer for query 5")
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			return
		}

<<<<<<< HEAD
		for _, sentimentProfitRatio := range batch.Data {
			sentimentProfitRatios.PositiveProfitRatio.ProfitRatioSum += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum
			sentimentProfitRatios.PositiveProfitRatio.ProfitRatioCount += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount
			sentimentProfitRatios.NegativeProfitRatio.ProfitRatioSum += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioSum
			sentimentProfitRatios.NegativeProfitRatio.ProfitRatioCount += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioCount

			if sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum > 10000 {
				slog.Debug("ALOT positive sentiment profit ratio", slog.Any("count", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount), slog.Any("sum", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum))
=======
			clientID := batch.Header.GetClientID()
			if _, ok := r.sessions[clientID]; !ok {
				r.sessions[clientID] = NewClientSession(clientID, 1)
				r.sessions[clientID].SetData(common.SentimentProfitRatioAccumulator{})
			}
			sentimentProfitRatios := r.sessions[clientID].GetData().(common.SentimentProfitRatioAccumulator)

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

			r.sessions[clientID].SetData(sentimentProfitRatios)

			r.sessions[clientID].AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				r.sessions[clientID].SetEofWeight(int32(batch.Header.TotalWeight))
			}

			if r.sessions[clientID].IsFinished() {
				sentimentProfitRatioAverage := calculateSentimentProfitRatioAverage(sentimentProfitRatios)
				response, err := json.Marshal(sentimentProfitRatioAverage)
				if err != nil {
					slog.Error("error marshalling response", slog.String("error", err.Error()))
				}
				r.connection.ChanToSend <- response
				slog.Info("sent query5 final response", slog.Float64("positive avg profit ratio", sentimentProfitRatioAverage.PositiveAvgProfitRatio), slog.Float64("negative avg profit ratio", sentimentProfitRatioAverage.NegativeAvgProfitRatio))
				delete(r.sessions, clientID)
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
>>>>>>> 3d023c0 (feat: clients sessions in final reducers)
			}
		}
	})

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) finishAndSendBatchForQuery2(clientId string) {
	slog.Info("finishing and sending batch for query 2", slog.String("client id", clientId))
	countries := r.sessions[clientId].GetData().(map[pkg.Country]uint64)
	top5Countries := calculateTop5Countries(countries)
	response, err := json.Marshal(top5Countries)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query2 final response")
	delete(r.sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery3(clientId string) {
	slog.Info("finishing and sending batch for query 3", slog.String("client id", clientId))
	movies := r.sessions[clientId].GetData().(map[string]common.MovieAvgRating)
	bestAndWorstMovies := calculateBestAndWorstMovie(movies)
	response, err := json.Marshal(bestAndWorstMovies)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query3 final response", slog.String("best movie id", bestAndWorstMovies.BestMovie.MovieID), slog.String("worst movie id", bestAndWorstMovies.WorstMovie.MovieID))
	delete(r.sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery4(clientId string) {
	slog.Info("finishing and sending batch for query 4", slog.String("client id", clientId))
	actorMovies := r.sessions[clientId].GetData().(map[string]common.ActorMoviesAmount)
	top10Actors := calculateTop10Actors(actorMovies)
	response, err := json.Marshal(top10Actors)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query4 final response", slog.Any("top10 actors", top10Actors))
	delete(r.sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery5(clientId string) {
	slog.Info("finishing and sending batch for query 5", slog.String("client id", clientId))
	sentimentProfitRatios := r.sessions[clientId].GetData().(common.SentimentProfitRatioAccumulator)
	sentimentProfitRatioAverage := calculateSentimentProfitRatioAverage(sentimentProfitRatios)
	response, err := json.Marshal(sentimentProfitRatioAverage)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query5 final response", slog.Float64("positive avg profit ratio", sentimentProfitRatioAverage.PositiveAvgProfitRatio), slog.Float64("negative avg profit ratio", sentimentProfitRatioAverage.NegativeAvgProfitRatio))
	delete(r.sessions, clientId)
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
	bestRatingAvg := float64(0.0)
	worstRatingAvg := float64(0.0)
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

	bestMovieWithTitle := common.MovieReview{MovieID: bestMovie, Title: movies[bestMovie].Title, Rating: bestRatingAvg}
	worstMovieWithTitle := common.MovieReview{MovieID: worstMovie, Title: movies[worstMovie].Title, Rating: worstRatingAvg}
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

func (r *FinalReducer) stop() {
	if err := r.middleware.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("final reducer stopped")
}
