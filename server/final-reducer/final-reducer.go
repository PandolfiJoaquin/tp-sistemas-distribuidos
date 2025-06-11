package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/persistency"

	pkg "pkg/models"
)

// const rabbitHost = "rabbitmq"
const rabbitHost = "127.0.0.1"
const maxTransactionsOnLog = 500
const separator = ";"

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

const ( //TODO: Optimize encoding
	AggCountriesBudgetOp      = "AGG_COUNTRIES_BUDGET"
	AggMovieRatingsOp         = "AGG_MOVIE_RATINGS"
	AggActorMoviesOp          = "AGG_ACTOR_MOVIES"
	AggSentimentProfitRatioOP = "AGG_SENTIMENT_PROFIT_RATIO"
	UpdateWeightsOp           = "UPDATE_WEIGHTS"
	FilterDuplicatesOp        = "FILTER_DUPLICATES"
)

type FinalReducer struct {
	middleware         *common.Middleware
	connection         connection
	queryNum           int
	joinerShards       int
	Sessions           map[string]*ClientSession `json:"sessions"`
	persistencyHandler *persistency.PersistencyHandler[FinalReducer]
	transactionsOnLog  int
}

func (r FinalReducer) ApplyFunc(entry persistency.TransactionEntry) (FinalReducer, error) {
	switch entry.Op {
	case AggCountriesBudgetOp:
		batch, err := common.GetBatchFromString[common.CountryBudget](entry.Args)
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error getting batch from string: %w", err)
		}
		r.aggCountriesBudget(batch)

	case AggMovieRatingsOp:
		batch, err := common.GetBatchFromString[common.MovieAvgRating](entry.Args)
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error getting batch from string: %w", err)
		}
		r.aggMovieRatings(batch)

	case AggActorMoviesOp:
		batch, err := common.GetBatchFromString[common.ActorMoviesAmount](entry.Args)
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error getting batch from string: %w", err)
		}
		r.aggActorMovies(batch)

	case AggSentimentProfitRatioOP:
		batch, err := common.GetBatchFromString[common.SentimentProfitRatioAccumulator](entry.Args)
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error getting batch from string: %w", err)
		}
		r.aggSentimentProfitRatio(batch)

	case UpdateWeightsOp:
		header, err := common.HeaderFromString(entry.Args)
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error getting header from string: %w", err)
		}
		session := r.getSession(header.ClientID, r.queryNum)
		session.AddCurrentWeight(header.Weight)
		if header.IsEof() {
			slog.Info("setting eof weight", slog.String("client id", session.SessionId), slog.Any("eof weight", header.TotalWeight))
			session.SetEofWeight(header.TotalWeight)
		}
		return r, nil

	case FilterDuplicatesOp:
		args := strings.Split(entry.Args, separator)
		clientID := args[0]
		messageID, err := strconv.Atoi(args[1])
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error converting message id to int: %w", err)
		}
		shardID, err := strconv.Atoi(args[2])
		if err != nil {
			return FinalReducer{}, fmt.Errorf("error converting message shardId to int: %w", err)
		}
		r.getSession(clientID, r.queryNum).FilterMsg(messageID, shardID)
	default:
		return FinalReducer{}, fmt.Errorf("unknown operation: %s", entry.Op)
	}
	return r, nil
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewFinalReducer(queryNum int, rabbitUser, rabbitPass string, amtOfShards int) (*FinalReducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}
	connection, err := initializeConnectionForQuery(queryNum, middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for query %d: %w", queryNum, err)
	}

	persistencyHandler, err := persistency.NewPersistencyHandler[FinalReducer]()
	if err != nil {
		return nil, fmt.Errorf("error creating persistency handler: %w", err)
	}

	finalReducer := FinalReducer{
		middleware:         middleware,
		connection:         connection,
		queryNum:           queryNum,
		joinerShards:       amtOfShards,
		Sessions:           make(map[string]*ClientSession),
		persistencyHandler: persistencyHandler,
		transactionsOnLog:  0,
	}

	fromBytes := func(data []byte) (FinalReducer, error) {
		if len(data) != 0 {
			if err = json.Unmarshal(data, &finalReducer); err != nil {
				return FinalReducer{}, fmt.Errorf("error unmarshalling persistency: %w", err)
			}
			// for _, session := range finalReducer.Sessions {
			// 	data, ok := session.Data.(map[string]common.MovieAvgRating)
			// 	if ok {
			// 		slog.Info("session data is a map[string]common.MovieAvgRating", slog.String("client id", session.SessionId), slog.Any("data", data))
			// 	}
			// 	slog.Info("session data", slog.String("client id", session.SessionId), slog.Any("data", session.Data))
			// }
		}
		return finalReducer, nil
	}

	finalReducer, err = persistencyHandler.RecoverFromLogs(fromBytes)
	if err != nil {
		return nil, fmt.Errorf("error recovering persistency: %w", err)
	}

	if err := finalReducer.saveCheckpoint(); err != nil {
		return nil, fmt.Errorf("error saving checkpoint: %w", err)
	}

	return &finalReducer, nil
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

func startReceiving[T common.Stringer](
	ctx context.Context,
	chanToRecv <-chan common.Message,
	sessions map[string]*ClientSession,
	finishAndSendBatch func(clientId string),
	processBatch func(batch common.LoggableBatch[T]) persistency.Transaction,
	freddyFazbear *FinalReducer,
) error {
	for clientID, session := range sessions {
		if session.IsFinished() {
			finishAndSendBatch(clientID)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-chanToRecv:
			var batch common.LoggableBatch[T]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			session := freddyFazbear.getSession(batch.GetClientID(), freddyFazbear.queryNum)
			if !session.FilterMsg(batch.MessageID.ID, batch.MessageID.JoinerID) {
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			transaction := processBatch(batch)
			transaction.Do(FilterDuplicatesOp, batch.ClientID+separator+strconv.Itoa(batch.MessageID.ID)+separator+strconv.Itoa(batch.MessageID.JoinerID))

			session.AddCurrentWeight(batch.Header.Weight)
			if batch.IsEof() {
				slog.Info("setting eof weight", slog.String("client id", session.SessionId), slog.Any("eof weight", batch.Header.TotalWeight))
				session.SetEofWeight(batch.Header.TotalWeight)
			}
			transaction.Do(UpdateWeightsOp, batch.Header.ToString())

			if session.IsFinished() {
				slog.Info("finishing and sending batch", slog.String("client id", session.SessionId), slog.String("message weight", fmt.Sprintf("%d", batch.Header.Weight)))
				finishAndSendBatch(session.SessionId)
			}

			freddyFazbear.save(transaction)

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
		}
	}

}

func (r *FinalReducer) getSession(clientID string, queryNum int) *ClientSession {
	slog.Info("getting session", slog.String("client id", clientID), slog.Int("query num", queryNum))
	if _, ok := r.Sessions[clientID]; ok {
		return r.Sessions[clientID]
	}
	slog.Info("session not found, creating new one", slog.String("client id", clientID), slog.Int("query num", queryNum))
	switch queryNum {
	case 2:
		r.Sessions[clientID] = NewClientSession(clientID, 1)
		r.Sessions[clientID].SetData(make(map[pkg.Country]uint64))
	case 3:
		r.Sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
		r.Sessions[clientID].SetData(make(map[string]common.MovieAvgRating))
	case 4:
		r.Sessions[clientID] = NewClientSession(clientID, uint32(r.joinerShards))
		r.Sessions[clientID].SetData(make(map[string]common.ActorMoviesAmount))
	case 5:
		r.Sessions[clientID] = NewClientSession(clientID, 1)
		r.Sessions[clientID].SetData(common.SentimentProfitRatioAccumulator{})
	default:
		slog.Error("query number not found", slog.Int("query number", r.queryNum))
		return nil
	}
	return r.Sessions[clientID]
}

func (r *FinalReducer) startReceivingQ2(ctx context.Context) {
	err := startReceiving(
		ctx,
		r.connection.ChanToRecv,
		r.Sessions,
		r.finishAndSendBatchForQuery2,
		r.aggCountriesBudget,
		r)

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ3(ctx context.Context) {
	err := startReceiving(ctx, r.connection.ChanToRecv, r.Sessions, r.finishAndSendBatchForQuery3, r.aggMovieRatings, r)

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ4(ctx context.Context) {
	err := startReceiving(ctx, r.connection.ChanToRecv, r.Sessions, r.finishAndSendBatchForQuery4, r.aggActorMovies, r)

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) startReceivingQ5(ctx context.Context) {
	//TODO: add Sessions here instead of in the struct and use generics
	err := startReceiving(ctx, r.connection.ChanToRecv, r.Sessions, r.finishAndSendBatchForQuery5, r.aggSentimentProfitRatio, r)

	if err != nil {
		slog.Error("error receiving", slog.String("error", err.Error()))
	}
}

func (r *FinalReducer) finishAndSendBatchForQuery2(clientId string) {
	slog.Info("finishing and sending batch for query 2", slog.String("client id", clientId))
	countries := r.Sessions[clientId].GetData().(map[pkg.Country]uint64)
	top5Countries := calculateTop5Countries(countries)
	top5Countries.ClientId = clientId
	response, err := json.Marshal(top5Countries)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query2 final response")
	delete(r.Sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery3(clientId string) {
	slog.Info("finishing and sending batch for query 3", slog.String("client id", clientId))
	movies := r.Sessions[clientId].GetData().(map[string]common.MovieAvgRating)
	bestAndWorstMovies := calculateBestAndWorstMovie(movies)
	bestAndWorstMovies.ClientId = clientId
	response, err := json.Marshal(bestAndWorstMovies)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query3 final response", slog.String("best movie id", bestAndWorstMovies.BestMovie.MovieID), slog.String("worst movie id", bestAndWorstMovies.WorstMovie.MovieID))
	delete(r.Sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery4(clientId string) {
	slog.Info("finishing and sending batch for query 4", slog.String("client id", clientId))
	actorMovies := r.Sessions[clientId].GetData().(map[string]common.ActorMoviesAmount)
	top10Actors := calculateTop10Actors(actorMovies)
	top10Actors.ClientId = clientId
	response, err := json.Marshal(top10Actors)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query4 final response", slog.Any("top10 actors", top10Actors))
	delete(r.Sessions, clientId)
}

func (r *FinalReducer) finishAndSendBatchForQuery5(clientId string) {
	slog.Info("finishing and sending batch for query 5", slog.String("client id", clientId))
	sentimentProfitRatios := r.Sessions[clientId].GetData().(common.SentimentProfitRatioAccumulator)
	sentimentProfitRatioAverage := calculateSentimentProfitRatioAverage(sentimentProfitRatios)
	sentimentProfitRatioAverage.ClientId = clientId
	response, err := json.Marshal(sentimentProfitRatioAverage)
	if err != nil {
		slog.Error("error marshalling response", slog.String("error", err.Error()))
	}
	r.connection.ChanToSend <- response
	slog.Info("sent query5 final response", slog.Float64("positive avg profit ratio", sentimentProfitRatioAverage.PositiveAvgProfitRatio), slog.Float64("negative avg profit ratio", sentimentProfitRatioAverage.NegativeAvgProfitRatio))
	delete(r.Sessions, clientId)
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

func (r *FinalReducer) save(transaction persistency.Transaction) error {
	if err := r.persistencyHandler.Commit(transaction); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	r.transactionsOnLog++

	if r.transactionsOnLog == maxTransactionsOnLog {
		return r.saveCheckpoint()
	}
	return nil

}

func (r *FinalReducer) saveCheckpoint() error {
	jsonData, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("error marshalling internal state: %w", err)
	}
	if err = r.persistencyHandler.SaveCheckpoint(jsonData); err != nil {
		return fmt.Errorf("error saving checkpoint: %w", err)
	}
	r.transactionsOnLog = 0
	return nil
}

func (r *FinalReducer) aggCountriesBudget(batch common.LoggableBatch[common.CountryBudget]) persistency.Transaction {
	transaction := persistency.NewTransaction()

	session := r.getSession(batch.Header.GetClientID(), r.queryNum)
	countries := session.GetData().(map[pkg.Country]uint64)

	for _, countryBudget := range batch.Data {
		countries[countryBudget.Country] += countryBudget.Budget
	}
	transaction.Do(AggCountriesBudgetOp, batch.ToString())
	return transaction
}

func (r *FinalReducer) aggMovieRatings(batch common.LoggableBatch[common.MovieAvgRating]) persistency.Transaction {
	transaction := persistency.NewTransaction()

	session := r.getSession(batch.Header.GetClientID(), r.queryNum)
	movies := session.GetData().(map[string]common.MovieAvgRating)

	for _, movieRating := range batch.Data {
		if currentRating, ok := movies[movieRating.MovieID]; !ok {
			movies[movieRating.MovieID] = movieRating
		} else {
			currentRating.RatingSum += movieRating.RatingSum
			currentRating.RatingCount += movieRating.RatingCount
			movies[movieRating.MovieID] = currentRating
		}
	}
	transaction.Do(AggMovieRatingsOp, batch.ToString())
	return transaction
}

func (r *FinalReducer) aggActorMovies(batch common.LoggableBatch[common.ActorMoviesAmount]) persistency.Transaction {
	transaction := persistency.NewTransaction()

	session := r.getSession(batch.Header.GetClientID(), r.queryNum)
	actorMovies := session.GetData().(map[string]common.ActorMoviesAmount)

	for _, actorMoviesAmount := range batch.Data {
		if currentMoviesAmount, ok := actorMovies[actorMoviesAmount.ActorID]; !ok {
			actorMovies[actorMoviesAmount.ActorID] = actorMoviesAmount
		} else {
			currentMoviesAmount.MoviesAmount += actorMoviesAmount.MoviesAmount
			actorMovies[actorMoviesAmount.ActorID] = currentMoviesAmount
		}
	}
	transaction.Do(AggActorMoviesOp, batch.ToString())
	return transaction
}

func (r *FinalReducer) aggSentimentProfitRatio(batch common.LoggableBatch[common.SentimentProfitRatioAccumulator]) persistency.Transaction {
	transaction := persistency.NewTransaction()

	session := r.getSession(batch.Header.GetClientID(), r.queryNum)
	sentimentProfitRatios := session.GetData().(common.SentimentProfitRatioAccumulator)

	for _, sentimentProfitRatio := range batch.Data {
		slog.Info("adding sentiment profit ratio", slog.Any("sentiment profit ratio", sentimentProfitRatio))
		sentimentProfitRatios.PositiveProfitRatio.ProfitRatioSum += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum
		sentimentProfitRatios.PositiveProfitRatio.ProfitRatioCount += sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount
		sentimentProfitRatios.NegativeProfitRatio.ProfitRatioSum += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioSum
		sentimentProfitRatios.NegativeProfitRatio.ProfitRatioCount += sentimentProfitRatio.NegativeProfitRatio.ProfitRatioCount

		if sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum > 10000 {
			slog.Debug("ALOT positive sentiment profit ratio", slog.Any("count", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioCount), slog.Any("sum", sentimentProfitRatio.PositiveProfitRatio.ProfitRatioSum))
		}
	}

	session.SetData(sentimentProfitRatios)
	transaction.Do(AggSentimentProfitRatioOP, batch.ToString())
	return transaction
}
