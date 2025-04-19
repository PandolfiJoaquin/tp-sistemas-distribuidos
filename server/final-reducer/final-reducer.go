package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"tp-sistemas-distribuidos/server/common"

	pkg "pkg/models"
)

var previousQueuesForQueries = map[int]string{
	2: "q2-to-final-reduce",
	3: "q3-to-final-reduce",
	4: "q4-to-final-reduce",
	5: "q5-to-final-reduce",
}

var resultQueuesForQueries = map[int]string{
	2: "q2-results",
	3: "q3-results",
	4: "q4-results",
	5: "q5-results",
}

type FinalReducer struct {
	middleware *common.Middleware
	connection connection
	queryNum   int
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewFinalReducer(queryNum int, rabbitUser, rabbitPass string) (*FinalReducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connection, err := initializeConnectionForQuery(queryNum, middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for query %d: %w", queryNum, err)
	}

	return &FinalReducer{
		middleware: middleware,
		connection: connection,
		queryNum:   queryNum,
	}, nil
}

func initializeConnectionForQuery(queryNum int, middleware *common.Middleware) (connection, error) {
	if _, ok := previousQueuesForQueries[queryNum]; !ok {
		return connection{}, fmt.Errorf("query number %d not found", queryNum)
	}
	if _, ok := resultQueuesForQueries[queryNum]; !ok {
		return connection{}, fmt.Errorf("query number %d not found", queryNum)
	}

	previousQueue := previousQueuesForQueries[queryNum]
	nextQueue := resultQueuesForQueries[queryNum]

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

func (r *FinalReducer) Start() {
	if r.queryNum == 2 {
		slog.Info("starting final reducer for query 2")
		go r.startReceivingQ2()
	} else if r.queryNum == 3 {
		slog.Info("starting final reducer for query 3")
		go r.startReceivingQ3()
	}

	forever := make(chan bool)
	<-forever
}

func (r *FinalReducer) startReceivingQ2() {
	countries := make(map[pkg.Country]uint32)
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for msg := range r.connection.ChanToRecv {
		slog.Info("received message", slog.String("message", string(msg.Body)))

		var batch common.CoutriesBudgetMsg
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		for _, countryBudget := range batch.Countries {
			countries[countryBudget.Country] += countryBudget.Budget
		}

		currentWeight += batch.Header.Weight

		if batch.IsEof() {
			eofWeight = int32(batch.Header.TotalWeight)
		}

		if int32(currentWeight) == eofWeight {
			top5Countries := calculateTop5Countries(countries)
			response, err := json.Marshal(top5Countries)
			if err != nil {
				slog.Error("error marshalling response", slog.String("error", err.Error()))
				continue //TODO: ack?
			}
			r.connection.ChanToSend <- response
			slog.Info("sent query2 final response")
		}

		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}

	}
}

func (r *FinalReducer) startReceivingQ3() {
	movies := make(map[string]common.MovieAvgRating)
	currentWeight := uint32(0)
	eofWeight := int32(0)
	for msg := range r.connection.ChanToRecv {
		slog.Info("received message", slog.String("message", string(msg.Body)))

		var batch common.MoviesAvgRatingMsg
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		for _, movieRating := range batch.MoviesRatings {
			if previousRating, ok := movies[movieRating.MovieID]; !ok {
				movies[movieRating.MovieID] = movieRating
			} else {
				movies[movieRating.MovieID] = common.MovieAvgRating{MovieID: movieRating.MovieID, RatingSum: previousRating.RatingSum + movieRating.RatingSum, RatingCount: previousRating.RatingCount + movieRating.RatingCount}
			}
		}

		currentWeight += batch.Header.Weight

		if batch.IsEof() {
			eofWeight = batch.Header.TotalWeight
		}

		if int32(currentWeight) == eofWeight {
			bestAndWorstMovies := calculateBestAndWorstMovie(movies)
			response, err := json.Marshal(bestAndWorstMovies)
			if err != nil {
				slog.Error("error marshalling response", slog.String("error", err.Error()))
				continue //TODO: ack?
			}
			r.connection.ChanToSend <- response
			slog.Info("sent query3 final response")
		}

		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}

	}
}

func calculateTop5Countries(countries map[pkg.Country]uint32) common.Top5Countries {
	if len(countries) == 0 {
		slog.Warn("countries count is 0, returning empty top 5 countries")
		return common.Top5Countries{}
	}

	type countryBudget struct {
		country pkg.Country
		budget  uint32
	}

	counts := make([]countryBudget, 0, len(countries))
	for country, budget := range countries {
		counts = append(counts, countryBudget{country: country, budget: budget})
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].budget > counts[j].budget
	})

	if len(counts) < 5 {
		slog.Warn("countries count is less than 5, repeating last country")
		for len(counts) < 5 {
			last := counts[len(counts)-1]
			counts = append(counts, last)
		}
	}

	return common.Top5Countries{
		FirstCountry:  counts[0].country,
		SecondCountry: counts[1].country,
		ThirdCountry:  counts[2].country,
		FourthCountry: counts[3].country,
		FifthCountry:  counts[4].country,
	}
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
	return common.BestAndWorstMovies{BestMovie: bestMovie, WorstMovie: worstMovie}
}

func (r *FinalReducer) Stop() error {
	return nil
}
