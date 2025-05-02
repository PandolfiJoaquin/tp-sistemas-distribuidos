package common

import (
	"encoding/json"
	pkg "pkg/models"
)

type ToProcessMsg struct {
	Type     string          `json:"type"`
	ClientId string          `json:"client_id"`
	Body     json.RawMessage `json:"body"`
}

// Query 2
type CountryBudget struct {
	Country pkg.Country `json:"country"`
	Budget  uint64      `json:"budget"`
}

type Top5Countries struct {
	Countries []CountryBudget `json:"countries"`
}

// Query 3
type MovieAvgRating struct {
	MovieID     string  `json:"movie_id"`
	Title       string  `json:"title"`
	RatingSum   float64 `json:"rating_sum"`
	RatingCount uint32  `json:"rating_count"`
}

type BestAndWorstMovies struct {
	BestMovie  MovieReview `json:"best_movie"`
	WorstMovie MovieReview `json:"worst_movie"`
}

// Query 4
type ActorMoviesAmount struct {
	ActorID      string `json:"actor_id"`
	ActorName    string `json:"actor_name"`
	MoviesAmount uint32 `json:"movies_amount"`
}

type Top10Actors struct {
	TopActors []ActorMoviesAmount `json:"top_actors"`
}

// Query 5
type ProfitRatioAccumulator struct {
	ProfitRatioSum   float64 `json:"profit_ratio_sum"`
	ProfitRatioCount uint32  `json:"profit_ratio_count"`
}

type SentimentProfitRatioAccumulator struct {
	PositiveProfitRatio ProfitRatioAccumulator `json:"positive_profit_ratio"`
	NegativeProfitRatio ProfitRatioAccumulator `json:"negative_profit_ratio"`
}

type SentimentProfitRatioAverage struct {
	PositiveAvgProfitRatio float64 `json:"positive_avg_profit_ratio"`
	NegativeAvgProfitRatio float64 `json:"negative_avg_profit_ratio"`
}
