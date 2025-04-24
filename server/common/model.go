package common

import (
	"encoding/json"
	pkg "pkg/models"
)

type Movie struct {
	ID                  string        `json:"id"`
	Title               string        `json:"title"`
	Year                int           `json:"year"`
	Genres              []pkg.Genre   `json:"genres"`
	ProductionCountries []pkg.Country `json:"production_countries"`
	Budget              uint64        `json:"budget"`
	Revenue             uint64        `json:"revenue,omitempty"`
	Overview            string        `json:"overview"`
}

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"` //-1 if its uknown for the moment
}

type Batch[T any] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (h *Header) IsEof() bool {
	return h.TotalWeight > 0
}

type Review struct {
	ID      string  `json:"id"`
	MovieID string  `json:"movie_id"`
	Rating  float64 `json:"rating"`
}

type Credit struct {
	Actors  []Actor `json:"actors"`
	MovieId string  `json:"movie_id"`
}

type ToProcessMsg struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

type Sentiment int

const (
	Positive Sentiment = iota
	Negative
)

type MovieWithSentimentOverview struct {
	Movie
	Sentiment Sentiment `json:"sentiment"`
}

type CountryBudget struct {
	Country pkg.Country `json:"country"`
	Budget  uint64      `json:"budget"`
}

type MovieAvgRating struct {
	MovieID     string  `json:"movie_id"`
	Title       string  `json:"title"`
	RatingSum   float64 `json:"rating_sum"`
	RatingCount uint32  `json:"rating_count"`
}

type ActorMoviesAmount struct {
	ActorID      string `json:"actor_id"`
	ActorName    string `json:"actor_name"`
	MoviesAmount uint32 `json:"movies_amount"`
}

type ProfitRatioAccumulator struct {
	ProfitRatioSum   float64 `json:"profit_ratio_sum"`
	ProfitRatioCount uint32  `json:"profit_ratio_count"`
}

type SentimentProfitRatioAccumulator struct {
	PositiveProfitRatio ProfitRatioAccumulator `json:"positive_profit_ratio"`
	NegativeProfitRatio ProfitRatioAccumulator `json:"negative_profit_ratio"`
}

type Top5Countries struct {
	Countries []CountryBudget `json:"countries"`
}

type MovieWithTitle struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Rating float32 `json:"rating"`
}

type BestAndWorstMovies struct {
	BestMovie  MovieWithTitle `json:"best_movie"`
	WorstMovie MovieWithTitle `json:"worst_movie"`
}

type Top10Actors struct {
	TopActors []ActorMoviesAmount `json:"top_actors"`
}

type SentimentProfitRatioAverage struct {
	PositiveAvgProfitRatio float64 `json:"positive_avg_profit_ratio"`
	NegativeAvgProfitRatio float64 `json:"negative_avg_profit_ratio"`
}

var mockedMovies = []Movie{
	{
		ID:                  "1",
		Title:               "Interstellar",
		Year:                2010,
		Genres:              []pkg.Genre{{ID: 1, Name: "Space"}},
		ProductionCountries: []pkg.Country{{Code: "GB", Name: "England"}, {Code: "US", Name: "USA"}},
		Budget:              100000000,
	},
	{
		ID:                  "2",
		Title:               "The Dark Knight",
		Year:                2008,
		Genres:              []pkg.Genre{{ID: 2, Name: "Action"}},
		ProductionCountries: []pkg.Country{{Code: "US", Name: "USA"}},
		Budget:              280000000,
	},
	{
		ID:                  "3",
		Title:               "Rata blanca",
		Year:                2011,
		Genres:              []pkg.Genre{{ID: 3, Name: "Comedy"}},
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}},
		Budget:              930000000,
	},
	{
		ID:                  "4",
		Title:               "El secreto de sus ojos",
		Year:                2009,
		Genres:              []pkg.Genre{{ID: 4, Name: "Drama"}},
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}, {Code: "ES", Name: "Spain"}},
		Budget:              69200000,
	},
	{
		ID:                  "5",
		Title:               "Relatos salvajes",
		Year:                2009,
		Genres:              []pkg.Genre{{ID: 5, Name: "Comedy"}},
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}},
		Budget:              27000000,
	},
	{
		ID:                  "6",
		Title:               "El padrino",
		Year:                1980,
		Genres:              []pkg.Genre{{ID: 6, Name: "Drama"}},
		ProductionCountries: []pkg.Country{{Code: "ES", Name: "Spain"}},
		Budget:              34000000,
	},
}

var MockedBatch = Batch[Movie]{
	Header: Header{Weight: uint32(len(mockedMovies)), TotalWeight: int32(-1)},
	Data:   mockedMovies,
}

var EOF = Batch[Movie]{
	Header: Header{
		TotalWeight: int32(len(MockedBatch.Data)),
	},
}

// MovieReview represents a review joined with a movie
type MovieReview struct {
	MovieID string  `json:"movie_id"`
	Title   string  `json:"title"`
	Rating  float64 `json:"rating"`
}

type Actor struct {
	ActorID string `json:"actor_id"`
	Name    string `json:"name"`
}

type MovieWithSentiment struct {
	Movie
	Sentiment Sentiment `json:"sentiment"`
}
