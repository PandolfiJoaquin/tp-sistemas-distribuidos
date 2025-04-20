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

type ToProcessMsg struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

type CountryBudget struct {
	Country pkg.Country `json:"country"`
	Budget  uint64      `json:"budget"`
}

type CoutriesBudgetMsg struct {
	Header    Header          `json:"header"`
	Countries []CountryBudget `json:"countries"`
}

func (b *CoutriesBudgetMsg) IsEof() bool { //TODO: se puede obviar si se compone con el Header y le pongo el metodo al header
	return b.Header.TotalWeight > 0
}

type MovieAvgRating struct {
	MovieID     string  `json:"movie_id"`
	RatingSum   float64 `json:"rating_sum"`
	RatingCount uint32  `json:"rating_count"`
}

type MoviesAvgRatingMsg struct {
	Header        Header           `json:"header"`
	MoviesRatings []MovieAvgRating `json:"movies_ratings"`
}

func (b *MoviesAvgRatingMsg) IsEof() bool { //TODO: se puede obviar si se compone con el Header y le pongo el metodo al header
	return b.Header.TotalWeight > 0
}

type BestAndWorstMovies struct {
	BestMovie  string `json:"best_movie"`
	WorstMovie string `json:"worst_movie"`
}

type Top5Countries struct {
	FirstCountry  pkg.Country `json:"first_country"`
	SecondCountry pkg.Country `json:"second_country"`
	ThirdCountry  pkg.Country `json:"third_country"`
	FourthCountry pkg.Country `json:"fourth_country"`
	FifthCountry  pkg.Country `json:"fifth_country"`
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
	MovieID string `json:"movie_id"`
}

type MovieActor struct { //si, es igual al de arriba
	ActorID string `json:"actor_id"`
	Name    string `json:"name"`
	MovieID string `json:"movie_id"`
}
