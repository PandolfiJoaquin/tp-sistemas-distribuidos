package common

import (
	pkg "pkg/models"
	"strconv"
)

type Sentiment int

const (
	Positive Sentiment = iota
	Negative
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

func (m *Movie) ToString() string {
	return m.ID + "," + m.Title + "," + string(rune(m.Year)) + "," +
		strconv.FormatUint(m.Budget, 10) + "," + strconv.FormatUint(m.Revenue, 10) + "," + m.Overview
}

type MovieWithSentiment struct {
	Movie
	Sentiment Sentiment `json:"sentiment"`
}

type Review struct {
	ID      string  `json:"id"`
	MovieID string  `json:"movie_id"`
	Rating  float64 `json:"rating"`
}

type Actor struct {
	ActorID string `json:"actor_id"`
	Name    string `json:"name"`
}

type Credit struct {
	Actors  []Actor `json:"actors"`
	MovieId string  `json:"movie_id"`
}

type MovieReview struct {
	MovieID string  `json:"movie_id"`
	Title   string  `json:"title"`
	Rating  float64 `json:"rating"`
}
