package common

import (
	"encoding/json"
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

func (m Movie) ToString() string {
	json, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(json)
}

func MovieFromString(s string) (Movie, error) {
	var m Movie
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		return Movie{}, err
	}
	return m, nil
}

type MovieWithSentiment struct {
	Movie
	Sentiment Sentiment `json:"sentiment"`
}

func (m *MovieWithSentiment) ToString() string {
	return m.Movie.ToString() + "," + strconv.Itoa(int(m.Sentiment))
}

type Review struct {
	ID      string  `json:"id"`
	MovieID string  `json:"movie_id"`
	Rating  float64 `json:"rating"`
}

func (r Review) ToString() string {
	return r.ID + "," + r.MovieID + "," + strconv.FormatFloat(r.Rating, 'f', -1, 64)
}

type Actor struct {
	ActorID string `json:"actor_id"`
	Name    string `json:"name"`
}

func (a Actor) ToString() string {
	json, err := json.Marshal(a)
	if err != nil {
		return ""
	}
	return string(json)
}

func ReviewFromString(s string) (Review, error) {
	var r Review
	err := json.Unmarshal([]byte(s), &r)
	if err != nil {
		return Review{}, err
	}
	return r, nil
}

type Credit struct {
	Actors  []Actor `json:"actors"`
	MovieId string  `json:"movie_id"`
}


func (c Credit) ToString() string {
	json, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(json)
}

func CreditFromString(s string) (Credit, error) {
	var c Credit
	err := json.Unmarshal([]byte(s), &c)
	if err != nil {
		return Credit{}, err
	}
	return c, nil
}

type MovieReview struct {
	MovieID string  `json:"movie_id"`
	Title   string  `json:"title"`
	Rating  float64 `json:"rating"`
}

func (mr MovieReview) ToString() string {
	return mr.MovieID + "," + mr.Title + "," + strconv.FormatFloat(mr.Rating, 'f', -1, 64)
}

type Stringer interface {
	ToString() string
}
