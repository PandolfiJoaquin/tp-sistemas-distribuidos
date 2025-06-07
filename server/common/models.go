package common

import (
	"fmt"
	pkg "pkg/models"
	"strconv"
	"strings"
)

type Sentiment int

const (
	Positive Sentiment = iota
	Negative
)

const movieSep = ","
const creditSep = ","
const actorSep = ","



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
	
	return m.ID + movieSep + m.Title + movieSep + string(rune(m.Year)) + movieSep +
		strconv.FormatUint(m.Budget, 10) + movieSep + strconv.FormatUint(m.Revenue, 10) + movieSep + m.Overview
	  + movieSep +
}

type MovieWithSentiment struct {
	Movie
	Sentiment Sentiment `json:"sentiment"`
}

func MovieFromString(data string) (Movie, error) {

	parts := strings.Split(data, movieSep)
	if len(parts) < 6 {
		return Movie{}, fmt.Errorf("invalid movie format: %s", data)
	}

	id := parts[0]
	title := parts[1]
	year, err := strconv.Atoi(parts[2])
	if err != nil {
		return Movie{}, fmt.Errorf("invalid year format: %s", parts[2])
	}
	budget, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return Movie{}, fmt.Errorf("invalid budget format: %s", parts[3])
	}
	revenue, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return Movie{}, fmt.Errorf("invalid revenue format: %s", parts[4])
	}
	overview := parts[5]

	return Movie{
		ID:                  id,
		Title:               title,
		Year:                year,
		Budget:              budget,
		Revenue:             revenue,
		Overview:            overview,
	}, nil
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
	return a.ActorID + "," + a.Name
}

type Credit struct {
	Actors  []Actor `json:"actors"`
	MovieId string  `json:"movie_id"`
}


func (c Credit) ToString() string {
	actorsStr := ""
	for i, actor := range c.Actors {
		if i > 0 {
			actorsStr += ";"
		}
		actorsStr += actor.ToString()
	}
	return c.MovieId + "," + actorsStr
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
