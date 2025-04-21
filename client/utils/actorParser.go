package utils

import (
	"fmt"
	"pkg/models"
)

const (
	colCast = iota
	colCrew
	colActorMovieID
)

var ErrInvalidCredit = fmt.Errorf("invalid credits")

func parseCredits(record []string) (*models.RawCredits, error) {
	if hasNaNValues(record, []int{colActorMovieID}) {
		return nil, ErrInvalidCredit
	}
	cast, err := ParseJSONArray[models.CastMember](record[colCast])
	if err != nil {
		return nil, fmt.Errorf("error parsing cast: %v", err)
	}

	crew, err := ParseJSONArray[models.CrewMember](record[colCrew])
	if err != nil {
		return nil, fmt.Errorf("error parsing genres: %v", err)
	}

	movieID := record[colActorMovieID]

	return &models.RawCredits{
		Cast:    cast,
		Crew:    crew,
		MovieId: movieID,
	}, nil
}
