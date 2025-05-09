package utils

import (
	"errors"
	"pkg/models"
)

const (
	colUserID = iota
	colMovieID
	colRating
	colTimestamp
)

var reviewsNotNa = []int{
	colMovieID,
	colRating,
	colTimestamp,
}

var ErrInvalidReview = errors.New("invalid review")

func parseReview(record []string) (*models.RawReview, error) {
	if hasNaNValues(record, reviewsNotNa) {
		return nil, ErrInvalidReview
	}

	userID := record[colUserID]
	movieID := record[colMovieID]
	rating, err := parseFloat32(record[colRating], "rating")
	if err != nil {
		return nil, err
	}

	timestamp, err := parseTimestamp(record[colTimestamp], "timestamp")
	if err != nil {
		return nil, err
	}

	return &models.RawReview{
		UserID:    userID,
		MovieID:   movieID,
		Rating:    float64(rating),
		Timestamp: timestamp,
	}, nil
}
