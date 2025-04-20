package models

import "time"

type RawReview struct {
	UserID    string    `json:"user_id"`
	MovieID   string    `json:"movie_id"`
	Rating    float64   `json:"rating"`
	Timestamp time.Time `json:"timestamp"`
}
