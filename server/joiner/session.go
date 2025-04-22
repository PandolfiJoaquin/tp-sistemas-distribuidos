package main

import (
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

type JoinerSession struct {
	movies          []common.Movie
	moviesReceived  uint32
	reviewsReceived uint32
	creditsReceived uint32
	moviesToExpect  int32
	reviewsToExpect int32
	creditsToExpect int32
}

func NewJoinerSession() *JoinerSession {
	return &JoinerSession{
		movies:          []common.Movie{},
		moviesReceived:  0,
		reviewsReceived: 0,
		creditsReceived: 0,
		moviesToExpect:  -1,
		reviewsToExpect: -1,
		creditsToExpect: -1,
	}
}

func (s *JoinerSession) SaveMovies(batch common.Batch[common.Movie]) {
	if batch.IsEof() {
		slog.Info("movies Eof received", slog.Any("header", batch.Header))
		s.moviesToExpect = batch.TotalWeight
	}
	s.movies = append(s.movies, batch.Data...)
	s.moviesReceived += batch.Weight
}

func (s *JoinerSession) GetMovies() []common.Movie {
	return s.movies
}

func (s *JoinerSession) AllMoviesReceived() bool {
	if s.moviesReceived > uint32(s.moviesToExpect) {
		slog.Error("total weight received is greater than total weight", slog.Any("moviesReceived", s.moviesReceived), slog.Any("moviesToExpect", s.moviesToExpect))
	}
	return s.moviesReceived == uint32(s.moviesToExpect)
}

func (s *JoinerSession) NotifyCredit(header common.Header) {
	if header.IsEof() {
		slog.Info("credits Eof received", slog.Any("header", header))
		s.creditsToExpect = header.TotalWeight
	}
	s.creditsReceived += header.Weight
	s.cleanUp()
}

func (s *JoinerSession) cleanUp() {
	if s.creditsReceived != uint32(s.creditsToExpect) ||
		s.reviewsReceived != uint32(s.reviewsToExpect) {
		return
	}
	slog.Info("cleaning up session", slog.Any("creditsReceived", s.creditsReceived), slog.Any("reviewsReceived", s.reviewsReceived))
	s.movies = []common.Movie{}

}

func (s *JoinerSession) NotifyReview(header common.Header) {
	if header.IsEof() {
		slog.Info("reviews Eof received", slog.Any("header", header))
		s.reviewsToExpect = header.TotalWeight
		return
	}
	s.reviewsReceived += header.Weight
	s.cleanUp()
}

/*if totalWeightReceived > totalWeight {
	slog.Error("total weight received is greater than total weight")
	return false
}*/
