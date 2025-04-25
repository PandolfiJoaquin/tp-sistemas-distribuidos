package main

import (
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

type JoinerService struct {
	movies          []common.Movie
	moviesReceived  uint32
	reviewsReceived uint32
	creditsReceived uint32
	moviesToExpect  int32
	reviewsToExpect int32
	creditsToExpect int32
}

func NewJoinerService() *JoinerService {
	return &JoinerService{
		movies:          []common.Movie{},
		moviesReceived:  0,
		reviewsReceived: 0,
		creditsReceived: 0,
		moviesToExpect:  -1,
		reviewsToExpect: -1,
		creditsToExpect: -1,
	}
}

func (s *JoinerService) SaveMovies(batch common.Batch[common.Movie]) {
	if batch.IsEof() {
		slog.Info("movies Eof received", slog.Any("header", batch.Header))
		s.moviesToExpect = batch.TotalWeight
		return
	}
	s.movies = append(s.movies, batch.Data...)
	s.moviesReceived += batch.Weight
}

func (s *JoinerService) GetMovies() []common.Movie {
	return s.movies
}

func (s *JoinerService) AllMoviesReceived() bool {
	if s.moviesReceived > uint32(s.moviesToExpect) {
		slog.Error("total weight received is greater than total weight", slog.Any("moviesReceived", s.moviesReceived), slog.Any("moviesToExpect", s.moviesToExpect))
	}
	return s.moviesReceived == uint32(s.moviesToExpect)
}

func (s *JoinerService) NotifyCredit(header common.Header) {
	if header.IsEof() {
		slog.Info("credits Eof received", slog.Any("header", header), slog.Any("creditsReceived", s.creditsReceived), slog.Any("reviewsReceived", s.reviewsReceived))
		s.creditsToExpect = header.TotalWeight
	} else {
		s.creditsReceived += header.Weight
	}
}

func (s *JoinerService) NotifyReview(header common.Header) {
	if header.IsEof() {
		slog.Info("reviews Eof received", slog.Any("header", header), slog.Any("creditsReceived", s.creditsReceived), slog.Any("reviewsReceived", s.reviewsReceived))
		s.reviewsToExpect = header.TotalWeight
	} else {
		s.reviewsReceived += header.Weight
	}
}

func (s *JoinerService) IsDone() bool {
	return s.creditsReceived == uint32(s.creditsToExpect) &&
		s.reviewsReceived == uint32(s.reviewsToExpect)
}

func (s *JoinerService) LogState() {
	slog.Info(
		"JoinerService",
		slog.Any("moviesReceived", s.moviesReceived),
		slog.Any("moviesToExpect", s.moviesToExpect),
		slog.Any("creditsReceived", s.creditsReceived),
		slog.Any("creditsToExpect", s.creditsToExpect),
		slog.Any("reviewsReceived", s.reviewsReceived),
		slog.Any("reviewsToExpect", s.reviewsToExpect))
}

func (s *JoinerService) join(reviews []common.Review) []common.MovieReview {
	joinedReviews := common.Map(reviews, s.joinReview)
	return common.Flatten(joinedReviews)
}

func (s *JoinerService) joinReview(r common.Review) []common.MovieReview {

	movies := s.GetMovies()
	moviesForReview := common.Filter(movies, func(m common.Movie) bool { return m.ID == r.MovieID })
	reviewXMovies := common.Map(moviesForReview, func(m common.Movie) common.MovieReview {
		return common.MovieReview{
			MovieID: m.ID,
			Title:   m.Title,
			Rating:  r.Rating,
		}
	})
	return reviewXMovies
}

func (s *JoinerService) filterCredits(data []common.Credit) []common.Credit {
	movies := s.GetMovies()
	movieIds := common.Map(movies, func(m common.Movie) string { return m.ID })
	ids := make(map[string]bool)
	for _, id := range movieIds {
		ids[id] = true
	}
	actors := common.Filter(data, func(c common.Credit) bool {
		return ids[c.MovieId]
	})
	return actors
}
