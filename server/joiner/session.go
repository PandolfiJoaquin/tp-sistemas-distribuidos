package main

import (
	"log/slog"
	"tp-sistemas-distribuidos/server/common"
)

type DuplicatesFilters struct {
	MovieFilter   *common.DuplicateFilter `json:"movies"`
	ReviewsFilter *common.DuplicateFilter `json:"reviews"`
	CreditsFilter *common.DuplicateFilter `json:"credits"`
}

type JoinerSession struct {
	Movies          []common.Movie     `json:"movies"`
	MoviesReceived  uint32             `json:"moviesReceived"`
	ReviewsReceived uint32             `json:"reviewsReceived"`
	CreditsReceived uint32             `json:"creditsReceived"`
	MoviesToExpect  int32              `json:"moviesToExpect"`
	ReviewsToExpect int32              `json:"reviewsToExpect"`
	CreditsToExpect int32              `json:"creditsToExpect"`
	Filters         *DuplicatesFilters `json:"duplicate"`
}

func NewJoinerSession() *JoinerSession {
	df := &DuplicatesFilters{
		common.NewDuplicateFilter(),
		common.NewDuplicateFilter(),
		common.NewDuplicateFilter(),
	}
	return &JoinerSession{
		Movies:          []common.Movie{},
		MoviesReceived:  0,
		ReviewsReceived: 0,
		CreditsReceived: 0,
		MoviesToExpect:  -1,
		ReviewsToExpect: -1,
		CreditsToExpect: -1,
		Filters:         df,
	}
}

func (j *JoinerSession) FilterMoviesMsg(id int) bool {
	return j.Filters.MovieFilter.Accept(id)
}

func (j *JoinerSession) FilterReviewsMsg(id int) bool {
	return j.Filters.ReviewsFilter.Accept(id)

}
func (j *JoinerSession) FilterCreditsMsg(id int) bool {
	return j.Filters.CreditsFilter.Accept(id)
}

func (s *JoinerSession) UpdateMoviesWeights(header common.Header) {
	if header.IsEof() {
		// slog.Info("movies Eof received", slog.Any("header", header))
		s.MoviesToExpect = header.TotalWeight
		return
	}
	s.MoviesReceived += header.Weight
}

func (s *JoinerSession) SaveMovies(movies []common.Movie) {
	s.Movies = append(s.Movies, movies...)
}

func (s *JoinerSession) GetMovies() []common.Movie {
	return s.Movies
}

func (s *JoinerSession) AllMoviesReceived() bool {
	if s.MoviesReceived > uint32(s.MoviesToExpect) {
		slog.Error("total weight received is greater than total weight", slog.Any("moviesReceived", s.MoviesReceived), slog.Any("moviesToExpect", s.MoviesToExpect))
	}
	return s.MoviesReceived == uint32(s.MoviesToExpect)
}

func (s *JoinerSession) UpdateCreditsWeights(header common.Header) {
	if header.IsEof() {
		// slog.Info("credits Eof received", slog.Any("header", header), slog.Any("creditsReceived", s.CreditsReceived), slog.Any("reviewsReceived", s.ReviewsReceived))
		s.CreditsToExpect = header.TotalWeight
	} else {
		s.CreditsReceived += header.Weight
	}
}

func (s *JoinerSession) UpdateReviewsWeights(header common.Header) {
	if header.IsEof() {
		// slog.Info("reviews Eof received", slog.Any("header", header), slog.Any("creditsReceived", s.CreditsReceived), slog.Any("reviewsReceived", s.ReviewsReceived))
		s.ReviewsToExpect = header.TotalWeight
	} else {
		s.ReviewsReceived += header.Weight
	}
}

func (s *JoinerSession) IsDone() bool {
	return s.CreditsReceived == uint32(s.CreditsToExpect) &&
		s.ReviewsReceived == uint32(s.ReviewsToExpect)
}

func (s *JoinerSession) LogState() {
	slog.Info(
		"JoinerSession",
		slog.Any("moviesReceived", s.MoviesReceived),
		slog.Any("moviesToExpect", s.MoviesToExpect),
		slog.Any("creditsReceived", s.CreditsReceived),
		slog.Any("creditsToExpect", s.CreditsToExpect),
		slog.Any("reviewsReceived", s.ReviewsReceived),
		slog.Any("reviewsToExpect", s.ReviewsToExpect))
}

func (s *JoinerSession) Join(reviews []common.Review) []common.MovieReview {
	joinedReviews := common.Map(reviews, s.joinReview)
	return common.Flatten(joinedReviews)
}

func (s *JoinerSession) joinReview(r common.Review) []common.MovieReview {

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

func (s *JoinerSession) filterCredits(data []common.Credit) []common.Credit {
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
