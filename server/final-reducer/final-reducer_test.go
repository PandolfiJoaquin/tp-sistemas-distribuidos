package main

import (
	"testing"

	pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"

	"github.com/stretchr/testify/require"
)

func TestCalculateTop5Countries(t *testing.T) {
	tests := []struct {
		name     string
		input    map[pkg.Country]uint64
		expected common.Top5Countries
	}{
		{
			name: "normal case with more than 5 countries",
			input: map[pkg.Country]uint64{
				{Code: "US", Name: "USA"}:     1000,
				{Code: "CN", Name: "China"}:   2000,
				{Code: "JP", Name: "Japan"}:   1500,
				{Code: "DE", Name: "Germany"}: 800,
				{Code: "FR", Name: "France"}:  700,
				{Code: "GB", Name: "UK"}:      600,
			},
			expected: common.Top5Countries{
				Countries: []common.CountryBudget{
					{Country: pkg.Country{Code: "CN", Name: "China"}, Budget: 2000},
					{Country: pkg.Country{Code: "JP", Name: "Japan"}, Budget: 1500},
					{Country: pkg.Country{Code: "US", Name: "USA"}, Budget: 1000},
					{Country: pkg.Country{Code: "DE", Name: "Germany"}, Budget: 800},
					{Country: pkg.Country{Code: "FR", Name: "France"}, Budget: 700},
				},
			},
		},
		{
			name:     "empty map",
			input:    map[pkg.Country]uint64{},
			expected: common.Top5Countries{},
		},
		{
			name: "less than 5 countries",
			input: map[pkg.Country]uint64{
				{Code: "US", Name: "USA"}:   1000,
				{Code: "CN", Name: "China"}: 2000,
				{Code: "JP", Name: "Japan"}: 1500,
			},
			expected: common.Top5Countries{
				Countries: []common.CountryBudget{
					{Country: pkg.Country{Code: "CN", Name: "China"}, Budget: 2000},
					{Country: pkg.Country{Code: "JP", Name: "Japan"}, Budget: 1500},
					{Country: pkg.Country{Code: "US", Name: "USA"}, Budget: 1000},
					{Country: pkg.Country{Code: "US", Name: "USA"}, Budget: 1000},
					{Country: pkg.Country{Code: "US", Name: "USA"}, Budget: 1000},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateTop5Countries(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateBestAndWorstMovie(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]common.MovieAvgRating
		expected common.BestAndWorstMovies
	}{
		{
			name: "normal case",
			input: map[string]common.MovieAvgRating{
				"movie1": {Title: "Best Movie", RatingSum: 100, RatingCount: 10},
				"movie2": {Title: "Worst Movie", RatingSum: 20, RatingCount: 10},
				"movie3": {Title: "Average Movie", RatingSum: 50, RatingCount: 10},
			},
			expected: common.BestAndWorstMovies{
				BestMovie:  common.MovieReview{MovieID: "movie1", Title: "Best Movie", Rating: 10.0},
				WorstMovie: common.MovieReview{MovieID: "movie2", Title: "Worst Movie", Rating: 2.0},
			},
		},
		{
			name:     "empty map",
			input:    map[string]common.MovieAvgRating{},
			expected: common.BestAndWorstMovies{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateBestAndWorstMovie(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateTop10Actors(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]common.ActorMoviesAmount
		expected common.Top10Actors
	}{
		{
			name: "normal case with more than 10 actors",
			input: map[string]common.ActorMoviesAmount{
				"actor1":  {ActorID: "actor1", ActorName: "Actor 1", MoviesAmount: 100},
				"actor2":  {ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
				"actor3":  {ActorID: "actor3", ActorName: "Actor 3", MoviesAmount: 80},
				"actor4":  {ActorID: "actor4", ActorName: "Actor 4", MoviesAmount: 70},
				"actor5":  {ActorID: "actor5", ActorName: "Actor 5", MoviesAmount: 60},
				"actor6":  {ActorID: "actor6", ActorName: "Actor 6", MoviesAmount: 50},
				"actor7":  {ActorID: "actor7", ActorName: "Actor 7", MoviesAmount: 40},
				"actor8":  {ActorID: "actor8", ActorName: "Actor 8", MoviesAmount: 30},
				"actor9":  {ActorID: "actor9", ActorName: "Actor 9", MoviesAmount: 20},
				"actor10": {ActorID: "actor10", ActorName: "Actor 10", MoviesAmount: 10},
				"actor11": {ActorID: "actor11", ActorName: "Actor 11", MoviesAmount: 5},
			},
			expected: common.Top10Actors{
				TopActors: []common.ActorMoviesAmount{
					{ActorID: "actor1", ActorName: "Actor 1", MoviesAmount: 100},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor3", ActorName: "Actor 3", MoviesAmount: 80},
					{ActorID: "actor4", ActorName: "Actor 4", MoviesAmount: 70},
					{ActorID: "actor5", ActorName: "Actor 5", MoviesAmount: 60},
					{ActorID: "actor6", ActorName: "Actor 6", MoviesAmount: 50},
					{ActorID: "actor7", ActorName: "Actor 7", MoviesAmount: 40},
					{ActorID: "actor8", ActorName: "Actor 8", MoviesAmount: 30},
					{ActorID: "actor9", ActorName: "Actor 9", MoviesAmount: 20},
					{ActorID: "actor10", ActorName: "Actor 10", MoviesAmount: 10},
				},
			},
		},
		{
			name:     "empty map",
			input:    map[string]common.ActorMoviesAmount{},
			expected: common.Top10Actors{},
		},
		{
			name: "less than 10 actors",
			input: map[string]common.ActorMoviesAmount{
				"actor1": {ActorID: "actor1", ActorName: "Actor 1", MoviesAmount: 100},
				"actor2": {ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
			},
			expected: common.Top10Actors{
				TopActors: []common.ActorMoviesAmount{
					{ActorID: "actor1", ActorName: "Actor 1", MoviesAmount: 100},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
					{ActorID: "actor2", ActorName: "Actor 2", MoviesAmount: 90},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateTop10Actors(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateSentimentProfitRatioAverage(t *testing.T) {
	tests := []struct {
		name     string
		input    common.SentimentProfitRatioAccumulator
		expected common.SentimentProfitRatioAverage
	}{
		{
			name: "normal case with both positive and negative ratios",
			input: common.SentimentProfitRatioAccumulator{
				PositiveProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   100.0,
					ProfitRatioCount: 10,
				},
				NegativeProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   50.0,
					ProfitRatioCount: 5,
				},
			},
			expected: common.SentimentProfitRatioAverage{
				PositiveAvgProfitRatio: 10.0,
				NegativeAvgProfitRatio: 10.0,
			},
		},
		{
			name: "zero counts",
			input: common.SentimentProfitRatioAccumulator{
				PositiveProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   100.0,
					ProfitRatioCount: 0,
				},
				NegativeProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   50.0,
					ProfitRatioCount: 0,
				},
			},
			expected: common.SentimentProfitRatioAverage{
				PositiveAvgProfitRatio: -1.0,
				NegativeAvgProfitRatio: -1.0,
			},
		},
		{
			name: "only positive ratios",
			input: common.SentimentProfitRatioAccumulator{
				PositiveProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   100.0,
					ProfitRatioCount: 10,
				},
				NegativeProfitRatio: common.ProfitRatioAccumulator{
					ProfitRatioSum:   0.0,
					ProfitRatioCount: 0,
				},
			},
			expected: common.SentimentProfitRatioAverage{
				PositiveAvgProfitRatio: 10.0,
				NegativeAvgProfitRatio: -1.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateSentimentProfitRatioAverage(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
