package main

import (
	"pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

func MovieToQResult(batch common.Batch[common.Movie]) models.TotalQueryResults {
	q1Movies := make([]models.QueryResult, 0)
	for _, movie := range batch.Data {
		q1Movies = append(q1Movies, models.Q1Movie{
			Title:  movie.Title,
			Genres: movie.Genres,
		})
	}
	return models.TotalQueryResults{
		QueryId: 1,
		Items:   q1Movies,
		Last:    batch.IsEof(),
	}
}

func Top5CountriesToQResult(top5 common.Top5Countries) models.TotalQueryResults {
	q2Result := make([]models.QueryResult, len(top5.Countries))
	for i, country := range top5.Countries {
		q2Result[i] = models.Q2Country{
			Country: country.Country,
			Budget:  country.Budget,
		}
	}
	return models.TotalQueryResults{
		QueryId: 2,
		Items:   q2Result,
		Last:    true,
	}
}

func BestAndWorstToQResult(bestAndWorse common.BestAndWorstMovies) models.TotalQueryResults {
	best := models.Q3Movie{
		ID:     bestAndWorse.BestMovie.MovieID,
		Title:  bestAndWorse.BestMovie.Title,
		Rating: float32(bestAndWorse.BestMovie.Rating),
	}

	worst := models.Q3Movie{
		ID:     bestAndWorse.WorstMovie.MovieID,
		Title:  bestAndWorse.WorstMovie.Title,
		Rating: float32(bestAndWorse.WorstMovie.Rating),
	}

	return models.TotalQueryResults{
		QueryId: 3,
		Items: []models.QueryResult{
			models.Q3Result{
				Best:  best,
				Worst: worst,
			},
		},
		Last: true,
	}
}

func Top10ActorsToQResult(top10Actors common.Top10Actors) models.TotalQueryResults {
	q4Results := make([]models.QueryResult, len(top10Actors.TopActors))
	for i, actor := range top10Actors.TopActors {
		q4Results[i] = models.Q4Actors{
			ActorId:     actor.ActorID,
			ActorName:   actor.ActorName,
			Appearances: actor.MoviesAmount,
		}
	}
	return models.TotalQueryResults{
		QueryId: 4,
		Items:   q4Results,
		Last:    true,
	}
}

func SentimentToQResult(ratio common.SentimentProfitRatioAverage) models.TotalQueryResults {
	q5Result := []models.QueryResult{
		models.Q5Avg{
			PositiveAvgProfitRatio: ratio.PositiveAvgProfitRatio,
			NegativeAvgProfitRatio: ratio.NegativeAvgProfitRatio,
		},
	}
	return models.TotalQueryResults{
		QueryId: 5,
		Items:   q5Result,
		Last:    true,
	}
}
