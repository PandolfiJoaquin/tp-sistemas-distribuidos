package main

import (
	"pkg/models"
	"strconv"
	"tp-sistemas-distribuidos/server/common"
)

func preprocessMovies(batch models.RawBatch[models.RawMovie]) common.Batch[common.Movie] {
	movies := make([]common.Movie, 0)

	for _, movie := range batch.Data {
		id := strconv.Itoa(int(movie.ID))
		title := movie.Title
		year := movie.ReleaseDate.Year()

		movies = append(movies, common.Movie{
			ID:                  id,
			Title:               title,
			Year:                year,
			Genres:              movie.Genres,
			ProductionCountries: movie.ProductionCountries,
			Budget:              movie.Budget,
			Overview:            movie.Overview,
			Revenue:             movie.Revenue,
		})
	}

	res := makeBatchMsg[common.Movie](batch.Header.Weight, movies, batch.Header.TotalWeight)

	return res
}

func preprocessReviews(batch models.RawBatch[models.RawReview]) common.Batch[common.Review] {
	reviews := make([]common.Review, 0)

	for _, review := range batch.Data {
		id := review.UserID
		movieID := review.MovieID
		rating := review.Rating

		reviews = append(reviews, common.Review{
			ID:      id,
			MovieID: movieID,
			Rating:  rating,
		})
	}
	res := makeBatchMsg[common.Review](batch.Header.Weight, reviews, batch.Header.TotalWeight)

	return res
}

func preprocessCredits(batch models.RawBatch[models.RawCredits]) common.Batch[common.Credit] {
	credits := make([]common.Credit, 0)

	for _, credit := range batch.Data {
		movieID := credit.MovieId
		actors := make([]common.Actor, 0)

		for _, actor := range credit.Cast {
			actors = append(actors, common.Actor{
				ActorID: strconv.Itoa(actor.ID),
				Name:    actor.Name,
			})
		}

		credits = append(credits, common.Credit{
			MovieId: movieID,
			Actors:  actors,
		})
	}

	res := makeBatchMsg[common.Credit](batch.Header.Weight, credits, batch.Header.TotalWeight)

	return res
}
