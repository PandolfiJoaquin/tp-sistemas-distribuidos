package utils

import (
	"errors"
	"fmt"
	"log/slog"
	"pkg/models"
)

const (
	colAdult = iota
	colCollection
	colBudget
	colGenres
	colHomepage
	colID
	colIMDBID
	colOriginalLanguage
	colOriginalTitle
	colOverview
	colPopularity
	colPosterPath
	colProductionCompanies
	colProductionCountries
	colReleaseDate
	colRevenue
	colRuntime
	colSpokenLanguages
	colStatus
	colTagline
	colTitle
	colVideo
	colVoteAverage
	colVoteCount
)

var moviesNotNa = []int{
	colID,
	colTitle,
	colGenres,
	colReleaseDate,
	colOverview,
	colProductionCountries,
	colSpokenLanguages,
	colBudget,
	colRevenue,
}

var ErrInvalidMovie = errors.New("invalid movie")

// parseMovie builds a RawMovie from a CSV record slice.
func parseMovie(record []string) (*models.RawMovie, error) {
	if hasNaNValues(record, moviesNotNa) {
		slog.Warn("invalid movie, dropping", slog.Any("record", record[colID]))
		return nil, ErrInvalidMovie
	}

	adult, err := parseBool(record[colAdult], "adult")
	if err != nil {
		return nil, err
	}

	collection, err := ParseJSONToObject[models.Collection](record[colCollection])
	if err != nil {
		return nil, fmt.Errorf("error parsing collection: %v", err)
	}

	budget, err := parseUint64(record[colBudget], "budget")
	if err != nil {
		return nil, err
	}

	genres, err := ParseJSONArray[models.Genre](record[colGenres])
	if err != nil {
		return nil, fmt.Errorf("error parsing genres: %v", err)
	}

	id, err := parseUint32(record[colID], "id")
	if err != nil {
		return nil, err
	}

	popularity, err := parseFloat32(record[colPopularity], "popularity")
	if err != nil {
		return nil, err
	}

	productionCompanies, err := ParseJSONArray[models.Company](record[colProductionCompanies])
	if err != nil {
		return nil, fmt.Errorf("error parsing production companies: %v", err)
	}

	productionCountries, err := ParseJSONArray[models.Country](record[colProductionCountries])
	if err != nil {
		return nil, fmt.Errorf("error parsing production countries: %v", err)
	}

	releaseDate, err := parseTime(record[colReleaseDate], "release_date")
	if err != nil {
		return nil, err
	}

	revenue, err := parseUint64(record[colRevenue], "revenue")
	if err != nil {
		return nil, err
	}

	runtime, err := parseFloat32(record[colRuntime], "runtime")
	if err != nil {
		return nil, err
	}

	spokenLanguages, err := ParseJSONArray[models.Language](record[colSpokenLanguages])
	if err != nil {
		return nil, fmt.Errorf("error parsing spoken languages: %v", err)
	}

	status := record[colStatus]
	tagline := record[colTagline]
	title := record[colTitle]

	video, err := parseBool(record[colVideo], "video")
	if err != nil {
		return nil, err
	}

	voteAverage, err := parseFloat32(record[colVoteAverage], "vote_average")
	if err != nil {
		return nil, err
	}

	voteCount, err := parseUint32(record[colVoteCount], "vote_count")
	if err != nil {
		return nil, err
	}

	return &models.RawMovie{
		Adult:               adult,
		BelongsToCollection: collection,
		Budget:              budget,
		Genres:              genres,
		Homepage:            record[colHomepage],
		ID:                  id,
		IMDBID:              record[colIMDBID],
		OriginalLanguage:    record[colOriginalLanguage],
		OriginalTitle:       record[colOriginalTitle],
		Overview:            record[colOverview],
		Popularity:          popularity,
		PosterPath:          record[colPosterPath],
		ProductionCompanies: productionCompanies,
		ProductionCountries: productionCountries,
		ReleaseDate:         releaseDate,
		Revenue:             revenue,
		Runtime:             runtime,
		SpokenLanguages:     spokenLanguages,
		Status:              status,
		Tagline:             tagline,
		Title:               title,
		Video:               video,
		VoteAverage:         voteAverage,
		VoteCount:           voteCount,
	}, nil
}
