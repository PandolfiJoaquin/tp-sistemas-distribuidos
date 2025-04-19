package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"pkg/models"
	"regexp"
	"strconv"
	"strings"
	"time"

	"log/slog"
)

// Splits a string into pairs based on commas, while respecting quoted sections.
func splitPairs(input string) ([]string, error) {
	var pairs []string
	var current strings.Builder
	inQuote := false
	var quoteChar rune // will hold either ' or "

	for _, r := range input {
		// Check if the current rune is a quote (either ' or ")
		if r == '\'' || r == '"' {
			if inQuote {
				// We're inside a quoted section; only toggle off if we see the same kind of quote.
				if r == quoteChar {
					inQuote = false
					quoteChar = 0
				}
			} else {
				// Start a quoted section.
				inQuote = true
				quoteChar = r
			}
			current.WriteRune(r)
			continue
		}

		if r == ',' && !inQuote {
			// Outside any quotes, treat comma as a separator.
			pair := strings.TrimSpace(current.String())
			if pair != "" {
				pairs = append(pairs, pair)
			}
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		pair := strings.TrimSpace(current.String())
		if pair != "" {
			pairs = append(pairs, pair)
		}
	}

	return pairs, nil
}

// fixJSONObject fixes a JSON object by ensuring it is properly formatted. The "bad" format comes from python dictionaries. Thus we can take some liberties when formatting.
func fixJSONObject(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '{' || input[len(input)-1] != '}' {
		return "", fmt.Errorf("input is not a valid JSON object: %q", input)
	}
	content := strings.TrimSpace(input[1 : len(input)-1])
	if content == "" {
		return "{}", nil
	}

	pairs, err := splitPairs(content)
	if err != nil {
		return "", fmt.Errorf("error splitting pairs: %v", err)
	}

	var entries []string
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("unable to parse pair: %q", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		key = strings.Trim(key, "'")
		keyQuoted := strconv.Quote(key)

		var valueFixed string
		if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
			inner := value[1 : len(value)-1]
			valueFixed = strconv.Quote(inner)
		} else if strings.ToLower(value) == "none" {
			valueFixed = "null"
		} else {
			valueFixed = value
		}

		entries = append(entries, fmt.Sprintf("%s: %s", keyQuoted, valueFixed))
	}

	goodJSON := "{" + strings.Join(entries, ", ") + "}"
	return goodJSON, nil
}

// Fixes a JSON array by ensuring it is properly formatted. See fixJSONObject for details.
func fixJSONArray(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '[' || input[len(input)-1] != ']' {
		return "", fmt.Errorf("input is not a valid JSON array: %q", input)
	}

	// Use regex to extract substrings matching { ... }
	re := regexp.MustCompile(`\{[^}]*\}`)
	matches := re.FindAllString(input, -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("no objects found in input: %q", input)
	}

	var fixedObjects []string
	for _, obj := range matches {
		fixed, err := fixJSONObject(obj)
		if err != nil {
			return "", fmt.Errorf("error fixing object %q: %v", obj, err)
		}
		fixedObjects = append(fixedObjects, fixed)
	}

	// Reassemble the objects into a proper JSON array.
	return "[" + strings.Join(fixedObjects, ", ") + "]", nil
}

// ParseJSONToObject parses a JSON object into a struct of type T.
func ParseJSONToObject[T any](input string) (*T, error) {
	if input == "" {
		return nil, nil
	}

	fixed, err := fixJSONObject(input)
	if err != nil {
		return nil, fmt.Errorf("error fixing JSON object: %v", err)
	}

	var result T
	err = json.Unmarshal([]byte(fixed), &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	return &result, nil
}

// ParseJSONArray parses a JSON array of objects into a slice of type T.
func ParseJSONArray[T any](input string) ([]T, error) {
	if input == "" {
		return nil, nil
	}

	if input == "[]" {
		return []T{}, nil
	}

	fixed, err := fixJSONArray(input)
	if err != nil {
		return nil, fmt.Errorf("error fixing JSON array: %v", err)
	}

	var result []T
	err = json.Unmarshal([]byte(fixed), &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	return result, nil
}

// parseBool converts a string to bool ("True"/"False")
func parseBool(str, field string) (bool, error) {
	// Uses strings.EqualFold to handle case insensitivity.
	if strings.EqualFold(str, "true") {
		return true, nil
	} else if strings.EqualFold(str, "false") {
		return false, nil
	}

	return false, fmt.Errorf("invalid %s: %q", field, str)
}

// parseUint32 converts a string to uint32
func parseUint32(str, field string) (uint32, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return uint32(v), nil
}

// parseUint64 converts a string to uint64
func parseUint64(str, field string) (uint64, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return v, nil
}

// parseFloat32 converts a string to float32
func parseFloat32(str, field string) (float32, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(str, 32)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return float32(v), nil
}

// parseTime converts a string to time.Time
func parseTime(str, field string) (time.Time, error) {
	if str == "" {
		return time.Time{}, nil
	}
	v, err := time.Parse("2006-01-02", str)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return v, nil
}

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

var notNa = []int{
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

// hasNaNValues checks whether required fields are empty or NaN
func hasNaNValues(record []string) bool {
	for _, idx := range notNa {
		value := record[idx]
		if value == "" || value == "NaN" {
			return true
		}
	}
	return false
}

// parseMovie builds a RawMovie from a CSV record slice.
func parseMovie(record []string) (*models.RawMovie, error) {
	if hasNaNValues(record) {
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
