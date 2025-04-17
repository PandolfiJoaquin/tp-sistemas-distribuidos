package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"pkg/models"
)

type MoviesReader struct {
	Finished bool
	Reader *csv.Reader
	file *os.File
	batch_size int
	fields []string
}

func NewMoviesReader(path string, batch_size int) *MoviesReader {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file:", err)		
		return nil
	}

	csv_reader := csv.NewReader(file)
	csv_reader.LazyQuotes = true
	csv_reader.FieldsPerRecord = -1 // Allow variable number of fields

	fields, err := csv_reader.Read() // Read the header line
	if err != nil {
		fmt.Println("Error reading header:", err)
		return nil
	}

	reader := &MoviesReader{
		Reader: csv_reader,
		file: file,
		batch_size: batch_size,
		fields : fields,
	}

	return reader
}

func (mr *MoviesReader) ReadMovie() (*models.Movie, error) {
	expectedFields := len(mr.fields)

	if mr.Finished {
		return nil, nil
	}

		record, err := mr.Reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				mr.Finished = true
				return nil, nil
			}else {
				return nil, err
			}
		}

		// If we read a record but its field count is off, try to join with more rows.
		if len(record) != expectedFields {
			record, err = joinRecords(mr.Reader, record, expectedFields)
			if err != nil {
				return nil, err
			}
		}

		movie, err := parseMovie(record)
		if err != nil {
			return nil, fmt.Errorf("error parsing movie: %v", err)
		}

		return movie, nil
}

func joinRecords(r *csv.Reader, current []string, expectedFields int) ([]string, error) {
	// Keep joining records until we have at least expectedFields fields.
	joined := current
	for len(joined) < expectedFields {
		next, err := r.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to join record: %v", err)
		}

		// Here we simply join the last field with all fields from the next read,
		joined[len(joined)-1] = joined[len(joined)-1] + " " + next[0]

		// If the next record had additional fields, append them.
		if len(next) > 1 {
			joined = append(joined, next[1:]...)
		}
	}

	return joined, nil
}

func (mr *MoviesReader) Close() {
	if mr.file != nil {
		mr.file.Close()
	}
}

func parseMovie(record []string) (*models.Movie, error) {
	adult := record[0] == "True"

	collection, err := ParseObject[models.Collection](record[1])
	if err != nil {
		return nil, err
	}


	budget, err := strconv.ParseUint(record[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing budget: %v", err)
	}

	genres , err := ParseObjectArray[models.Genre](record[3])
	if err != nil {
		return nil, fmt.Errorf("error parsing genres: %v", err)
	}

	homepage := record[4]

	id, err := strconv.ParseInt(record[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing id: %v", err)
	}

	imdbID := record[6]
	originalLanguage := record[7]
	originalTitle := record[8]
	overview := record[9]
	popularity, err := strconv.ParseFloat(record[10], 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing popularity: %v", err)
	}

	posterPath := record[11]

	productionCompanies, err := ParseObjectArray[models.Company](record[12])
	if err != nil {
		return nil, err
	}

	productionCountries, err := ParseObjectArray[models.Country](record[13])
	if err != nil {
		return nil, err
	}

	releaseDate := record[14]
	revenue, err := strconv.ParseInt(record[15], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing revenue: %v", err)
	}

    var runtime float64
    if record[16] == "" {
        runtime = 0
    } else {
        runtime, err = strconv.ParseFloat(record[16], 64)
        if err != nil {
            return nil, fmt.Errorf("error parsing runtime: %v", err)
        }
    }

	spokenLanguages , err := ParseObjectArray[models.Language](record[17])
	if err != nil {
		return nil, err
	}

	status := record[18]
	tagline := record[19]
	title := record[20]
	video := record[21] == "True"

	voteAverage, err := strconv.ParseFloat(record[22], 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing vote average: %v", err)
	}

	voteCount, err := strconv.ParseInt(record[23], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing vote count: %v", err)
	}

	return &models.Movie{
		Adult:               adult,
		BelongsToCollection: collection,
		Budget:              budget,
		Genres:              genres,
		Homepage:            homepage,
		ID:                  id,
		IMDBID:              imdbID,
		OriginalLanguage:    originalLanguage,
		OriginalTitle:       originalTitle,
		Overview:            overview,
		Popularity:          popularity,
		PosterPath:          posterPath,
		ProductionCompanies: productionCompanies,
		ProductionCountries: productionCountries,
		ReleaseDate:         releaseDate,
		Revenue:             revenue,
		Runtime:             runtime,
		SpokenLanguages:     spokenLanguages,
		Status:              status,
		Tagline: 		     tagline,
		Title:               title,
		Video:               video,
		VoteAverage:         voteAverage,
		VoteCount:           voteCount,
	} , nil
}
