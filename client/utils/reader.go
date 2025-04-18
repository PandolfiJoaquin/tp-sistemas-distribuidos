package utils

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"pkg/models"
)

// MoviesReader is a struct that reads movies from a CSV file.

type MoviesReader struct {
	Finished  bool
	Reader    *csv.Reader
	file      *os.File
	batchSize int
	fields    []string
}

func NewMoviesReader(path string, batchSize int) (*MoviesReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	csv_reader := csv.NewReader(file)
	csv_reader.LazyQuotes = true
	csv_reader.FieldsPerRecord = -1 // Allow variable number of fields

	fields, err := csv_reader.Read() // Read the header line
	if err != nil {
		return nil, fmt.Errorf("error reading header line: %v", err)
	}

	reader := &MoviesReader{
		Reader:    csv_reader,
		file:      file,
		batchSize: batchSize,
		fields:    fields,
	}

	return reader, nil
}

func (mr *MoviesReader) ReadMovie() (*models.RawMovie, error) {
	expectedFields := len(mr.fields)

	if mr.Finished {
		return nil, nil
	}

	record, err := mr.Reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			mr.Finished = true
			return nil, nil
		} else {
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

func (mr *MoviesReader) ReadMovies() ([]*models.RawMovie, error) {
	var movies []*models.RawMovie
	for i := 0; i < mr.batchSize; i++ {
		movie, err := mr.ReadMovie()
		if err != nil {
			return nil, err
		}
		if movie == nil {
			break
		}
		movies = append(movies, movie)
	}
	return movies, nil
}
