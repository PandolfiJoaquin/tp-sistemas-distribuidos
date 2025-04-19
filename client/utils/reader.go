package utils

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"pkg/models"
)

// TODO: ver si se puede hacer una interface

// MoviesReader is a struct that reads movies from a CSV file.

type MoviesReader struct {
	Finished  bool
	Reader    *csv.Reader
	file      *os.File
	batchSize int
	fields    []string
	Total     int
}

func NewMoviesReader(path string, batchSize int) (*MoviesReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	csvReader := csv.NewReader(file)
	if csvReader == nil {
		return nil, fmt.Errorf("error creating CSV reader")
	}

	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	fields, err := csvReader.Read() // Read the header line
	if err != nil {
		return nil, fmt.Errorf("error reading header line: %v", err)
	}

	reader := &MoviesReader{
		Reader:    csvReader,
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
		if errors.Is(err, ErrInvalidMovie) {
			return nil, err
		}
		return nil, fmt.Errorf("error parsing movie: %v", err)
	}

	mr.Total++
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

func (mr *MoviesReader) ReadMovies() ([]models.RawMovie, error) {
	var movies []models.RawMovie

	for len(movies) < mr.batchSize {
		movie, err := mr.ReadMovie()
		if err != nil {
			if errors.Is(err, ErrInvalidMovie) {
				continue // drop invalid movies, don't count toward batch
			}
			return nil, err // actual error
		}
		if movie == nil {
			break // EOF
		}
		movies = append(movies, *movie)
	}

	return movies, nil
}

type ReviewReader struct {
	Finished  bool
	Reader    *csv.Reader
	file      *os.File
	batchSize int
	fields    []string
	Total     int
}

func NewReviewReader(path string, batchSize int) (*ReviewReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	csvReader := csv.NewReader(file)
	if csvReader == nil {
		return nil, fmt.Errorf("error creating CSV reader")
	}

	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	fields, err := csvReader.Read() // Read the header line
	if err != nil {
		return nil, fmt.Errorf("error reading header line: %v", err)
	}

	reader := &ReviewReader{
		Reader:    csvReader,
		file:      file,
		batchSize: batchSize,
		fields:    fields,
	}

	return reader, nil
}

func (rr *ReviewReader) ReadReview() (*models.RawReview, error) {
	expectedFields := len(rr.fields)

	if rr.Finished {
		return nil, nil
	}

	record, err := rr.Reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			rr.Finished = true
			return nil, nil
		} else {
			return nil, err
		}
	}

	// If we read a record but its field count is off, try to join with more rows.
	if len(record) != expectedFields {
		record, err = joinRecords(rr.Reader, record, expectedFields)
		if err != nil {
			return nil, err
		}
	}

	review, err := parseReview(record)
	if err != nil {
		if errors.Is(err, ErrInvalidReview) {
			return nil, err
		}
		return nil, fmt.Errorf("error parsing review: %v", err)
	}

	rr.Total++
	return review, nil
}

func (rr *ReviewReader) ReadReviews() ([]models.RawReview, error) {
	var reviews []models.RawReview

	for len(reviews) < rr.batchSize {
		review, err := rr.ReadReview()
		if err != nil {
			if errors.Is(err, ErrInvalidReview) {
				continue // drop invalid reviews, don't count toward batch
			}
			return nil, err // actual error
		}
		if review == nil {
			break // EOF
		}
		reviews = append(reviews, *review)
	}

	return reviews, nil
}
