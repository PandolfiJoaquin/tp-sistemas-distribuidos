package utils

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"pkg/models"
)

type BatchReader[T any] interface {
	ReadBatch() ([]T, error)
	Close() error
	Finished() bool
	TotalRead() int
}

type baseReader struct {
	finished  bool
	reader    *csv.Reader
	file      *os.File
	batchSize int
	fields    []string
	total     int
}

func (br *baseReader) Finished() bool {
	return br.finished
}

func (br *baseReader) Close() error {
	if br.file != nil {
		return br.file.Close()
	}
	return nil
}

func (br *baseReader) TotalRead() int {
	return br.total
}

func newBaseReader(path string, batchSize int) (*baseReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	csvReader := csv.NewReader(file)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	fields, err := csvReader.Read() // Read the header line
	if err != nil {
		return nil, fmt.Errorf("error reading header line: %v", err)
	}

	return &baseReader{
		reader:    csvReader,
		file:      file,
		batchSize: batchSize,
		fields:    fields,
	}, nil
}

type MoviesReader struct {
	*baseReader
}

func NewMoviesReader(path string, batchSize int) (BatchReader[models.RawMovie], error) {
	base, err := newBaseReader(path, batchSize)
	if err != nil {
		return nil, err
	}
	return &MoviesReader{baseReader: base}, nil
}

func (mr *MoviesReader) ReadBatch() ([]models.RawMovie, error) {
	var movies []models.RawMovie

	for len(movies) < mr.batchSize {
		movie, err := readAndParse(mr.baseReader, parseMovie)
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
	*baseReader
}

func NewReviewReader(path string, batchSize int) (BatchReader[models.RawReview], error) {
	base, err := newBaseReader(path, batchSize)
	if err != nil {
		return nil, err
	}
	return &ReviewReader{baseReader: base}, nil
}

func (rr *ReviewReader) ReadBatch() ([]models.RawReview, error) {
	var reviews []models.RawReview

	for len(reviews) < rr.batchSize {
		review, err := readAndParse(rr.baseReader, parseReview)
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

type CreditsReader struct {
	*baseReader
}

func NewCreditsReader(path string, batchSize int) (BatchReader[models.RawCredits], error) {
	base, err := newBaseReader(path, batchSize)
	if err != nil {
		return nil, err
	}
	return &CreditsReader{baseReader: base}, nil
}

func (cr *CreditsReader) ReadBatch() ([]models.RawCredits, error) {
	var credits []models.RawCredits

	for len(credits) < cr.batchSize {
		credit, err := readAndParse(cr.baseReader, parseCredits)
		if err != nil {
			if errors.Is(err, ErrInvalidCredit) {
				continue // drop invalid credits, don't count toward batch
			}
			return nil, err // actual error
		}
		if credit == nil {
			break // EOF
		}
		credits = append(credits, *credit)
	}

	return credits, nil
}

// Aux function to join records that are split across multiple lines
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

func readAndParse[T any](reader *baseReader, parseFunc func([]string) (*T, error)) (*T, error) {
	expectedFields := len(reader.fields)

	if reader.Finished() {
		return nil, nil
	}

	record, err := reader.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			reader.finished = true
			return nil, nil
		} else {
			return nil, err
		}
	}

	if len(record) != expectedFields {
		record, err = joinRecords(reader.reader, record, expectedFields)
		if err != nil {
			return nil, err
		}
	}

	parsedRecord, err := parseFunc(record)
	if err != nil {
		return nil, err
	}

	reader.total++

	return parsedRecord, nil

}
