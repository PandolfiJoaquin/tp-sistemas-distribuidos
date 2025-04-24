package communication

import (
	"encoding/json"
	"fmt"
	"net"
	"pkg/models"
)

func SendMovies(conn net.Conn, movies []models.RawMovie) error {
	batch := models.RawMovieBatch{
		Header: models.Header{
			Weight:      uint32(len(movies)),
			TotalWeight: -1,
		},
		Movies: movies,
	}

	if err := sendBatch(conn, &batch); err != nil {
		return err
	}
	return nil
}

// TODO: Merge eofs into one (with RawBatch)
func SendMovieEof(conn net.Conn, total int32) error {
	batch := models.RawMovieBatch{
		Header: models.Header{
			TotalWeight: total,
			Weight:      0,
		},
	}

	if err := sendBatch(conn, &batch); err != nil {
		return err
	}

	return nil
}

func RecvMovies(conn net.Conn) (models.RawMovieBatch, error) {
	batch, err := recvMovieBatch(conn)
	if err != nil {
		return batch, err
	}
	return batch, nil
}

func SendReviews(conn net.Conn, reviews []models.RawReview) error {
	batch := models.RawReviewBatch{
		Header: models.Header{
			Weight:      uint32(len(reviews)),
			TotalWeight: -1,
		},
		Reviews: reviews,
	}

	if err := sendBatch(conn, &batch); err != nil {
		return err
	}
	return nil
}

func SendReviewEof(conn net.Conn, total int32) error {
	batch := models.RawReviewBatch{
		Header: models.Header{
			TotalWeight: total,
			Weight:      0,
		},
	}

	if err := sendBatch(conn, &batch); err != nil {
		return err
	}

	return nil
}

func RecvReviews(conn net.Conn) (models.RawReviewBatch, error) {
	batch, err := recvReviewBatch(conn)
	if err != nil {
		return batch, err
	}
	return batch, nil
}

func SendCredits(conn net.Conn, credits []models.RawCredits) error {
	batch := models.RawCreditBatch{
		Header: models.Header{
			Weight:      uint32(len(credits)),
			TotalWeight: -1,
		},
		Credits: credits,
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}
	return nil
}

func SendCreditsEof(conn net.Conn, total int32) error {
	batch := models.RawCreditBatch{
		Header: models.Header{
			TotalWeight: total,
			Weight:      0,
		},
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}

	return nil
}

func RecvCredits(conn net.Conn) (models.RawCreditBatch, error) {
	batch, err := recvCreditBatch(conn)
	if err != nil {
		return batch, err
	}
	return batch, nil
}

func SendQueryEof(conn net.Conn, queryID int) error {
	results := models.Results{
		QueryID: queryID,
	}

	err := sendResults(conn, results)
	if err != nil {
		return fmt.Errorf("error sending query response: %w", err)
	}

	return nil
}

func SendQueryResults(conn net.Conn, queryID int, payload []models.QueryResult) error {
	itemsJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error sending query response: %w", err)
	}

	// wraps in a results struct
	results := models.Results{
		QueryID: queryID,
		Items:   itemsJSON,
	}

	err = sendResults(conn, results)
	if err != nil {
		return fmt.Errorf("error sending query response: %w", err)
	}

	return nil
}

func unmarshalSlice[T models.QueryResult](data []byte) ([]models.QueryResult, error) {
	var items []T
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, err
	}

	res := make([]models.QueryResult, len(items))
	for i, v := range items {
		res[i] = v
	}
	return res, nil
}

func RecvQueryResults(conn net.Conn) (models.TotalQueryResults, error) {
	var totalResults models.TotalQueryResults
	results, err := recvResults(conn)
	if err != nil {
		return totalResults, fmt.Errorf("error receiving query response: %w", err)
	}

	if results.Items == nil {
		return models.TotalQueryResults{
			QueryId: results.QueryID,
			Items:   nil,
		}, nil
	}

	var resultsArr []models.QueryResult
	switch results.QueryID {
	case 1:
		resultsArr, err = unmarshalSlice[models.Q1Movie](results.Items)
	case 2:
		resultsArr, err = unmarshalSlice[models.Q2Country](results.Items)
	case 3:
		resultsArr, err = unmarshalSlice[models.Q3Result](results.Items)
	case 4:
		resultsArr, err = unmarshalSlice[models.Q4Actors](results.Items)
	case 5:
		resultsArr, err = unmarshalSlice[models.Q5Avg](results.Items)
	default:
		return totalResults, fmt.Errorf("unknown query id: %d", results.QueryID)
	}

	if err != nil {
		return totalResults, fmt.Errorf("error unmarshalling query response: %w", err)
	}

	totalResults.QueryId = results.QueryID
	totalResults.Items = resultsArr
	return totalResults, nil
}

func IsQueryEof(results models.TotalQueryResults) bool {
	return len(results.Items) == 0
}
