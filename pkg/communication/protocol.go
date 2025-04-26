package communication

import (
	"encoding/json"
	"fmt"
	"net"
	"pkg/models"
)

func SendBatchEOF(conn net.Conn, total int32) error {
	batch := models.RawBatch[any]{ //Doesn't matter type as it is empty
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

func SendData[T any](conn net.Conn, data []T) error {
	batch := models.RawBatch[T]{
		Header: models.Header{
			Weight:      uint32(len(data)),
			TotalWeight: -1,
		},
		Data: data,
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}
	return nil
}

func RecvBatch[T any](conn net.Conn) (models.RawBatch[T], error) {
	var batch models.RawBatch[T]
	var err error

	batch, err = recvBatch[T](conn)
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

// SendQueryResults TODO: Do generics in results
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
