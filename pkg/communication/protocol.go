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
			Weight: uint32(len(movies)),
		},
		Movies: movies,
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}
	return nil
}

func SendEOF(conn net.Conn, total int32) error {
	batch := models.RawMovieBatch{
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

func RecvMovies(conn net.Conn) (models.RawMovieBatch, error) {
	batch, err := recvBatch(conn)
	if err != nil {
		return batch, err
	}
	return batch, nil
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

func RecvQueryResults(conn net.Conn) ([]models.QueryResult, error) {
	results, err := recvResults(conn)
	if err != nil {
		return nil, fmt.Errorf("error receiving query response: %w", err)
	}

	switch results.QueryID {
	case 1:
		return unmarshalSlice[models.Q1Movie](results.Items)
	case 2:
		return unmarshalSlice[models.Q2Country](results.Items)
	case 3:
		return unmarshalSlice[models.Q3Movie](results.Items)
	case 4:
		return unmarshalSlice[models.Q4Actors](results.Items)
	case 5:
		return unmarshalSlice[models.Q5Avg](results.Items)
	default:
		return nil, fmt.Errorf("unknown query id: %d", results.QueryID)
	}
}
