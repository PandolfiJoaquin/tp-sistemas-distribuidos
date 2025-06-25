package communication

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"pkg/models"
	"sync"
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

func RecvBatch[T any](conn net.Conn, connMutex *sync.Mutex) (models.RawBatch[T], error) {
	var batch models.RawBatch[T]
	var err error

	slog.Debug("Receiving batch", slog.String("type", fmt.Sprintf("%T", batch.Data)), slog.Int("batch_id", batch.Header.BatchID))
	batch, err = recvBatch[T](conn)
	if err != nil {
		return batch, err
	}

	connMutex.Lock()
	defer connMutex.Unlock()
	slog.Debug("Received batch", slog.String("type", fmt.Sprintf("%T", batch.Data)), slog.Int("batch_id", batch.Header.BatchID), slog.Int("weight", int(batch.Header.Weight)), slog.Int("total_weight", int(batch.Header.TotalWeight)))
	if err := sendAck(conn, int(batch.Header.Weight)); err != nil {
		return batch, fmt.Errorf("error sending ack: %w", err)
	}

	return batch, nil
}

func SendQueryResults(conn net.Conn, results models.TotalQueryResults) error {
	itemsJson, err := json.Marshal(results.Items)
	if err != nil {
		return fmt.Errorf("error marshalling query results: %w", err)
	}

	rawResults := models.RawQueryResults{
		QueryId: results.QueryId,
		Items:   itemsJson,
		Last:    results.Last,
		Header:  results.Header,
	}

	err = sendResults(conn, rawResults)
	if err != nil {
		return fmt.Errorf("error sending query responose: %w", err)
	}

	return nil
}

func unmarshalSlice[T models.QueryResult](data []byte) ([]models.QueryResult, error) {
	if data == nil {
		return nil, nil
	}
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

	var resultsArr []models.QueryResult
	// Must unmarshal the items to the correct type
	switch results.QueryId {
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
		return totalResults, fmt.Errorf("unknown query id: %d", results.QueryId)
	}

	if err != nil {
		return totalResults, fmt.Errorf("error unmarshalling query response: %w", err)
	}

	totalResults.QueryId = results.QueryId
	totalResults.Items = resultsArr
	totalResults.Last = results.Last
	totalResults.Header = results.Header
	return totalResults, nil
}

func RecvAck(conn net.Conn) (int, error) {
	acked, err := recvAck(conn)
	if err != nil {
		return -1, err
	}

	return acked, nil
}

func RecvTypeOfResults(conn net.Conn) (int, error) {
	resType, err := recvTypeOfResults(conn)
	if err != nil {
		return -1, err
	}

	if resType != QueryMsg && resType != AckMsg {
		return -1, fmt.Errorf("invalid response type: %d", resType)
	}

	return resType, nil
}
