package common

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const headerSep = ","

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"` //-1 if its uknown for the moment
	ClientID    string `json:"client_id"`
	MessageID   int    `json:"message_id"`
}

type Batch[T any] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

type LoggableBatch[T Stringer] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (b LoggableBatch[T]) ToString() string {
	// serializedData := Map(b.Data, func(m T) string {
	// 	return m.ToString()
	// })
	// return strings.Join(serializedData, "¬.¬")
	json, err := json.Marshal(b)
	if err != nil {
		return ""
	}
	return string(json)
}

func GetBatchFromString[T Stringer](s string) (LoggableBatch[T], error) {
	var data LoggableBatch[T]
	err := json.Unmarshal([]byte(s), &data)
	if err != nil {
		return LoggableBatch[T]{}, err
	}
	return data, nil
}

func (b LoggableBatch[T]) AsBatch() Batch[T] {
	return Batch[T]{
		Header: b.Header,
		Data:   b.Data,
	}
}

func (h *Header) IsEof() bool {
	return h.TotalWeight > 0
}

func (h *Header) GetClientID() string {
	return h.ClientID
}

func (h *Header) ToString() string {

	return h.ClientID + headerSep + strconv.Itoa(int(h.Weight)) + headerSep + string(h.TotalWeight)
}

func HeaderFromString(args string) (Header, error) {
	parts := strings.Split(args, headerSep)
	if len(parts) != 3 {
		return Header{}, fmt.Errorf("invalid header format: %s", args)
	}
	weight, err := strconv.Atoi(parts[1])
	if err != nil {
		return Header{}, fmt.Errorf("invalid weight in header: %w, args: %s", err, parts[1])
	}
	totalWeight, err := strconv.Atoi(parts[2])
	if err != nil {
		return Header{}, fmt.Errorf("invalid totalWeight in header: %w, args: %s", err, parts[2])
	}

	return Header{
		ClientID:    parts[0],
		Weight:      uint32(weight),
		TotalWeight: int32(totalWeight),
	}, nil
}
