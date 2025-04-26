package models

import "encoding/json"

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"`
}

type RawBatch[T any] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (b *RawBatch[T]) IsEof() bool {
	return b.Header.TotalWeight > 0
}

type Results struct {
	QueryID int             `json:"query_id"`
	Items   json.RawMessage `json:"items"`
}
