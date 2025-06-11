package models

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"`
	BatchID     int    `json:"batch_id"`
}

type RawBatch[T any] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (b *RawBatch[T]) IsEof() bool {
	return b.Header.TotalWeight > 0
}
