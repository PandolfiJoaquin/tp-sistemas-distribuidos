package models

import "encoding/json"

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"`
}

type RawMovieBatch struct {
	Header Header     `json:"header"`
	Movies []RawMovie `json:"movies_raw,omitempty"`
}

type Results struct {
	QueryID int             `json:"query_id"`
	Items   json.RawMessage `json:"items"`
}

func (b *RawMovieBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}
