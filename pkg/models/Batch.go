package models

import "encoding/json"

type RawBatch interface {
	IsEof() bool
}

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"`
}

type RawMovieBatch struct {
	Header Header     `json:"header"`
	Movies []RawMovie `json:"movies_raw,omitempty"`
}

func (b RawMovieBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}

type RawReviewBatch struct {
	Header  Header      `json:"header"`
	Reviews []RawReview `json:"reviews_raw,omitempty"`
}

func (b RawReviewBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}

type RawCreditBatch struct {
	Header  Header       `json:"header"`
	Credits []RawCredits `json:"credits_raw,omitempty"`
}

func (b RawCreditBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}

type Results struct {
	QueryID int             `json:"query_id"`
	Items   json.RawMessage `json:"items"`
}
