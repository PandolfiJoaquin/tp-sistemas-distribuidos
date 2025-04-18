package models

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"`
}

type Ack struct {
	Read int `json:"read"`
}

type RawMovieBatch struct {
	Header Header     `json:"header"`
	Movies []RawMovie `json:"movies_raw,omitempty"`
}

func (b *RawMovieBatch) IsEof() bool {
	return b.Header.TotalWeight > 0
}
