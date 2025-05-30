package common

import (
	"strconv"
	"strings"
)

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"` //-1 if its uknown for the moment
	ClientID    string `json:"client_id"`
}

type Batch[T Stringer] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (b Batch[T]) GetDataAsString() string {
	serializedData := Map(b.Data, func(m T) string {
		return m.ToString()
	})
	return strings.Join(serializedData, "¬.¬")
}

func (h *Header) IsEof() bool {
	return h.TotalWeight > 0
}

func (h *Header) GetClientID() string {
	return h.ClientID
}

func (h *Header) ToString() string {
	return h.ClientID + "," + strconv.Itoa(int(h.Weight)) + "," + string(h.TotalWeight)
}
