package common

import (
	"fmt"
	"strconv"
	"strings"
)

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"` //-1 if its uknown for the moment
	ClientID    string `json:"client_id"`
}

type Batch[T any] struct {
	Header `json:"header"`
	Data   []T `json:"data"`
}

func (h *Header) IsEof() bool {
	return h.TotalWeight > 0
}

func (h *Header) GetClientID() string {
	return h.ClientID
}

func (h *Header) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%s,%d,%d",h.ClientID, h.Weight, h.TotalWeight), nil
}

func (h *Header) UnmarshalText(text []byte) error {
	parts := strings.Split(string(text), ",")
	if len(parts) != 3 {
		return fmt.Errorf("invalid header format")
	}
	
	h.ClientID = parts[0]
	weight, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid weight: %w", err)
	}

	totalWeight, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid total weight: %w", err)
	}

	h.Weight = uint32(weight)
	h.TotalWeight = int32(totalWeight)
	return nil
}