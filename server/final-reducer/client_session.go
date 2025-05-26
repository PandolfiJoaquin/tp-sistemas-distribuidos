package main

type ClientSession struct {
	CurrentWeight uint32 `json:"current_weight"`
	EofWeight     int32  `json:"eof_weight"`
	EofMultiplier uint32 `json:"eof_multiplier"`
	SessionId     string `json:"session_id"`
	Data          any    `json:"Data"`
}

func NewClientSession(sessionId string, eofMultiplier uint32) *ClientSession {
	return &ClientSession{
		SessionId:     sessionId,
		EofMultiplier: eofMultiplier,
	}
}

func (c *ClientSession) SetData(data any) {
	c.Data = data
}

func (c *ClientSession) GetData() any {
	return c.Data
}

func (c *ClientSession) AddCurrentWeight(weight uint32) {
	c.CurrentWeight += weight
}

func (c *ClientSession) SetEofWeight(weight int32) {
	c.EofWeight = weight
}

func (c *ClientSession) GetEofWeight() int32 {
	return c.EofWeight
}

func (c *ClientSession) IsFinished() bool {
	return c.EofWeight > 0 && c.CurrentWeight == (uint32(c.EofWeight)*c.EofMultiplier)
}
