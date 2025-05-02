package main

type ClientSession struct {
	currentWeight  uint32
	eofWeight      int32
	eofMultiplier uint32
	sessionId      string
	data           any
}

func NewClientSession(sessionId string, eofMultiplier uint32) *ClientSession {
	return &ClientSession{
		sessionId:      sessionId,
		eofMultiplier: eofMultiplier,
	}
}

func (c *ClientSession) SetData(data any) {
	c.data = data
}

func (c *ClientSession) GetData() any {
	return c.data
}

func (c *ClientSession) AddCurrentWeight(weight uint32) {
	c.currentWeight += weight
}

func (c *ClientSession) SetEofWeight(weight int32) {
	c.eofWeight = weight
}

func (c *ClientSession) GetEofWeight() int32 {
	return c.eofWeight
}

func (c *ClientSession) IsFinished() bool {
	return c.eofWeight > 0 && c.currentWeight == (uint32(c.eofWeight) * c.eofMultiplier)
}
