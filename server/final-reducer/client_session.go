package main

import (
	// pkg "pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

type ClientSession struct {
	CurrentWeight uint32                                 `json:"current_weight"`
	EofWeight     int32                                  `json:"eof_weight"`
	EofMultiplier uint32                                 `json:"eof_multiplier"`
	SessionId     string                                 `json:"session_id"`
	Q2Data        map[string]uint64                      `json:"data_q2"`
	Q3Data        map[string]common.MovieAvgRating       `json:"data_q3"`
	Q4Data        map[string]common.ActorMoviesAmount    `json:"data_q4"`
	Q5Data        common.SentimentProfitRatioAccumulator `json:"data_q5"`
	Filters       *common.DuplicateFilterWithShards      `json:"filters"`
}

func NewClientSession(sessionId string, eofMultiplier uint32) *ClientSession {
	return &ClientSession{
		SessionId:     sessionId,
		EofMultiplier: eofMultiplier,
		Filters:       common.NewDuplicateFilterWithShards(),
		Q2Data:        make(map[string]uint64),
		Q3Data:        make(map[string]common.MovieAvgRating),
		Q4Data:        make(map[string]common.ActorMoviesAmount),
	}
}

func (c *ClientSession) FilterMsg(id int, shardID int) bool {
	return c.Filters.Accept(id, shardID)
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
