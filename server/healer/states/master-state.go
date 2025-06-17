package states

import (
	"fmt"
	"log/slog"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const heartBeatPeriod = 10 * time.Millisecond

type MasterState struct{}

func NewMasterState() *MasterState {
	return &MasterState{}
}

func (m *MasterState) HandleMailBox(mailbox chan election_model.Event) ProcessState {
	ticker := time.NewTicker(heartBeatPeriod)
	select {
	case <-ticker.C:
		//TODO: broadcast heartbeat
		return m
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			//TODO
			slog.Warn("heartbeat received by master")
			return m
		case election_model.Election:
			slog.Warn("election received by master", slog.Int("election_id", event.Parameter))
			if event.Parameter > 1 {
				return NewNotACandidateState()
			} else {
				return NewMasterState()
			}
		case election_model.Ok:
			return m
		case election_model.Victory:
			slog.Warn("master received victory")
			if event.Parameter > 1 {
				return NewWorkerState()
			} else {
				//TODO: respond with victory
				return NewMasterState()
			}
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}
