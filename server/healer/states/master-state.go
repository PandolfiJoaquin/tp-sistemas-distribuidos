package states

import (
	"fmt"
	"log/slog"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const heartBeatPeriod = 10 * time.Millisecond

type MasterState struct {
	config election_model.Config
}

func NewMasterState(config election_model.Config) *MasterState {
	return &MasterState{config}
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
				return NewNotACandidateState(m.config)
			} else {
				return NewMasterState(m.config)
			}
		case election_model.Ok:
			return m
		case election_model.Victory:
			slog.Warn("master received victory")
			if event.Parameter > 1 {
				return NewWorkerState(m.config)
			} else {
				//TODO: respond with victory
				return NewMasterState(m.config)
			}
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}
