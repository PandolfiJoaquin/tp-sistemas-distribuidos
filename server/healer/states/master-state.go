package states

import (
	"fmt"
	"log/slog"
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const heartBeatPeriod = 100 * time.Millisecond

type MasterState struct {
	config election_model.Config
}

func NewMasterState(config election_model.Config) *MasterState {

	return &MasterState{config}
}

func (m *MasterState) HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers) ProcessState {
	ticker := time.NewTicker(heartBeatPeriod)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		peers.Broadcast(election_model.Event{Type: election_model.HeartBeat, Parameter: m.config.Id})
		return m
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
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
			slog.Warn("OK received by master", slog.Int("sender id", event.Parameter))
			return m
		case election_model.Victory:
			slog.Warn("master received victory")
			if event.Parameter > m.config.Id {
				return NewWorkerState(m.config)
			} else {
				peers.SendToId(election_model.Event{Type: election_model.Victory, Parameter: m.config.Id}, event.Parameter)
				return NewMasterState(m.config)
			}
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}

func (m *MasterState) GetName() string {

	return "master-state"
}
