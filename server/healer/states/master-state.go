package states

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
	healer_utils "tp-sistemas-distribuidos/server/healer/healer-utils"
)

const heartBeatPeriod = 100 * time.Millisecond

type MasterState struct {
	config election_model.Config
}

func NewMasterState(config election_model.Config, ctx context.Context) *MasterState {
	toMonitor, err := healer_utils.ReadContainerToMonitor("healer-" + strconv.Itoa(config.Id))
	if err != nil {
		slog.Error("Error reading YAML file", slog.String("error", err.Error()))
	}

	slog.Info("Containers to monitor", slog.Any("containers", toMonitor))

	healer := healer_utils.NewHealer(toMonitor)
	healer.Start(ctx)
	return &MasterState{config}
}

func (m *MasterState) HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers, ctx context.Context) ProcessState {
	select {
	case <-ctx.Done():
		slog.Info("Context cancelled")
		return nil
	case <-time.After(heartBeatPeriod):
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
				return m
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
				return m
			}
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
}

func (m *MasterState) Close() {

}

func (m *MasterState) GetName() string {

	return "master-state"
}
