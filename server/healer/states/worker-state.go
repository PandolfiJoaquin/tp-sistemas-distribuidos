package states

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const (
	heartBeatTolerance = 2 * time.Second
)

type WorkerState struct {
	config election_model.Config
}

func NewWorkerState(config election_model.Config) *WorkerState {
	return &WorkerState{config}
}

func (w *WorkerState) HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers, ctx context.Context) ProcessState {
	select {
	case <-ctx.Done():
		slog.Info("Context cancelled")
		return nil
	case <-time.After(heartBeatTolerance):
		slog.Info("Timeout, converting to candidate")
		return NewCandidateState(w.config, peers)
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			return w
		case election_model.Election:
			if event.Parameter > w.config.Id {
				slog.Info("Received Election event as a worker (Event.id > config.id), becoming not a candidate")
				return NewNotACandidateState(w.config)
			} else {
				slog.Info("Received Election event as a worker (Event.id <= config.id), sending Ok")
				peers.SendToId(election_model.Event{Type: election_model.Ok, Parameter: w.config.Id}, event.Parameter)
				return NewCandidateState(w.config, peers)
			}
		case election_model.Ok:
			slog.Info("Received Ok event as a worker")
			return w
		case election_model.Victory:
			slog.Info("Received Victory event as a worker", slog.Any("event.Parameter", event.Parameter))
			return w
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")

}

func (w *WorkerState) Close() {}

func (w *WorkerState) GetName() string {
	return "worker-state"
}
