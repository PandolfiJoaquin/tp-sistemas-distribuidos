package states

import (
	"fmt"
	"log/slog"
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const (
	heartBeatTolerance = 5 * time.Second
)

type WorkerState struct {
	config election_model.Config
}

func NewWorkerState(config election_model.Config) *WorkerState {
	return &WorkerState{config}
}

func (w *WorkerState) HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers) ProcessState {
	//TODO
	ticker := time.NewTicker(heartBeatTolerance)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		slog.Info("Timeout, converting to candidate")
		return NewCandidateState(w.config, peers)
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			slog.Info("HeartBeat")
			return w
		case election_model.Election:
			if event.Parameter > w.config.Id {
				return NewNotACandidateState(w.config)
			} else {
				peers.SendToId(election_model.Event{Type: election_model.Ok, Parameter: w.config.Id}, event.Parameter)
				return NewCandidateState(w.config, peers)
			}
		case election_model.Ok:
			//TODO: mensaje viejo, ignoro, pero que hago si algun otro de los mensaes es viejo? revisar.
			slog.Info("Received Ok event as a worker")
			return w
		case election_model.Victory:
			//TODO: mensaje viejo, ignoro, pero que hago si algun otro de los mensaes es viejo? revisar.
			slog.Info("Received Victory event as a worker", slog.Any("event.Parameter", event.Parameter))
			return w
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")

}

func (w *WorkerState) GetName() string {
	return "worker-state"
}
