package states

import (
	"fmt"
	"log/slog"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

type NotACandidateState struct{}

func NewNotACandidateState() *NotACandidateState {
	return &NotACandidateState{}
}

func (n *NotACandidateState) HandleMailBox(mailbox chan election_model.Event) ProcessState {
	ticker := time.NewTicker(1 * time.Second)
	select {
	case <-ticker.C:
		slog.Warn("a non-candidate process time'd out. Becoming candidate again")
		return NewCandidateState()
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			slog.Info("received heartbeat event. ignoring")
			return n
		case election_model.Election:
			return n
		case election_model.Ok:
			return n
		case election_model.Victory:
			return NewWorkerState()
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}
