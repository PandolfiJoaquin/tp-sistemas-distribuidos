package states

import (
	"fmt"
	"log/slog"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const timeToWInElection = 2 * time.Second

type CandidateState struct {
	config election_model.Config
}

func NewCandidateState(config election_model.Config) *CandidateState {
	return &CandidateState{config}
}

func (c *CandidateState) HandleMailBox(mailbox chan election_model.Event) ProcessState {

	ticker := time.NewTicker(timeToWInElection)
	select {
	case <-ticker.C:
		//TODO broadcast election winning. or in master constructor
		return NewMasterState(c.config)
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			return c
		case election_model.Election:
			if event.Parameter > 1 /*config.id*/ {
				return NewNotACandidateState(c.config)
			} else {
				//tengo que enviar aca mensaje de eleccion? Tambien tengo que responder OK
				// o muevo el broadcast de mensaje de eleccion a cuando se crea el candidate?
				return NewCandidateState(c.config)
			}
		case election_model.Ok:
			return NewNotACandidateState(c.config)
		case election_model.Victory:
			return NewWorkerState(c.config)
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}
