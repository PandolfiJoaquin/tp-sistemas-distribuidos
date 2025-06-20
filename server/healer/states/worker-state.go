package states

import (
	"fmt"
	"log/slog"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const (
	heartBeatTolerance = 3 * time.Second
)

type WorkerState struct {
	config election_model.Config
}

func NewWorkerState(config election_model.Config) *WorkerState {
	return &WorkerState{config}
}

func (w *WorkerState) HandleMailBox(mailbox chan election_model.Event) ProcessState {
	//TODO
	ticker := time.NewTicker(heartBeatTolerance)
	select {
	case <-ticker.C:
		//broadcast mensaje de eleccion
		return NewCandidateState(w.config)
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			return w
		case election_model.Election:
			if event.Parameter > 1 /*config.id*/ {
				//answer with ok to the specific client
				return NewNotACandidateState(w.config)
			} else {
				//tengo que enviar aca mensaje de eleccion?
				// o muevo el broadcast de mensaje de eleccion a cuando se crea el candidate?
				return NewCandidateState(w.config)
			}
		case election_model.Ok:
			//TODO: mensaje viejo, ignoro, pero que hago si algun otro de los mensaes es viejo? revisar.
			fallthrough
		case election_model.Victory:
			//TODO: mensaje viejo, ignoro, pero que hago si algun otro de los mensaes es viejo? revisar.
			return w
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")

}
