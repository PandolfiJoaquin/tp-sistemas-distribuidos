package main

import (
	"sync"
	"tp-sistemas-distribuidos/server/healer/election-model"
	"tp-sistemas-distribuidos/server/healer/states"
)

// Process Represents a process. More specifically, a master or a worker.
type Process struct {
	peers     []Peer
	mutex     *sync.Mutex
	ProcState states.ProcessState
}

func NewProcess() Process {
	//creo mutex y lista vacia
	//leo todas las variables de entorno y archivos necesarios para correr
	//Creo el ProcessState Worker
	return Process{
		nil,
		nil,
		states.NewWorkerState(),
	}
}

func (p *Process) SetState(newState states.ProcessState) {
	p.ProcState = newState
}

func (p *Process) StartProcess(id, totalHealers int) {
	/*
		lanzo el listener con referencia a lista protegida. este listener settea con timeout todos los reads
		inicio el ticker
	*/

	mailbox := make(chan election_model.Event)
	for {
		p.ProcState = p.ProcState.HandleMailBox(mailbox)

	}
}
