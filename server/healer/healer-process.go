package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"pkg/communication"
	"strconv"
	"sync"
	"time"
	"tp-sistemas-distribuidos/server/healer/election-model"
	"tp-sistemas-distribuidos/server/healer/states"
)

// Process Represents a process. More specifically, a master or a worker.
type Process struct {
	peers     map[int]chan election_model.Event
	mutex     *sync.RWMutex
	ProcState states.ProcessState
	running   bool
	config    election_model.Config
	mailbox   chan election_model.Event
}

func NewProcess() (*Process, error) {
	//todo: leer aca los archivos y variables de entorno necesarios

	id, err := strconv.Atoi(os.Getenv("HEALER_ID"))
	if err != nil {
		return nil, fmt.Errorf("error getting process id: %w", err)
	}

	amtOfHealers, err := strconv.Atoi(os.Getenv("HEALERS_AMOUNT"))
	if err != nil {
		return nil, fmt.Errorf("error getting healers amount: %w", err)
	}

	config := election_model.Config{
		Id:           id,
		AmtOfHealers: amtOfHealers,
	}
	mailbox := make(chan election_model.Event)
	return &Process{
		peers:     make(map[int]chan election_model.Event),
		ProcState: states.NewWorkerState(config),
		running:   true,
		config:    config,
		mailbox:   mailbox,
	}, nil
}

func (p *Process) StartProcess(id, totalHealers int) {

	go p.listener()
	go p.connector()

	for p.running {
		p.ProcState = p.ProcState.HandleMailBox(p.mailbox)

	}
}

func (p *Process) listener() {
	listener, err := net.Listen("tcp", "0.0.0.0:"+"1234")
	defer wrapDeferredError(listener)
	if err != nil {
		slog.Error("Error starting listener")
		return
	}
	for p.running {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("error accepting connection", slog.Any("error", err))
		}

		id, err := getServiceId(conn)
		if err != nil {
			slog.Error("error getting service name", slog.Any("error", err))
			wrapDeferredError(conn)
		}
		peerMailBox := p.startPeer(conn, id)
		p.SafeAddPeer(id, peerMailBox)
	}
}

func (p *Process) SafeAddPeer(id int, mailbox chan election_model.Event) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.peers[id] = mailbox
}

func (p *Process) SafeRemovePeer(id int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.peers, id)
}

func (p *Process) SafeContains(id int) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	_, ok := p.peers[id]
	return ok
}

func (p *Process) connector() {
	for p.running {
		for id := 0; id < p.config.AmtOfHealers; id++ {
			if id == p.config.Id {
				continue
			}
			if p.SafeContains(id) {
				continue
			}
			slog.Info("attemping to connecto to peer", slog.Int("peer", id))
			conn, err := net.DialTimeout("tcp", "healer-"+strconv.Itoa(id)+":1234", 1*time.Second)
			if err != nil {
				slog.Debug("error connecting to healer", slog.Any("error", err))
				continue
			}
			peerMailbox := p.startPeer(conn, id)
			p.SafeAddPeer(id, peerMailbox)

		}
		time.Sleep(4 * time.Second)
	}

}

func wrapDeferredError(conn io.Closer) {
	if err := conn.Close(); err != nil {
		slog.Error("Error closing conn", slog.Any("error", err))
	}
}

func getServiceId(conn net.Conn) (int, error) {
	rawData, err := communication.RecvAll(conn, 2)
	if err != nil {
		return 0, fmt.Errorf("error receiving service name: %w", err)
	}
	id := binary.BigEndian.Uint16(rawData[0:2])
	return int(id), nil
}

func sendServiceID(conn net.Conn, id uint16) error {
	buffer := make([]byte, 2)
	binary.BigEndian.PutUint16(buffer, id)
	_, err := conn.Write(buffer)
	return err
}
