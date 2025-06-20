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
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
	"tp-sistemas-distribuidos/server/healer/states"
)

// Process Represents a process. More specifically, a master or a worker.
type Process struct {
	peers     *concurrencyutils.Peers
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
		peers:     concurrencyutils.NewPeers(),
		ProcState: states.NewWorkerState(config),
		running:   true,
		config:    config,
		mailbox:   mailbox,
	}, nil
}

func (p *Process) StartProcess() {
	delay, err := strconv.Atoi(os.Getenv("DELAY"))
	if err != nil {
		slog.Error("Error converting DELAY env var to int", slog.String("error", err.Error()))
		return
	}

	go p.listener()
	go p.connector()

	time.Sleep(time.Duration(delay) * time.Second)
	slog.Info(p.ProcState.GetName())
	for p.running {
		oldState := p.ProcState.GetName()
		p.ProcState = p.ProcState.HandleMailBox(p.mailbox, p.peers)
		if p.ProcState.GetName() != oldState {
			slog.Info(p.ProcState.GetName())
		}

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
		go func(p *Process) {
			if err != nil {
				slog.Error("error accepting connection", slog.Any("error", err))
			}

			id, err := getServiceId(conn)
			if err != nil {
				slog.Error("error getting service name", slog.Any("error", err))
				wrapDeferredError(conn)
			}
			peerMailBox := p.startPeer(conn, id)
			p.peers.SafeAddPeer(id, peerMailBox)
		}(p)
	}
}

func (p *Process) connector() {
	for p.running {

		for id := p.config.Id + 1; id <= p.config.AmtOfHealers; id++ {
			if p.peers.SafeContains(id) {
				continue
			}
			slog.Info("attemping to connecto to peer", slog.Int("peer", id))
			conn, err := net.DialTimeout("tcp", "healer-"+strconv.Itoa(id)+":1234", 1*time.Second)
			if err != nil {
				slog.Debug("error connecting to healer", slog.Any("error", err))
				continue
			}
			slog.Debug("connected to peer", slog.Any("peer", id))
			if err := sendServiceID(conn, uint16(id)); err != nil {
				return
			}
			peerMailbox := p.startPeer(conn, id)
			p.peers.SafeAddPeer(id, peerMailbox)
		}
		time.Sleep(3 * time.Second)
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
