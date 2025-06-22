package main

import (
	"context"
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
	election_model "tp-sistemas-distribuidos/server/healer/election-model"
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

func sendHeartbeat(heartbeatChan chan struct{}) {
	select {
	case heartbeatChan <- struct{}{}:
	default:
		slog.Error("failed to send heartbeat: channel is full")
	}
}

func (p *Process) StartProcess(ctx context.Context, heartbeatChan chan struct{}) {
	go p.listener(ctx)
	go p.connector(ctx)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	slog.Info(p.ProcState.GetName())
	for p.running {
		select {
		case <-ctx.Done():
			slog.Info("context done")
			return
		case <-ticker.C:
		default:
			oldState := p.ProcState.GetName()
			p.ProcState = p.ProcState.HandleMailBox(p.mailbox, p.peers, ctx)
			if p.ProcState == nil {
				return
			}
			if p.ProcState.GetName() != oldState {
				slog.Info(p.ProcState.GetName())
			}
		}
		sendHeartbeat(heartbeatChan)
	}
	//p.ProcState.Close()
	//TODO: the defere stop is enough? if yes then remove close from interface and states
}

func (p *Process) listener(ctx context.Context) {
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
		go func(p *Process) {
			id, err := getServiceId(conn)
			if err != nil {
				slog.Error("error getting service name", slog.Any("error", err))
				wrapDeferredError(conn)
			}
			slog.Info("Accepted connection", slog.Int("id", id))
			peerMailBox := p.startPeer(conn, id, ctx)
			p.peers.SafeAddPeer(id, peerMailBox)
		}(p)
	}
}

func (p *Process) connector(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			for id := p.config.Id + 1; id <= p.config.AmtOfHealers; id++ {
				if p.peers.SafeContains(id) {
					continue
				}
				conn, err := net.DialTimeout("tcp", "healer-"+strconv.Itoa(id)+":1234", 1*time.Second)
				if err != nil {
					slog.Debug("error connecting to healer", slog.Any("error", err))
					continue
				}
				if err := sendServiceID(conn, uint16(p.config.Id)); err != nil {
					slog.Error("error sending healer", slog.Any("error", err))
					if err := conn.Close(); err != nil {
						slog.Error("error closing connection", slog.Any("error", err))
					}
					return
				}
				peerMailbox := p.startPeer(conn, id, ctx)
				p.peers.SafeAddPeer(id, peerMailbox)
			}
		}
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
