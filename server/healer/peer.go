package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"pkg/communication"
	electionmodel "tp-sistemas-distribuidos/server/healer/election-model"
)

const EventLenBytes = 4

func (p *Process) startPeer(conn net.Conn, peerId int, ctx context.Context) chan electionmodel.Event {
	peerMailbox := make(chan electionmodel.Event)
	go p.startSender(conn, peerMailbox, peerId, ctx)
	go p.startReceiver(conn, peerId) //sender will close its connection so no need to pass it a context
	return peerMailbox
}

func (p *Process) startSender(conn net.Conn, mailbox chan electionmodel.Event, id int, ctx context.Context) {
	defer func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			slog.Error(err.Error())
		}
	}(conn)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-mailbox:
			serialized, err := json.Marshal(event)
			if err != nil {
				slog.Error("Error serializing event", err)
				return
			}

			serializedLen := make([]byte, EventLenBytes)
			binary.BigEndian.PutUint32(serializedLen, uint32(len(serialized)))
			if err := communication.SendAll(conn, serializedLen); err != nil {
				if errors.Is(err, net.ErrClosed) {
					p.peers.SafeRemovePeer(id)
					return
				} else {
					return
				}
			}
			if err := communication.SendAll(conn, serialized); err != nil {
				if errors.Is(err, net.ErrClosed) {
					p.peers.SafeRemovePeer(id)
					return
				} else {
					slog.Error("Error sending event:", err)
					return
				}
			}
		}
	}
}

func (p *Process) startReceiver(conn net.Conn, id int) {
	for p.running {
		event, err := p.RecvEvent(conn)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				p.peers.SafeRemovePeer(id)
				return
			} else {
				slog.Error("Error receiving event:", slog.Any("error", err))
				p.peers.SafeRemovePeer(id)
				return
			}
		}
		p.mailbox <- event
	}
}

func (p *Process) RecvEvent(conn net.Conn) (electionmodel.Event, error) {
	rawLen, err := communication.RecvAll(conn, EventLenBytes)
	if err != nil {
		return electionmodel.Event{}, fmt.Errorf("error receiving event len: %w", err)
	}

	eventLen := binary.BigEndian.Uint32(rawLen)
	rawEvent, err := communication.RecvAll(conn, int(eventLen))
	if err != nil {
		return electionmodel.Event{}, fmt.Errorf("error receiving event: %w", err)
	}
	event := electionmodel.Event{}
	if err := json.Unmarshal(rawEvent, &event); err != nil {
		slog.Error("Error unmarshaling event:", err)
	}
	return event, nil
}
