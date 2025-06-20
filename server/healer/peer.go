package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"pkg/communication"
	election_model "tp-sistemas-distribuidos/server/healer/election-model"
)

func (p *Process) startPeer(conn net.Conn, peerId int) chan election_model.Event {
	peerMailbox := make(chan election_model.Event)
	go p.startSender(conn, peerMailbox, peerId)
	go p.startReceiver(conn, peerId)
	return peerMailbox
}

func (p *Process) startSender(conn net.Conn, mailbox chan election_model.Event, id int) {
	for {
		event := <-mailbox
		serialized, err := json.Marshal(event)
		if err != nil {
			log.Println("Error serializing event:", err)
		}
		serializedLen := make([]byte, 4)
		binary.BigEndian.PutUint32(serializedLen, uint32(len(serialized)))
		if err := communication.SendAll(conn, serializedLen); err != nil {
			if errors.Is(err, net.ErrClosed) {
				p.SafeRemovePeer(id)
			}
			slog.Error("Error sending event len:", err)
		}
		if err := communication.SendAll(conn, serialized); err != nil {
			slog.Error("Error sending event:", err)
		}
	}
}

func (p *Process) startReceiver(conn net.Conn, id int) {
	for {
		event, err := p.RecvEvent(conn)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				p.SafeRemovePeer(id)
			}
		}
		p.mailbox <- event
	}
}

func (p *Process) RecvEvent(conn net.Conn) (election_model.Event, error) {
	rawLen, err := communication.RecvAll(conn, 4)
	if err != nil {
		return election_model.Event{}, fmt.Errorf("error receiving event len: %w", err)
	}

	eventLen := binary.BigEndian.Uint32(rawLen)
	rawEvent, err := communication.RecvAll(conn, int(eventLen))
	if err != nil {
		return election_model.Event{}, fmt.Errorf("error receiving event: %w", err)
	}
	event := election_model.Event{}
	if err := json.Unmarshal(rawEvent, &event); err != nil {
		slog.Error("Error unmarshaling event:", err)
	}
	return event, nil

}
