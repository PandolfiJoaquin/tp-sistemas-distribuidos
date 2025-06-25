package states

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

const timeToWinElection = 2 * time.Second

type CandidateState struct {
	config election_model.Config
}

func NewCandidateState(config election_model.Config, peers *concurrencyutils.Peers) *CandidateState {
	peers.Broadcast(election_model.Event{Type: election_model.Election, Parameter: config.Id})
	return &CandidateState{config}
}

func (c *CandidateState) HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers, ctx context.Context) ProcessState {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(timeToWinElection):
		slog.Info("Timeout, converting to master")
		peers.Broadcast(election_model.Event{Type: election_model.Victory, Parameter: c.config.Id})
		return NewMasterState(c.config, ctx)
	case event := <-mailbox:
		switch event.Type {
		case election_model.HeartBeat:
			slog.Debug("HeartBeat received by candidate")
			return c
		case election_model.Election:
			if event.Parameter > c.config.Id {
				slog.Info("Candidate received Election (Event.id > config.id)")
				return NewNotACandidateState(c.config)
			} else {
				peers.SendToId(election_model.Event{Type: election_model.Ok, Parameter: c.config.Id}, event.Parameter)
				return c
			}
		case election_model.Ok:
			slog.Info("Candidate received Ok event")
			return NewNotACandidateState(c.config)
		case election_model.Victory:
			slog.Warn(
				"candidate received election victory",
				slog.Int("new_leader_id:", event.Parameter),
			)
			return NewWorkerState(c.config)
		default:
			panic(fmt.Sprintf("Unknown event: %v", event.Type))
		}
	}
	slog.Info("unreachable")
	panic("unreachable")
}

func (c *CandidateState) Close() {}

func (c *CandidateState) GetName() string {

	return "candidate-state"
}
