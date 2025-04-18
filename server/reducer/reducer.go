package main

import (
	"fmt"
	"tp-sistemas-distribuidos/server/common"
)

const (
	previousQueueQuery2 = "q2-to-reduce"
	previousQueueQuery3 = "q3-to-reduce"
	previousQueueQuery4 = "q4-to-reduce"
	previousQueueQuery5 = "q5-to-reduce"

	nextQueueQuery2 = "q2-to-final-reduce"
	nextQueueQuery3 = "q3-to-final-reduce"
	nextQueueQuery4 = "q4-to-final-reduce"
	nextQueueQuery5 = "q5-to-final-reduce"
)

type Reducer struct {
	middleware *common.Middleware
	connections map[string]connection
}

type connection struct {
	ChanToRecv <-chan common.Message
	ChanToSend chan<- []byte
}

func NewReducer(rabbitUser, rabbitPass string) (*Reducer, error) {
	middleware, err := common.NewMiddleware(rabbitUser, rabbitPass)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	connections, err := initializeConnections(middleware)
	if err != nil {
		return nil, fmt.Errorf("error initializing connections: %w", err)
	}

	return &Reducer{middleware: middleware, connections: connections}, nil
}

func initializeConnection(middleware *common.Middleware, previousQueue, nextQueue string) (connection, error) {
	previousChan, err := middleware.GetChanToRecv(previousQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to receive: %w", previousQueue, err)
	}
	nextChan, err := middleware.GetChanToSend(nextQueue)
	if err != nil {
		return connection{}, fmt.Errorf("error getting channel %s to send: %w", nextQueue, err)
	}
	return connection{previousChan, nextChan}, nil
}

func initializeConnections(middleware *common.Middleware) (map[string]connection, error) {
	connectionQ2, err := initializeConnection(middleware, previousQueueQuery2, nextQueueQuery2)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery2, err)
	}
	connectionQ3, err := initializeConnection(middleware, previousQueueQuery3, nextQueueQuery3)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery3, err)
	}
	connectionQ4, err := initializeConnection(middleware, previousQueueQuery4, nextQueueQuery4)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery4, err)
	}
	connectionQ5, err := initializeConnection(middleware, previousQueueQuery5, nextQueueQuery5)
	if err != nil {
		return nil, fmt.Errorf("error initializing connection for %s: %w", previousQueueQuery5, err)
	}

	connections := map[string]connection{
		previousQueueQuery2: connectionQ2,
		previousQueueQuery3: connectionQ3,
		previousQueueQuery4: connectionQ4,
		previousQueueQuery5: connectionQ5,
	}

	return connections, nil
}
func (r *Reducer) Start() error {
	select {
	case <-r.connections[previousQueueQuery2].ChanToRecv:
		// if message != EOF, processQ2, then send (both)
		return nil
	case <-r.connections[previousQueueQuery3].ChanToRecv:
		// if message != EOF, processQ3, then send (both)
		return nil
	case <-r.connections[previousQueueQuery4].ChanToRecv:
		// if message != EOF, processQ4, then send (both)
		return nil
	case <-r.connections[previousQueueQuery5].ChanToRecv:
		// if message != EOF, processQ5, then send (both)
		return nil
	}
}

// func (r *Reducer) reduceQ2(msg common.Message) error {
// }

// func (r *Reducer) reduceQ3(msg common.Message) error {
// }

// func (r *Reducer) reduceQ4(msg common.Message) error {
// }

// func (r *Reducer) reduceQ5(msg common.Message) error {
// }

func (r *Reducer) Stop() error {
	return r.middleware.Close()
}
