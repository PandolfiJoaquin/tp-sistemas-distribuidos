package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net"
	"pkg/communication"
	"pkg/models"
	"sync"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/middleware"
)

type q1State struct {
	CurrentWeight   uint32
	EofWeight       int32
	DuplicateFilter *common.DuplicateFilter
}

type Client struct {
	id               string
	connMutex        sync.Mutex
	conn             net.Conn
	dead             bool
	recvChannel      chan *models.TotalQueryResults
	toPreprocess     middleware.SenderQueue
	flushQueue       middleware.SenderQueue
	deadChan         chan<- deadState
	deadWhileSending chan string // used to notify that the client is dead while sending data
	queriesReceived  map[int]bool
	q1State          *q1State
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewClient(conn net.Conn, toPreprocess middleware.SenderQueue, flushQueue middleware.SenderQueue, deadChan chan<- deadState) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		id:               uuid.NewString(),
		connMutex:        sync.Mutex{},
		conn:             conn,
		dead:             false,
		recvChannel:      make(chan *models.TotalQueryResults, 20),
		toPreprocess:     toPreprocess,
		flushQueue:       flushQueue,
		deadChan:         deadChan,
		deadWhileSending: make(chan string, 1),
		queriesReceived:  make(map[int]bool),
		q1State:          &q1State{DuplicateFilter: common.NewDuplicateFilter()},
		ctx:              ctx,
		cancel:           cancel,
	}
}

func (c *Client) Run() {
	defer c.Close()
	go c.sendHandler()
	c.recvHandler()
}

func (c *Client) sendResult(results *models.TotalQueryResults) {
	c.recvChannel <- results
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		slog.Error("Failed to close connection", slog.String("error", err.Error()))
	}
	c.dead = true
	c.cancel()
}

func (c *Client) IsDead() bool {
	return c.dead
}

func (c *Client) sendHandler() {
	err := receiveData[models.RawMovie](c.toPreprocess, "movies", &c.conn, c.id, &c.connMutex)
	if err != nil {
		c.checkSendError(err, "error receiving movies")
		c.deadWhileSending <- c.id
		return
	}

	err = receiveData[models.RawReview](c.toPreprocess, "reviews", &c.conn, c.id, &c.connMutex)
	if err != nil {
		c.checkSendError(err, "error receiving reviews")
		c.deadWhileSending <- c.id
		return
	}

	err = receiveData[models.RawCredits](c.toPreprocess, "credits", &c.conn, c.id, &c.connMutex)
	if err != nil {
		c.checkSendError(err, "error receiving credits")
		c.deadWhileSending <- c.id
		return
	}
}

func (c *Client) handleQ1(results *models.TotalQueryResults) bool {
	if !c.q1State.DuplicateFilter.Accept(results.Header.BatchID) {
		return false
	}
	c.q1State.CurrentWeight += results.Header.Weight
	if results.Header.TotalWeight >= 0 {
		c.q1State.EofWeight = int32(results.Header.TotalWeight)
	}
	if c.q1State.EofWeight > 0 && c.q1State.CurrentWeight == uint32(c.q1State.EofWeight) { //TODO: va a romper si el peso del archivo es 0
		c.queriesReceived[1] = true
		slog.Info("query received", slog.Int("query_id", results.QueryId), slog.String("client id", c.id))
	}
	return true
}

func (c *Client) recvHandler() {
	for {
		if len(c.queriesReceived) == 5 {
			slog.Info("client finished receiving all data", slog.String("id", c.id))
			c.deadChan <- deadState{ClientID: c.id, finishedCorrectly: true}
			break
		}
		select {
		case <-c.ctx.Done():
		case dead := <-c.deadWhileSending:
			slog.Debug("client is dead while sending data", slog.String("id", dead))
			c.dead = true
			c.deadChan <- deadState{ClientID: dead, finishedCorrectly: false}
			return

		case results := <-c.recvChannel:
			if _, ok := c.queriesReceived[results.QueryId]; ok {
				slog.Warn("duplicate query received", slog.Int("query_id", results.QueryId), slog.String("client id", c.id))
				continue
			}
			if results.QueryId == 1 {
				if !c.handleQ1(results) {
					slog.Warn("duplicate query received WINDOW", slog.Int("query_id", results.QueryId), slog.String("client id", c.id))
					continue
				}

			} else {
				c.queriesReceived[results.QueryId] = true
				slog.Info("query received", slog.Int("query_id", results.QueryId), slog.String("client id", c.id))
			}
			if len(results.Items) == 0 && results.Header.TotalWeight < 0 {
				// Empty results
				continue
			}
			c.connMutex.Lock()
			err := communication.SendQueryResults(c.conn, *results)
			c.connMutex.Unlock()
			if err != nil {
				c.checkSendError(err, "error sending query results")
				c.dead = true
				c.deadChan <- deadState{ClientID: c.id, finishedCorrectly: false}
				return
			} else {
				slog.Debug("query results sent", slog.Int("query_id", results.QueryId), slog.String("client id", c.id))
			}
		}
	}
}

func (c *Client) GetId() string {
	return c.id
}

func receiveData[T any](toPreprocess middleware.SenderQueue, batchType string, client *net.Conn, id string, connMutex *sync.Mutex) error {
	total := 0
	batchID := 0
	for {

		batch, err := communication.RecvBatch[T](*client, connMutex)
		if err != nil {
			return fmt.Errorf("error receiving %s: %w", batchType, err)
		}

		batch.Header.BatchID = batchID

		err = publishBatch(batch, batchType, toPreprocess, id)
		if err != nil {
			return fmt.Errorf("error publishing %s batch: %w", batchType, err)
		}

		total += int(batch.Header.Weight)

		if batch.IsEof() {
			break
		}

		batchID++

	}
	slog.Debug("Total received", slog.String("type", batchType), slog.Int("total", total), slog.String("id", id))
	return nil
}

func publishBatch[T any](batch models.RawBatch[T], batchType string, toPreprocess middleware.SenderQueue, clientId string) error {
	bodyBytes, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}

	rawBatch := common.ToProcessMsg{
		Type:     batchType,
		ClientId: clientId,
		Body:     bodyBytes,
	}

	batchToSend, err := json.Marshal(rawBatch)
	if err != nil {
		return fmt.Errorf("error marshalling raw batch: %w", err)
	}

	if err := toPreprocess.Send(batchToSend); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
}

func (c *Client) checkSendError(err error, msg string) {
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
		slog.Error(msg, slog.String("error", err.Error()))
	} else {
		c.dead = true
	}
}

func (c *Client) checkRecvError(err error) {
	if errors.Is(err, io.EOF) {
		slog.Info("Client Disconnected", slog.String("id", c.id))
	} else if errors.Is(err, net.ErrClosed) {
		slog.Error("Client Conn was already closed", slog.String("id", c.id))
	} else {
		slog.Error("error sending query results", slog.String("error", err.Error()), slog.String("id", c.id))
	}
}
