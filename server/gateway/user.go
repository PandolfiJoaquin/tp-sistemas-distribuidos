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
	"syscall"
	"tp-sistemas-distribuidos/server/common"
)

type Client struct {
	id           string
	conn         net.Conn
	dead         bool
	recvChannel  chan *models.TotalQueryResults
	toPreprocess *chan<- []byte
	done         uint8
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewClient(conn net.Conn, toPreprocess *chan<- []byte) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		id:           uuid.NewString(),
		conn:         conn,
		dead:         false,
		recvChannel:  make(chan *models.TotalQueryResults),
		toPreprocess: toPreprocess,
		ctx:          ctx,
		cancel:       cancel,
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

	err := receiveData[models.RawMovie](*c.toPreprocess, "movies", &c.conn, c.id)
	if err != nil {
		c.checkSendError(err, "error receiving movies")
		return
	}

	err = receiveData[models.RawReview](*c.toPreprocess, "reviews", &c.conn, c.id)
	if err != nil {
		c.checkSendError(err, "error receiving reviews")
		return
	}

	err = receiveData[models.RawCredits](*c.toPreprocess, "credits", &c.conn, c.id)
	if err != nil {
		c.checkSendError(err, "error receiving credits")
		return
	}
}

func (c *Client) recvHandler() {
	for {
		if c.done == 5 {
			slog.Info("client finished receiving all data", slog.String("id", c.id))
			break
		}
		select {
		case <-c.ctx.Done():
		case results := <-c.recvChannel:
			if results.Last {
				c.done++
			}
			err := communication.SendQueryResults(c.conn, *results)
			if err != nil {
				if errors.Is(err, io.EOF) {
					slog.Info("Client Disconnected", slog.String("id", c.id))
				} else if errors.Is(err, net.ErrClosed) {
					slog.Error("Client Conn was already closed", slog.String("id", c.id))
				} else {
					slog.Error("error sending query results", slog.String("error", err.Error()), slog.String("id", c.id))
				}
				return
			}
		}
	}
}

func (c *Client) GetId() string {
	return c.id
}

func receiveData[T any](toPreprocess chan<- []byte, batchType string, client *net.Conn, id string) error {
	total := 0
	for {
		batch, err := communication.RecvBatch[T](*client)
		if err != nil {
			return fmt.Errorf("error receiving %s: %w", batchType, err)
		}

		err = publishBatch(batch, batchType, toPreprocess, id)
		if err != nil {
			return fmt.Errorf("error publishing %s batch: %w", batchType, err)
		}

		total += int(batch.Header.Weight)

		if batch.IsEof() {
			break
		}
	}
	slog.Info("Total received", slog.String("type", batchType), slog.Int("total", total), slog.String("id", id))
	return nil
}

func publishBatch[T any](batch models.RawBatch[T], batchType string, toPreprocess chan<- []byte, clientId string) error {
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

	toPreprocess <- batchToSend
	return nil
}

func (c *Client) checkSendError(err error, msg string) {
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
		slog.Error(msg, slog.String("error", err.Error()))
	} else {
		c.dead = true
	}
}
