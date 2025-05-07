package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net"
	"pkg/communication"
	"pkg/models"
	"tp-sistemas-distribuidos/server/common"
)

type Client struct {
	id           string
	conn         net.Conn
	dead         bool
	recvChannel  chan *models.TotalQueryResults
	toPreprocess *chan<- []byte
	done         uint8
}

func NewClient(conn net.Conn, toPreprocess *chan<- []byte) *Client {
	return &Client{
		id:           uuid.NewString(),
		conn:         conn,
		dead:         false,
		recvChannel:  make(chan *models.TotalQueryResults),
		toPreprocess: toPreprocess,
	}
}

func (c *Client) Run() {
	go c.sendHandler()
	c.recvHandler()
}

func (c *Client) sendResult(results *models.TotalQueryResults) {
	c.recvChannel <- results
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		// Handle error
	}
}

func (c *Client) IsDead() bool {
	return c.dead
}

func (c *Client) sendHandler() {

	err := receiveData[models.RawMovie](*c.toPreprocess, "movies", &c.conn, c.id)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("error receiving movies", slog.String("error", err.Error()), slog.String("id", c.id))
		} else {
			c.dead = true
		}
		return
	}

	err = receiveData[models.RawReview](*c.toPreprocess, "reviews", &c.conn, c.id)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("error receiving reviews", slog.String("error", err.Error()), slog.String("id", c.id))
		} else {
			c.dead = true
		}
		return
	}

	err = receiveData[models.RawCredits](*c.toPreprocess, "credits", &c.conn, c.id)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("error receiving reviews", slog.String("error", err.Error()), slog.String("id", c.id))
		} else {
			c.dead = true
		}
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
		case results := <-c.recvChannel:
			if results.Last {
				c.done++
			}
			err := communication.SendQueryResults(c.conn, *results)
			if err != nil {
				slog.Error("error sending query results", slog.String("error", err.Error()), slog.String("id", c.id))
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
