package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pkg/communication"
	"pkg/models"
	"strings"
	"sync"
	"syscall"
	"tp-sistemas-distribuidos/client/utils"
)

const (
	MoviePath        = "archive/movies_metadata.csv"
	ReviewPath       = "archive/ratings_small.csv"
	CreditsPath      = "archive/credits.csv"
	OutputPathFormat = "results/queries-results-%d.txt"
	TotalQueries     = 5
)

type ClientConfig struct {
	Id             int
	ServerAddress  string
	MaxBatchMovie  int
	MaxBatchReview int
	MaxBatchCredit int
	sleep          int
}

func NewClientConfig(id int, serverAddress string, maxBatchMovie, maxBatchReview, maxBatchCredits, sleep int) ClientConfig {
	return ClientConfig{
		Id:             id,
		ServerAddress:  serverAddress,
		MaxBatchMovie:  maxBatchMovie,
		MaxBatchReview: maxBatchReview,
		MaxBatchCredit: maxBatchCredits,
		sleep:          sleep,
	}
}

type Client struct {
	config ClientConfig
	conn   net.Conn
}

func NewClient(config ClientConfig) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		slog.Error("error connecting to server", slog.String("error", err.Error()))
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) sigtermHandler(ctx context.Context, finishedChan chan bool) {
	select {
	case <-ctx.Done():

		slog.Info("Received shutdown signal, closing client")
		c.close()
	case <-finishedChan:
		return
	}
}

func (c *Client) close() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("error closing connection", slog.String("error", err.Error()))
		}
	}
}

func (c *Client) Start() {
	finishedChan := make(chan bool)
	wg := &sync.WaitGroup{}
	// SIGINT and SIGTERM signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go c.sigtermHandler(ctx, finishedChan)

	err := c.connect()
	if err != nil {
		slog.Error("error starting client", slog.String("error", err.Error()))
		return
	}
	defer c.close()

	slog.Info("client connected to server", slog.String("serverAddress", c.config.ServerAddress))

	wg.Add(1)
	go c.RecvAnswers(wg, ctx)
	c.sendAllData()
	wg.Wait()
	close(finishedChan)
	slog.Info("Shutting down client")
}

func (c *Client) sendAllData() {
	MovieSender := NewSender(&c.conn, MoviePath, c.config.MaxBatchMovie, utils.NewMoviesReader, "movies")
	if err := MovieSender.Send(); err != nil {
		c.checkSendError(err, "error sending movies")
		return
	}
	reviewPath := ReviewPath
	if c.config.Id == 2 {
		reviewPath = "archive/ratings.csv"
	}
	ReviewSender := NewSender(&c.conn, reviewPath, c.config.MaxBatchReview, utils.NewReviewReader, "reviews")
	if err := ReviewSender.Send(); err != nil {
		c.checkSendError(err, "error sending reviews")
		return
	}

	CreditsSender := NewSender(&c.conn, CreditsPath, c.config.MaxBatchCredit, utils.NewCreditsReader, "credits")
	if err := CreditsSender.Send(); err != nil {
		c.checkSendError(err, "error sending credits")
		return
	}
}

func (c *Client) checkSendError(err error, msg string) {
	// ignore EOF and closed errors (detection happens in recv)
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
		slog.Error(msg, slog.String("error", err.Error()))
	}
}

func (c *Client) writeQueryResults(queriesResults map[int][]models.QueryResult) {
	var sb strings.Builder

	for queryID := 1; queryID <= TotalQueries; queryID++ {
		results, exists := queriesResults[queryID]
		if !exists {
			slog.Error("query results not found", slog.Int("queryID", queryID))
			continue
		}

		sb.WriteString(fmt.Sprintf("Query %d: ", queryID))

		// For other queries, write results normally
		for i, result := range results {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(result.String())
		}
		sb.WriteString("\n")
	}

	// Write all results to a single file
	err := os.WriteFile(fmt.Sprintf(OutputPathFormat, c.config.Id), []byte(sb.String()), 0644)
	if err != nil {
		slog.Error("error writing query results", slog.String("error", err.Error()))
	}
}

func (c *Client) RecvAnswers(wg *sync.WaitGroup, ctx context.Context) {
	queriesReceived := make([]bool, 0) // Array to store when we get the complete query
	queriesResults := make(map[int][]models.QueryResult)
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if len(queriesReceived) == TotalQueries {
				slog.Info("All queries received")
				c.writeQueryResults(queriesResults)
				return
			}

			results, err := communication.RecvQueryResults(c.conn)
			if err != nil {
				if errors.Is(err, io.EOF) {
					slog.Info("Server closed connection")
					return
				}
				if errors.Is(err, net.ErrClosed) {
					return
				}
				slog.Error("error receiving query results", slog.String("error", err.Error()))
				return
			}

			if results.Last {
				queriesReceived = append(queriesReceived, true)
			}

			for _, result := range results.Items {
				txt := fmt.Sprintf("Query result %d", results.QueryId)
				slog.Info(txt, slog.String("result", result.String()))
				queriesResults[results.QueryId] = append(queriesResults[results.QueryId], result)
			}
		}
	}
}
