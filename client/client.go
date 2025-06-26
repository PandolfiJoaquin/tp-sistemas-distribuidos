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
	OutputPathFormat = "results/queries-results-%d.txt"
	TotalQueries     = 5
)

type ClientConfig struct {
	Id             int
	ServerAddress  string
	MoviesFile     string
	ReviewsFile    string
	CreditsFile    string
	MaxBatchMovie  int
	MaxBatchReview int
	MaxBatchCredit int
	sleep          int
}

func NewClientConfig(id int, serverAddress, moviesFile, reviewsFile, creditsFile string, maxBatchMovie, maxBatchReview, maxBatchCredits, sleep int) ClientConfig {
	return ClientConfig{
		Id:             id,
		ServerAddress:  serverAddress,
		MoviesFile:     moviesFile,
		ReviewsFile:    reviewsFile,
		CreditsFile:    creditsFile,
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

func (c *Client) sigtermHandler(signalCtx context.Context, ctx context.Context, cancel context.CancelFunc) {
	select {
	case <-signalCtx.Done():
		slog.Info("Received shutdown signal, closing client")
		c.close()
		cancel() // Closes the other context not the signal context
	case <-ctx.Done():
	}
}

func (c *Client) validateFiles() bool {
	fileTypes := []string{"movies", "reviews", "credits"}
	filesPath := []string{c.config.MoviesFile, c.config.ReviewsFile, c.config.CreditsFile}
	for i, file := range filesPath {
		err := utils.ValidateHeaders(file, fileTypes[i])
		if err != nil {
			slog.Error("Error validating file headers", slog.String("file", file), slog.String("type", fileTypes[i]), slog.String("error", err.Error()))
			return false
		}
	}
	return true
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
	if !c.validateFiles() {
		return
	}
	wg := &sync.WaitGroup{}
	// SIGINT and SIGTERM signal handling
	SignalCtx, cancelSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()

	ctx, cancel := context.WithCancel(SignalCtx)

	go c.sigtermHandler(SignalCtx, ctx, cancel)

	err := c.connect()
	if err != nil {
		slog.Error("error starting client", slog.String("error", err.Error()))
		return
	}
	defer c.close()

	slog.Info("client connected to server", slog.String("serverAddress", c.config.ServerAddress))

	AckChannel := make(chan int)

	wg.Add(1)
	go c.RecvAnswers(wg, SignalCtx, cancel, AckChannel)
	c.sendAllData(SignalCtx, ctx, AckChannel)
	wg.Wait()
	close(AckChannel)
}

func (c *Client) sendAllData(SignalCtx context.Context, ctx context.Context, ackChannel <-chan int) {
	defer slog.Debug("sendAllData finished", slog.Int("id", c.config.Id))
	MovieSender := NewSender(&c.conn, c.config.MoviesFile, c.config.MaxBatchMovie, utils.NewMoviesReader, "movies", ackChannel, SignalCtx, ctx)
	if err := MovieSender.Send(); err != nil {
		c.checkSendError(err, "error sending movies")
		return
	}
	ReviewSender := NewSender(&c.conn, c.config.ReviewsFile, c.config.MaxBatchReview, utils.NewReviewReader, "reviews", ackChannel, SignalCtx, ctx)
	if err := ReviewSender.Send(); err != nil {
		c.checkSendError(err, "error sending reviews")
		return
	}

	CreditsSender := NewSender(&c.conn, c.config.CreditsFile, c.config.MaxBatchCredit, utils.NewCreditsReader, "credits", ackChannel, SignalCtx, ctx)
	if err := CreditsSender.Send(); err != nil {
		c.checkSendError(err, "error sending credits")
		return
	}
}

func (c *Client) checkSendError(err error, msg string) {
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) {
		slog.Error(msg, slog.String("error", err.Error()))
	}
	slog.Debug("Error when sending data", slog.String("error", err.Error()), slog.String("type", msg))
}

func (c *Client) CheckRecvError(err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed) {
		slog.Info("Server closed connection")
		return
	}
	slog.Error("error receiving query results", slog.String("error", err.Error()))
	return
}

func (c *Client) writeQueryResults(queriesResults map[int][]models.QueryResult) {
	defer slog.Debug("writeQueryResults finished", slog.Int("id", c.config.Id))
	var sb strings.Builder

	for queryID := 1; queryID <= TotalQueries; queryID++ {
		results, exists := queriesResults[queryID]
		sb.WriteString(fmt.Sprintf("Query %d: ", queryID))
		if !exists || results == nil {
			sb.WriteString("Results empty \n")
		} else {
			for i, result := range results {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(result.String())
			}
			sb.WriteString("\n")
		}
	}

	// Write all results to a single file
	err := os.WriteFile(fmt.Sprintf(OutputPathFormat, c.config.Id), []byte(sb.String()), 0644)
	if err != nil {
		slog.Error("error writing query results", slog.String("error", err.Error()))
	}
}

func (c *Client) RecvAnswers(wg *sync.WaitGroup, SignalCtx context.Context, cancel context.CancelFunc, ackChannel chan<- int) {
	queriesReceived := make([]bool, 0) // Array to store when we get the complete query
	queriesResults := make(map[int][]models.QueryResult)
	defer wg.Done()
	// Cancel the context, not the signal context
	// this is to ensure that the sender is also stopped when there's a connection error
	defer cancel()
	defer slog.Debug("RecvAnswers finished", slog.Int("id", c.config.Id))
	for {
		select {
		case <-SignalCtx.Done():
			return
		default:
			if len(queriesReceived) == TotalQueries {
				slog.Info("All queries received")
				c.writeQueryResults(queriesResults)
				return
			}

			typeOfRes, err := communication.RecvTypeOfResults(c.conn)
			if err != nil {
				c.CheckRecvError(err)
				return
			}

			if typeOfRes == communication.AckMsg {
				err = c.handleAck(ackChannel)
				if err != nil {
					c.CheckRecvError(err)
					return
				}
			} else if typeOfRes == communication.QueryMsg {
				err = c.handleQueryResult(&queriesResults, &queriesReceived)
				if err != nil {
					c.CheckRecvError(err)
					return
				}
			} else {
				slog.Error("unknown message type received", slog.Int("type", typeOfRes))
				return
			}

		}
	}
}

func (c *Client) handleAck(ackChannel chan<- int) error {
	acked, err := communication.RecvAck(c.conn)
	if err != nil {
		return fmt.Errorf("error receiving ack: %w", err)
	}
	ackChannel <- acked
	return nil
}

func (c *Client) checkQ1Empty(queriesResult *map[int][]models.QueryResult) bool {
	_, exists := (*queriesResult)[1]
	return !exists
}

func (c *Client) handleQueryResult(queriesResults *map[int][]models.QueryResult, queriesReceived *[]bool) error {
	results, err := communication.RecvQueryResults(c.conn)
	if err != nil {
		return err
	}
	slog.Debug("Received Query Results", slog.Any("results", results))

	if results.Last {
		*queriesReceived = append(*queriesReceived, true)
		if results.IsEmpty() || (results.QueryId == 1 && c.checkQ1Empty(queriesResults)) {
			(*queriesResults)[results.QueryId] = nil
			txt := fmt.Sprintf("Query result %d", results.QueryId)
			slog.Info(txt, slog.String("result", "Empty"))
		}
	}
	for _, result := range results.Items {
		txt := fmt.Sprintf("Query result %d", results.QueryId)
		if result.IsEmpty() {
			slog.Info(txt, slog.String("result", "Empty"))
			(*queriesResults)[results.QueryId] = nil
		} else {
			slog.Info(txt, slog.String("result", result.String()))
			(*queriesResults)[results.QueryId] = append((*queriesResults)[results.QueryId], result)
		}
	}
	return nil
}
