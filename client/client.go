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
	"time"
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
	case <-finishedChan:
	}
	c.close()
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

	slog.Info("client connected to server", slog.String("serverAddress", c.config.ServerAddress))

	wg.Add(1)
	go c.RecvAnswers(wg, ctx)
	c.sendData()
	wg.Wait()
	finishedChan <- true
	slog.Info("Shutting down client")
}

func (c *Client) sendData() {
	total, err := c.sendAllMovies()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			slog.Error("error sending movies", slog.String("error", err.Error()))
		}
		return
	}
	slog.Info("Sent all movies to server", slog.Int("total", total))

	total, err = c.sendAllReviews()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			slog.Error("error sending reviews", slog.String("error", err.Error()))
		}
		return
	}
	slog.Info("Sent all reviews to server", slog.Int("total", total))

	total, err = c.sendAllCredits()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			slog.Error("error sending credits", slog.String("error", err.Error()))
		}
		return
	}

	slog.Info("Sent all credits to server", slog.Int("total", total))
}

func (c *Client) sendMovies(reader *utils.MoviesReader) error {
	movies, err := reader.ReadMovies()
	if err != nil {
		return fmt.Errorf("error sending movies: %w", err)
	}

	err = communication.SendMovies(c.conn, movies)
	if err != nil {
		return fmt.Errorf("error sending movies: %w", err)
	}

	time.Sleep(time.Duration(c.config.sleep) * time.Millisecond)

	return nil
}

func (c *Client) sendAllMovies() (int, error) {
	reader, err := utils.NewMoviesReader(MoviePath, c.config.MaxBatchMovie)
	if err != nil {
		return 0, fmt.Errorf("error creating movies reader: %w", err)
	}
	defer reader.Close()

	for !reader.Finished {
		err = c.sendMovies(reader)
		if err != nil {
			return 0, fmt.Errorf("error sending movies: %w", err)
		}
	}

	err = communication.SendMovieEof(c.conn, int32(reader.Total))
	if err != nil {
		return 0, fmt.Errorf("error sending EOF: %w", err)
	}
	return reader.Total, nil
}

func (c *Client) sendReviews(reader *utils.ReviewReader) error {
	reviews, err := reader.ReadReviews()
	if err != nil {
		return fmt.Errorf("error sending reviews: %w", err)
	}

	err = communication.SendReviews(c.conn, reviews)
	if err != nil {
		return fmt.Errorf("error sending reviews: %w", err)
	}

	time.Sleep(time.Duration(c.config.sleep) * time.Millisecond)

	return nil
}

func (c *Client) sendAllReviews() (int, error) {
	reader, err := utils.NewReviewReader(ReviewPath, c.config.MaxBatchReview)
	if err != nil {
		return 0, fmt.Errorf("error creating reviews reader: %w", err)
	}

	for !reader.Finished {
		err = c.sendReviews(reader)
		if err != nil {
			return 0, fmt.Errorf("error sending reviews: %w", err)
		}
	}

	err = communication.SendReviewEof(c.conn, int32(reader.Total))
	if err != nil {
		return 0, fmt.Errorf("error sending reviews EOF: %w", err)
	}
	return reader.Total, nil
}

func (c *Client) sendCredits(reader *utils.CreditsReader) error {
	credits, err := reader.ReadCredits()
	if err != nil {
		return fmt.Errorf("error sending credits: %w", err)
	}

	err = communication.SendCredits(c.conn, credits)
	if err != nil {
		return fmt.Errorf("error sending credits: %w", err)
	}

	time.Sleep(time.Duration(c.config.sleep) * time.Millisecond)

	return nil
}

func (c *Client) sendAllCredits() (int, error) {
	reader, err := utils.NewCreditsReader(CreditsPath, c.config.MaxBatchCredit)
	if err != nil {
		return 0, fmt.Errorf("error creating credits reader: %w", err)
	}

	for !reader.Finished {
		err = c.sendCredits(reader)
		if err != nil {
			return 0, fmt.Errorf("error sending credits: %w", err)
		}
	}

	err = communication.SendCreditsEof(c.conn, int32(reader.Total))
	if err != nil {
		return 0, fmt.Errorf("error sending credits EOF: %w", err)
	}
	return reader.Total, nil
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

			if communication.IsQueryEof(results) {
				queriesReceived = append(queriesReceived, true)
				continue
			}

			if results.QueryId != 1 {
				queriesReceived = append(queriesReceived, false)
			}

			for _, result := range results.Items {
				txt := fmt.Sprintf("Query result %d", results.QueryId)
				slog.Info(txt, slog.String("result", result.String()))
				queriesResults[results.QueryId] = append(queriesResults[results.QueryId], result)
			}
		}
	}
}
