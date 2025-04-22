package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/signal"
	"pkg/communication"
	"sync"
	"syscall"
	"tp-sistemas-distribuidos/client/utils"
)

const MoviePath = "archive/movies_metadata.csv"
const ReviewPath = "archive/ratings.csv"
const CreditsPath = "archive/credits.csv"

type ClientConfig struct {
	ServerAddress  string
	MaxBatchMovie  int
	MaxBatchReview int
	MaxBatchCredit int
}

func NewClientConfig(serverAddress string, maxBatchMovie, maxBatchReview, maxBatchCredits int) ClientConfig {
	return ClientConfig{
		ServerAddress:  serverAddress,
		MaxBatchMovie:  maxBatchMovie,
		MaxBatchReview: maxBatchReview,
		MaxBatchCredit: maxBatchCredits,
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

func (c *Client) SigtermHandler(ctx context.Context) {
	select {
	case <-ctx.Done():
		slog.Info("Received shutdown signal, closing client")
		if c.conn != nil {
			err := c.conn.Close()
			if err != nil {
				slog.Error("error closing connection", slog.String("error", err.Error()))
			}
		}
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
	wg := &sync.WaitGroup{}
	// SIGINT and SIGTERM signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go c.SigtermHandler(ctx)

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
	slog.Info("Shutting down client")
}

func (c *Client) sendData() {
	total, err := c.SendAllMovies()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			slog.Error("error sending movies", slog.String("error", err.Error()))
		}
		return
	}
	slog.Info("Sent all movies to server", slog.Int("total", total))

	total, err = c.SendAllReviews()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			slog.Error("error sending reviews", slog.String("error", err.Error()))
		}
		return
	}
	slog.Info("Sent all reviews to server", slog.Int("total", total))

	total, err = c.SendAllCredits()
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

	return nil
}

func (c *Client) SendAllMovies() (int, error) {
	reader, err := utils.NewMoviesReader(MoviePath, c.config.MaxBatchMovie)
	defer reader.Close()
	if err != nil {
		return 0, fmt.Errorf("error creating movies reader: %w", err)
	}

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

	return nil
}

func (c *Client) SendAllReviews() (int, error) {
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

	return nil
}

func (c *Client) SendAllCredits() (int, error) {
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

const TotalQueries = 5

func (c *Client) RecvAnswers(wg *sync.WaitGroup, ctx context.Context) {
	queriesReceived := make([]bool, 0) // Array to store when we get the complete query
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if len(queriesReceived) == TotalQueries {
				slog.Info("All queries received")
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
				slog.Info("Got query result", slog.String("result", result.String()))
			}
		}
	}
}
