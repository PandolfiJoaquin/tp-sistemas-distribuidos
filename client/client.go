package main

import (
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
	"sync"
	"tp-sistemas-distribuidos/client/utils"
)

const MoviePath = "archive/movies_metadata.csv"
const ReviewPath = "archive/ratings_small.csv"
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

func (c *Client) Start() {
	wg := &sync.WaitGroup{}

	err := c.connect()
	if err != nil {
		slog.Error("error starting client", slog.String("error", err.Error()))
		return
	}

	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			slog.Error("error closing connection", slog.String("error", err.Error()))
		}
	}(c.conn)

	slog.Info("client connected to server", slog.String("serverAddress", c.config.ServerAddress))

	wg.Add(1)
	go c.RecvAnswers(wg)

	c.SendAllMovies()
	c.SendAllReviews()
	c.SendAllCredits()

	wg.Wait()

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

func (c *Client) SendAllMovies() {
	reader, err := utils.NewMoviesReader(MoviePath, c.config.MaxBatchMovie)
	defer reader.Close()
	if err != nil {
		slog.Error("error creating movies reader", slog.String("error", err.Error()))
		return
	}

	for !reader.Finished {
		err = c.sendMovies(reader)
		if err != nil {
			slog.Error("error sending movies", slog.String("error", err.Error()))
			return
		}
	}

	err = communication.SendMovieEof(c.conn, int32(reader.Total))
	if err != nil {
		slog.Error("error sending EOF", slog.String("error", err.Error()))
		return
	}
	slog.Info("Sent all movies to server", slog.Int("total", reader.Total))
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

func (c *Client) SendAllReviews() {
	reader, err := utils.NewReviewReader(ReviewPath, c.config.MaxBatchReview)
	if err != nil {
		slog.Error("error creating reviews reader", slog.String("error", err.Error()))
		return
	}

	for !reader.Finished {
		err = c.sendReviews(reader)
		if err != nil {
			slog.Error("error sending reviews", slog.String("error", err.Error()))
			return
		}
	}

	err = communication.SendReviewEof(c.conn, int32(reader.Total))
	if err != nil {
		slog.Error("error sending EOF", slog.String("error", err.Error()))
		return
	}
	slog.Info("Sent all reviews to server", slog.Int("total", reader.Total))
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

func (c *Client) SendAllCredits() {
	reader, err := utils.NewCreditsReader(CreditsPath, c.config.MaxBatchCredit)
	if err != nil {
		slog.Error("error creating credits reader", slog.String("error", err.Error()))
		return
	}

	for !reader.Finished {
		err = c.sendCredits(reader)
		if err != nil {
			slog.Error("error sending credits", slog.String("error", err.Error()))
			return
		}
	}

	err = communication.SendCreditsEof(c.conn, int32(reader.Total))
	if err != nil {
		slog.Error("error sending EOF", slog.String("error", err.Error()))
		return
	}
	slog.Info("sent credits", slog.Int("total", reader.Total))
}

const TotalQueries = 7

func (c *Client) RecvAnswers(wg *sync.WaitGroup) {
	queriesReceived := make([]bool, 0) // Array to store when we get the complete query
	defer wg.Done()
	for {
		if len(queriesReceived) == TotalQueries {
			slog.Info("All queries received")
			break
		}

		// TODO:  Handle EOF of different queries
		results, err := communication.RecvQueryResults(c.conn)
		if err != nil {
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
