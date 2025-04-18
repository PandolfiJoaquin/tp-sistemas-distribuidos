package main

import (
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
	"sync"
	"tp-sistemas-distribuidos/client/utils"
)

const PATH = "archive/movies.csv"

type ClientConfig struct {
	ServerAddress string
	MaxBatch      int
}

func NewClientConfig(serverAddress string, maxBatch int) ClientConfig {
	return ClientConfig{
		ServerAddress: serverAddress,
		MaxBatch:      maxBatch,
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

	defer c.conn.Close()

	slog.Info("client connected to server", slog.String("serverAddress", c.config.ServerAddress))

	c.SendAllMovies()
	wg.Add(1)
	go c.RecvAnswers(wg)
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
	reader, err := utils.NewMoviesReader(PATH, c.config.MaxBatch)
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

	err = communication.SendEOF(c.conn, int32(reader.Total))
	if err != nil {
		slog.Error("error sending EOF", slog.String("error", err.Error()))
		return
	}
	slog.Info("Sent all movies to server", slog.Int("total", reader.Total))
}

func (c *Client) RecvAnswers(wg *sync.WaitGroup) {
	defer wg.Done()
	// Implement the logic to receive answers from the server
}
