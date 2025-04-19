package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"pkg/communication"
	"pkg/models"
	"sync"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
)

const PREVIOUS_STEP = "movies-to-preprocess"
const NEXT_STEP = "q1-results"

type GatewayConfig struct {
	RabbitUser string
	RabbitPass string
	port       string
}

func NewGatewayConfig(rabbitUser, rabbitPass, port string) GatewayConfig {
	return GatewayConfig{
		RabbitUser: rabbitUser,
		RabbitPass: rabbitPass,
		port:       port,
	}
}

type Gateway struct {
	middleware     *common.Middleware
	moviesToFilter chan<- []byte
	q1Results      <-chan common.Message
	config         GatewayConfig
	listener       net.Listener
	client         net.Conn
	running        bool
	ctx            context.Context
	free           chan bool
}

func NewGateway(rabbitUser, rabbitPass, port string) (*Gateway, error) {
	config := NewGatewayConfig(rabbitUser, rabbitPass, port)
	gateway := &Gateway{
		config:  config,
		running: true,
		free:    make(chan bool),
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		slog.Error("error starting gateway", slog.String("error", err.Error()))
		return nil, err
	}
	gateway.listener = listener

	err = gateway.middlewareSetup()
	if err != nil {
		slog.Error("error setting up gateway", slog.String("error", err.Error()))
		return nil, err
	}

	return gateway, nil
}

func (g *Gateway) middlewareSetup() error {
	middleware, err := common.NewMiddleware(g.config.RabbitUser, g.config.RabbitPass)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return err
	}
	g.middleware = middleware

	moviesToFilter, err := g.middleware.GetChanToSend(PREVIOUS_STEP)
	if err != nil {
		slog.Error("error getting channel 'movies-to-preprocess'", slog.String("error", err.Error()))
	}

	g.moviesToFilter = moviesToFilter

	q1Results, err := g.middleware.GetChanToRecv(NEXT_STEP)
	if err != nil {
		slog.Error("error getting channel 'q1-results'", slog.String("error", err.Error()))
	}

	g.q1Results = q1Results

	return nil
}

func (g *Gateway) listen() {
	// Listener will only accept one connection at a time
	for g.running {
		conn, err := g.listener.Accept()
		if err != nil {
			if g.running { // only log if not shutting down
				slog.Error("error accepting connection", slog.String("error", err.Error()))
			}
			return
		}
		g.handleConnection(conn)
		<-g.free // wait for the previous connection to finish
		conn.Close()
	}
}

func (g *Gateway) receiveMovies() error {
	total := 0
	for {
		movies, err := communication.RecvMovies(g.client)
		if err != nil {
			return fmt.Errorf("error receiving movies: %w", err)
		}

		body, err := json.Marshal(movies)
		if err != nil {
			return fmt.Errorf("error marshalling movies: %w", err)
		}

		g.moviesToFilter <- body

		total += int(movies.Header.Weight)

		if movies.IsEof() {
			break
		}
	}
	slog.Info("Total movies received", slog.Int("total", total))
	return nil
}

func (g *Gateway) handleConnection(conn net.Conn) {
	g.client = conn
	slog.Info("Client connected", slog.String("address", g.client.RemoteAddr().String()))
	err := g.receiveMovies()
	if err != nil {
		slog.Error("error receiving movies", slog.String("error", err.Error()))
		return
	}
	// TODO: Handle Ratings and Actors
}

func (g *Gateway) signalHandler(wg *sync.WaitGroup) {
	defer wg.Done()
	// Hears SIGINT and SIGTERM signals
	// and closes the listener and current connection
	select {
	case <-g.ctx.Done():
		slog.Info("Received shutdown signal")
		g.running = false
		if err := g.listener.Close(); err != nil {
			slog.Error("error closing listener", slog.String("error", err.Error()))
		}
		if g.client != nil {
			if err := g.client.Close(); err != nil {
				slog.Error("error closing client connection", slog.String("error", err.Error()))
			}
		}
	}
}

func (g *Gateway) Start() {
	wg := &sync.WaitGroup{}

	defer func(middleware *common.Middleware) {
		err := middleware.Close()
		if err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}(g.middleware)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	g.ctx = ctx
	defer cancel()

	wg.Add(2)
	go g.signalHandler(wg)
	go g.processMessages(wg)
	g.listen()
	wg.Wait()
}

func (g *Gateway) processMessages(wg *sync.WaitGroup) {
	defer wg.Done()
	done := 0

	for {
		select {
		case <-g.ctx.Done():
			return

		case msg := <-g.q1Results:
			batch, err := g.consumeBatch(msg.Body)
			if err != nil {
				slog.Error("error consuming results", slog.String("error", err.Error()))
				return
			}

			if len(batch.Movies) == 0 && batch.Header.TotalWeight == -1 {
				continue
			}

			err = g.processResult(batch, &done)
			if err != nil {
				if !g.running {
					slog.Error("error processing result", slog.String("error", err.Error()))
				}
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		}
	}
}

func (g *Gateway) consumeBatch(msg []byte) (common.Batch, error) {
	var batch common.Batch
	if err := json.Unmarshal(msg, &batch); err != nil {
		return batch, fmt.Errorf("error unmarshalling result: %w", err)
	}
	return batch, nil
}

const TotalQueries = 1

func (g *Gateway) processResult(batch common.Batch, done *int) error {
	if batch.IsEof() {
		slog.Info("EOF message received", slog.Any("headers", batch.Header))
		err := communication.SendQueryEof(g.client, 1)
		if err != nil {
			return fmt.Errorf("error sending EOF: %w", err)
		}
		*done++
		if *done == TotalQueries { // all queries done
			g.free <- true // signal that the connection is free for the next one
		}
	} else {
		// TODO: Check for different query results. Query 1 for now
		q1Movies := make([]models.QueryResult, len(batch.Movies))
		for i, movie := range batch.Movies {
			q1Movies[i] = models.Q1Movie{
				Title:  movie.Title,
				Genres: movie.Genres,
			}
		}
		err := communication.SendQueryResults(g.client, 1, q1Movies)
		if err != nil {
			return fmt.Errorf("error sending query results: %w", err)
		}
	}
	return nil
}
