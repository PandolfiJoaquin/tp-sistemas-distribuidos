package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
	"pkg/models"
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
}

func NewGateway(rabbitUser, rabbitPass, port string) (*Gateway, error) {
	config := NewGatewayConfig(rabbitUser, rabbitPass, port)
	gateway := &Gateway{
		config:  config,
		running: true,
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
	for g.running {
		conn, err := g.listener.Accept()
		if err != nil && g.running {
			slog.Error("error accepting connection", slog.String("error", err.Error()))
			continue
		}
		g.client = conn
		g.handleConnection()
	}
}

func (g *Gateway) handleConnection() {
	slog.Info("Client connected", slog.String("address", g.client.RemoteAddr().String()))
	total := 0
	for {
		movies, err := communication.RecvMovies(g.client)
		if err != nil {
			slog.Error("error receiving movies", slog.String("error", err.Error()))
			return
		}

		//slog.Info("Received movies", slog.Any("headers", movies.Header))

		body, err := json.Marshal(movies)
		if err != nil {
			slog.Error("error marshalling movies", slog.String("error", err.Error()))
			return
		}

		g.moviesToFilter <- body

		total += int(movies.Header.Weight)

		if movies.IsEof() {
			break
		}
	}

	slog.Info("Total movies received", slog.Int("total", total))
}

func (g *Gateway) Start() {
	go g.processMessages()
	g.listen()

	forever := make(chan bool)
	<-forever
}

func (g *Gateway) processMessages() {
	for msg := range g.q1Results {
		var batch common.Batch
		if err := json.Unmarshal(msg.Body, &batch); err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			return
		}

		// TODO: Check for different query results.
		q1Movies := make([]models.QueryResult, len(batch.Movies))
		for i, movie := range batch.Movies {
			q1Movies[i] = models.Q1Movie{
				Title:  movie.Title,
				Genres: movie.Genres,
			}
		}

		err := communication.SendQueryResults(g.client, 1, q1Movies)
		if err != nil {
			slog.Error("error sending query results", slog.String("error", err.Error()))
			return
		}

		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
			return
		}

		//if err := json.Unmarshal(msg.Body, &batch); err != nil {
		//	slog.Error("error unmarshalling message", slog.String("error", err.Error()))
		//}
		//
		//if batch.IsEof() {
		//	slog.Info("EOF received")
		//} else {
		//	slog.Info("Got movies", slog.Any("headers", batch.Header), slog.Any("movies", batch.Movies))
		//}
		//
		//if err := msg.Ack(); err != nil {
		//	slog.Error("error acknowledging message", slog.String("error", err.Error()))
		//}
	}
}
