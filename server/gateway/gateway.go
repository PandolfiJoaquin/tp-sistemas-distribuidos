package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"pkg/models"
	"sync"
	"syscall"
	"time"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/middleware"
)

const (
	dataPath          = "data/"
	clientsFile       = "clients.json"
	rabbitHost        = "rabbitmq"
	nextStep          = "to-preprocess"
	flushExchange     = "flush-exchange"
	flushTopic        = "client-flush"
	heartbeatInterval = 1 * time.Second
)

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
	middleware    *middleware.Middleware
	resultsQueues map[int]<-chan middleware.Message
	flushQueue    middleware.SenderQueue
	toPreprocess  middleware.SenderQueue
	deadChan      chan deadState
	ClientMutex   sync.Mutex
	config        GatewayConfig
	listener      net.Listener
	clients       map[string]*Client
	running       bool
	ctx           context.Context
}

func NewGateway(rabbitUser, rabbitPass, port string) (*Gateway, error) {
	config := NewGatewayConfig(rabbitUser, rabbitPass, port)
	gateway := &Gateway{
		config:        config,
		running:       true,
		resultsQueues: make(map[int]<-chan middleware.Message),
		clients:       make(map[string]*Client),
		deadChan:      make(chan deadState),
		ClientMutex:   sync.Mutex{},
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

	slog.Info("starting gateway")
	return gateway, nil
}

func (g *Gateway) middlewareSetup() error {
	middleware, err := middleware.NewMiddleware(g.config.RabbitUser, g.config.RabbitPass, rabbitHost)
	if err != nil {
		slog.Error("error creating middleware", slog.String("error", err.Error()))
		return fmt.Errorf("error creating middleware: %w", err)
	}
	g.middleware = middleware

	processorChan, err := g.middleware.GetQueueToSend(nextStep)
	if err != nil {
		slog.Error("error getting channel to send", slog.String("queue", nextStep), slog.String("error", err.Error()))
		return fmt.Errorf("error getting channel to send: %w", err)
	}

	for i := 1; i <= 5; i++ {
		resultsChan, err := g.middleware.GetChanToRecv(fmt.Sprintf("q%d-results", i))
		if err != nil {
			slog.Error("error getting channel to receive", slog.String("queue", fmt.Sprintf("q%d-results", i)), slog.String("error", err.Error()))
			return fmt.Errorf("error getting channel to receive: %w", err)
		}
		g.resultsQueues[i] = resultsChan
	}

	g.toPreprocess = processorChan

	flushChan, err := g.middleware.GetFanoutQueueToSend(flushExchange)
	if err != nil {
		slog.Error("error getting flush channel", slog.String("error", err.Error()))
		return fmt.Errorf("error getting flush channel: %w", err)
	}
	g.flushQueue = flushChan

	return nil
}

func (g *Gateway) listen() {
	for g.running {
		slog.Debug("Waiting for client connection")
		conn, err := g.listener.Accept()
		if err != nil {
			if g.running { // only log if not shutting down
				slog.Error("error accepting connection", slog.String("error", err.Error()))
			}
			return
		}
		client := NewClient(conn, g.toPreprocess, g.flushQueue, g.deadChan)
		slog.Info("Client connected", slog.String("address", conn.RemoteAddr().String()))
		g.ClientMutex.Lock()
		g.clients[client.GetId()] = client
		if err := g.saveClients(); err != nil {
			slog.Error("error saving clients", slog.String("error", err.Error()))
			continue
		}
		g.ClientMutex.Unlock()
		fmt.Printf("Client %s connected\n", client.GetId())
		go client.Run()
	}
}

func (g *Gateway) signalHandler(wg *sync.WaitGroup) {
	defer wg.Done()
	// Hears SIGINT and SIGTERM signals
	// and closes the listener and current connection
	<-g.ctx.Done()
	slog.Info("Received shutdown signal")
	g.running = false
	if err := g.listener.Close(); err != nil {
		slog.Error("error closing listener", slog.String("error", err.Error()))
	}
	slog.Info("listener closed")
}

func (g *Gateway) flushOldClients() error {
	// Reads the clients file of old gateway and sends a message to flush each client
	data, err := os.ReadFile(dataPath + clientsFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) { // If the file does not exist, we just ignore it, means no old clients
			return fmt.Errorf("error reading old clients: %w", err)
		}
		return nil
	}

	clients := make([]common.FlushClient, 0)
	if err := json.Unmarshal(data, &clients); err != nil {
		return fmt.Errorf("error unmarshalling old clients: %w", err)
	}

	if len(clients) > 0 {
		slog.Info("Flushing old clients", slog.Int("count", len(clients)))
	}

	for _, c := range clients {
		clientData, err := json.Marshal(c)
		if err != nil {
			return fmt.Errorf("error marshalling old client: %w", err)
		}
		err = g.flushQueue.Send(clientData)
		slog.Debug("Flushing old client", slog.String("client_id", c.ClientID))
		if err != nil {
			return fmt.Errorf("error flushing old clients: %w", err)
		}
	}

	err = os.Remove(dataPath + clientsFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) { // If the file does not exist, we just ignore it
			return fmt.Errorf("error removing old clients file: %w", err)
		}
	}

	return nil
}

func (g *Gateway) Start() {
	wg := &sync.WaitGroup{}

	defer func(middleware *middleware.Middleware) {
		err := middleware.Close()
		if err != nil {
			slog.Error("error closing middleware", slog.String("error", err.Error()))
		}
	}(g.middleware)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	g.ctx = ctx
	defer cancel()

	err := g.flushOldClients()
	if err != nil {
		slog.Error("error flushing old clients", slog.String("error", err.Error()))
		return
	}

	wg.Add(3)
	go g.signalHandler(wg)
	go g.processMessages(wg)
	go g.persistencyHandler(wg)
	g.listen()
	wg.Wait()

	g.closeClients()

	slog.Info("Gateway shut down")
}

func (g *Gateway) closeClients() {
	for id, client := range g.clients {
		if !client.IsDead() {
			slog.Info("Closing client connection", slog.String("id", id))
			client.Close()
		}
		delete(g.clients, id)
	}
}

func (g *Gateway) processMessages(wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		var err error
		select {
		case <-g.ctx.Done():
			slog.Info("Context done, stopping message processing")
			return
		case <-ticker.C:
		case msg := <-g.resultsQueues[1]:
			err = g.handleResult(msg, 1)
		case msg := <-g.resultsQueues[2]:
			err = g.handleResult(msg, 2)
		case msg := <-g.resultsQueues[3]:
			err = g.handleResult(msg, 3)
		case msg := <-g.resultsQueues[4]:
			err = g.handleResult(msg, 4)
		case msg := <-g.resultsQueues[5]:
			err = g.handleResult(msg, 5)
		}

		if err != nil {
			slog.Error("error processing message", slog.String("error", err.Error()))
			return
		}
		g.middleware.SendHeartbeat()
	}
}

func (g *Gateway) handleResult1(msg middleware.Message) (*models.ResultWithId, error) {
	batch, err := g.consumeBatch(msg.Body)
	if err != nil {
		return nil, fmt.Errorf("error consuming results: %w", err)
	}

	results := MovieToQResult(batch)
	resultsWithId := models.ResultWithId{
		Id:      batch.Header.ClientID,
		Results: results,
	}

	return &resultsWithId, err
}

func (g *Gateway) handleResults2(msg middleware.Message) (*models.ResultWithId, error) {
	var top5Countries common.Top5Countries
	if err := json.Unmarshal(msg.Body, &top5Countries); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 5 countries: %w", err)
	}


	results := Top5CountriesToQResult(top5Countries)
	resultsWithId := models.ResultWithId{
		Id:      top5Countries.ClientId,
		Results: results,
	}

	return &resultsWithId, nil
}

func (g *Gateway) handleResults3(msg middleware.Message) (*models.ResultWithId, error) {
	var bestAndWorstMovies common.BestAndWorstMovies
	if err := json.Unmarshal(msg.Body, &bestAndWorstMovies); err != nil {
		return nil, fmt.Errorf("error unmarshalling best and worst movies: %w", err)
	}

	results := BestAndWorstToQResult(bestAndWorstMovies)
	resultsWithId := models.ResultWithId{
		Id:      bestAndWorstMovies.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResults4(msg middleware.Message) (*models.ResultWithId, error) {
	var top10Actors common.Top10Actors
	if err := json.Unmarshal(msg.Body, &top10Actors); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 10 actors: %w", err)
	}

	results := Top10ActorsToQResult(top10Actors)
	resultsWithId := models.ResultWithId{
		Id:      top10Actors.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResults5(msg middleware.Message) (*models.ResultWithId, error) {
	var sentimentProfitRatio common.SentimentProfitRatioAverage
	if err := json.Unmarshal(msg.Body, &sentimentProfitRatio); err != nil {
		return nil, fmt.Errorf("error unmarshalling sentiment profit ratio: %w", err)
	}

	results := SentimentToQResult(sentimentProfitRatio)
	resultsWithId := models.ResultWithId{
		Id:      sentimentProfitRatio.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResult(msg middleware.Message, query int) error {
	defer msg.Ack()
	defer g.ClientMutex.Unlock()
	var results *models.ResultWithId
	var err error
	switch query {
	case 1:
		results, err = g.handleResult1(msg)
	case 2:
		results, err = g.handleResults2(msg)
	case 3:
		results, err = g.handleResults3(msg)
	case 4:
		results, err = g.handleResults4(msg)
	case 5:
		results, err = g.handleResults5(msg)
	}

	if err != nil {
		return fmt.Errorf("error handling results in query %d err: %s", query, err)
	}

	if results != nil { // can be nil due to empty results in query 1
		slog.Info("Received results", slog.Int("query", query), slog.String("client_id", results.Id))
		g.ClientMutex.Lock()
		client, ok := g.clients[results.Id]

		if !ok {
			return nil
		}

		if !client.IsDead() {
			client.sendResult(&results.Results)
		}
	}
	return nil
}

func (g *Gateway) consumeBatch(msg []byte) (common.Batch[common.Movie], error) {
	var batch common.Batch[common.Movie]
	if err := json.Unmarshal(msg, &batch); err != nil {
		return batch, fmt.Errorf("error unmarshalling result: %w", err)
	}
	return batch, nil
}

func (g *Gateway) saveClients() error {
	clients := make([]common.FlushClient, 0, len(g.clients))
	for id := range g.clients {
		clients = append(clients, common.FlushClient{ClientID: id})
	}

	data, err := json.Marshal(clients)
	if err != nil {
		return fmt.Errorf("error marshalling clients: %w", err)
	}

	err = common.AtomicWriteFile(dataPath+clientsFile, data, nil)
	if err != nil {
		return fmt.Errorf("error writing clients to file: %w", err)
	}
	return nil
}

type deadState struct {
	ClientID          string
	finishedCorrectly bool
}

func (g *Gateway) persistencyHandler(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case deadClientState := <-g.deadChan:
			slog.Debug("Received dead client in handler", slog.String("client_id", deadClientState.ClientID))
			if _, ok := g.clients[deadClientState.ClientID]; ok {
				slog.Debug("Removing dead client", slog.String("client_id", deadClientState.ClientID))
				g.ClientMutex.Lock()
				delete(g.clients, deadClientState.ClientID)
				err := g.saveClients()
				g.ClientMutex.Unlock()
				if err != nil {
					slog.Error("error saving clients after removing dead client", slog.String("error", err.Error()))
				} else {
					slog.Info("Dead client removed and clients saved", slog.String("client_id", deadClientState.ClientID))
				}
				if !deadClientState.finishedCorrectly {
					flushMsg := common.FlushClient{ClientID: deadClientState.ClientID}
					data, err := json.Marshal(flushMsg)
					if err != nil {
						slog.Error("error marshalling dead client for flush", slog.String("error", err.Error()))
					} else {
						if err := g.flushQueue.Send(data); err != nil {
							slog.Error("error sending dead client to flush", slog.String("error", err.Error()))
						} else {
							slog.Info("Dead client sent to flush", slog.String("client_id", deadClientState.ClientID))
						}
					}
				}
			} else {
				slog.Warn("Dead client not found in clients map", slog.String("client_id", deadClientState.ClientID))
			}
		case <-g.ctx.Done():
			return
		}
	}
}
