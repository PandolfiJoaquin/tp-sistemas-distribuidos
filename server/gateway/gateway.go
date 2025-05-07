package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"pkg/models"
	"sync"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
)

const (
	nextStep = "to-preprocess"
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
	middleware    *common.Middleware
	resultsQueues map[int]<-chan common.Message
	toPreprocess  chan<- []byte
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
		resultsQueues: make(map[int]<-chan common.Message),
		clients:       make(map[string]*Client),
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

	processorChan, err := g.middleware.GetChanToSend(nextStep)
	if err != nil {
		slog.Error("error getting channel to send", slog.String("queue", nextStep), slog.String("error", err.Error()))
		return err
	}

	for i := 1; i <= 5; i++ {
		resultsChan, err := g.middleware.GetChanToRecv(fmt.Sprintf("q%d-results", i))
		if err != nil {
			slog.Error("error getting channel to receive", slog.String("queue", fmt.Sprintf("q%d-results", i)), slog.String("error", err.Error()))
			return err
		}
		g.resultsQueues[i] = resultsChan
	}

	g.toPreprocess = processorChan

	return nil
}

func (g *Gateway) listen() {
	for g.running { // TODO: REAP DEAD CLIENTS
		slog.Info("Waiting for client connection")
		conn, err := g.listener.Accept()
		if err != nil {
			if g.running { // only log if not shutting down
				slog.Error("error accepting connection", slog.String("error", err.Error()))
			}
			return
		}
		client := NewClient(conn, &g.toPreprocess)
		slog.Info("Client connected", slog.String("address", conn.RemoteAddr().String()))
		g.clients[client.GetId()] = client
		fmt.Printf("Client %s connected\n", client.GetId())
		go client.Run()
	}
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
		slog.Info("listener closed")
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
	go g.processMessages()
	g.listen()
	wg.Wait()

	// TODO: Do shutdown of all clients
	slog.Info("Gateway shut down")
}

func (g *Gateway) processMessages() {
	for {
		var err error
		select {
		case <-g.ctx.Done():
			return

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
	}
}

func (g *Gateway) handleResult1(msg common.Message) (*models.ResultWithId, error) {
	batch, err := g.consumeBatch(msg.Body)
	if err != nil {
		return nil, fmt.Errorf("error consuming results: %w", err)
	}

	if len(batch.Data) == 0 && !batch.IsEof() {
		return nil, nil
	}

	results := MovieToQResult(batch)
	resultsWithId := models.ResultWithId{
		Id:      batch.Header.ClientID,
		Results: results,
	}

	return &resultsWithId, err
}

func (g *Gateway) handleResults2(msg common.Message) (*models.ResultWithId, error) {
	var top5Countries common.Top5Countries
	if err := json.Unmarshal(msg.Body, &top5Countries); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 5 countries: %w", err)
	}

	if len(top5Countries.Countries) != 5 {
		return nil, fmt.Errorf("expected 5 countries, got %d", len(top5Countries.Countries))
	}

	slog.Info("Top 5 countries", slog.Any("top5Countries", top5Countries))

	results := Top5CountriesToQResult(top5Countries)
	resultsWithId := models.ResultWithId{
		Id:      top5Countries.ClientId,
		Results: results,
	}

	return &resultsWithId, nil
}

func (g *Gateway) handleResults3(msg common.Message) (*models.ResultWithId, error) {
	var bestAndWorstMovies common.BestAndWorstMovies
	if err := json.Unmarshal(msg.Body, &bestAndWorstMovies); err != nil {
		return nil, fmt.Errorf("error unmarshalling best and worst movies: %w", err)
	}
	slog.Info("Best and worst movies", slog.Any("bestAndWorstMovies", bestAndWorstMovies))

	results := BestAndWorstToQResult(bestAndWorstMovies)
	resultsWithId := models.ResultWithId{
		Id:      bestAndWorstMovies.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResults4(msg common.Message) (*models.ResultWithId, error) {
	var top10Actors common.Top10Actors
	if err := json.Unmarshal(msg.Body, &top10Actors); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 10 actors: %w", err)
	}

	slog.Info("Top 10 actors", slog.Any("top10Actors", top10Actors))

	results := Top10ActorsToQResult(top10Actors)
	resultsWithId := models.ResultWithId{
		Id:      top10Actors.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResults5(msg common.Message) (*models.ResultWithId, error) {
	var sentimentProfitRatio common.SentimentProfitRatioAverage
	if err := json.Unmarshal(msg.Body, &sentimentProfitRatio); err != nil {
		return nil, fmt.Errorf("error unmarshalling sentiment profit ratio: %w", err)
	}

	slog.Info("received query 5 results", slog.Any("sentiment profit ratio", sentimentProfitRatio))

	results := SentimentToQResult(sentimentProfitRatio)
	resultsWithId := models.ResultWithId{
		Id:      sentimentProfitRatio.ClientId,
		Results: results,
	}
	return &resultsWithId, nil
}

func (g *Gateway) handleResult(msg common.Message, query int) error {
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
		slog.Info("Received results", slog.String("clientId", results.Id), slog.Int("query", query))
		client, ok := g.clients[results.Id]
		if !ok {
			return fmt.Errorf("client %s not found", results.Id)
		}
		if !client.IsDead() {
			client.sendResult(&results.Results)
		}

	}

	if err := msg.Ack(); err != nil {
		return fmt.Errorf("error acknowledging message: %w", err)
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

//func (g *Gateway) publishCleanupBatch() error {
//	// TODO: tidy up this code
//	cleanHeader := models.Header{
//		Weight:      0,
//		TotalWeight: -2,
//	}
//
//	emptyBatch := models.RawBatch[any]{ // no data
//		Header: cleanHeader,
//	}
//
//	err := publishBatch(emptyBatch, "movies", g.toPreprocess)
//	if err != nil {
//		return fmt.Errorf("error publishing movies batch: %w", err)
//	}
//
//	err = publishBatch(emptyBatch, "reviews", g.toPreprocess)
//	if err != nil {
//		return fmt.Errorf("error publishing reviews batch: %w", err)
//	}
//
//	err = publishBatch(emptyBatch, "credits", g.toPreprocess)
//	if err != nil {
//		return fmt.Errorf("error publishing credits batch: %w", err)
//	}
//	return nil
//}
