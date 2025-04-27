package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/signal"
	"pkg/communication"
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
	client        net.Conn
	running       bool
	ctx           context.Context
	done          uint8
}

func NewGateway(rabbitUser, rabbitPass, port string) (*Gateway, error) {
	config := NewGatewayConfig(rabbitUser, rabbitPass, port)
	gateway := &Gateway{
		config:        config,
		running:       true,
		resultsQueues: make(map[int]<-chan common.Message),
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
	for g.running {
		slog.Info("Waiting for client connection")
		conn, err := g.listener.Accept()
		if err != nil {
			if g.running { // only log if not shutting down
				slog.Error("error accepting connection", slog.String("error", err.Error()))
			}
			return
		}
		deadChan := make(chan bool)
		g.client = conn
		go g.handleConnection(deadChan) //Handle connection will always finish before the next accept
		g.processMessages(deadChan)
	}
}

func (g *Gateway) receiveMovies() error {
	total := 0
	for {
		movies, err := communication.RecvMovies(g.client)
		if err != nil {
			return fmt.Errorf("error receiving movies: %w", err)
		}

		err = g.publishBatch(movies, "movies")
		if err != nil {
			return fmt.Errorf("error publishing movies batch: %w", err)
		}

		total += int(movies.Header.Weight)

		if movies.IsEof() {
			break
		}
	}
	slog.Info("Total movies received", slog.Int("total", total))
	return nil
}

func (g *Gateway) receiveReviews() error {
	total := 0
	for {
		reviews, err := communication.RecvReviews(g.client)
		if err != nil {
			return fmt.Errorf("error receiving reviews: %w", err)
		}

		err = g.publishBatch(reviews, "reviews")
		if err != nil {
			return fmt.Errorf("error publishing reviews batch: %w", err)
		}

		total += int(reviews.Header.Weight)

		if reviews.IsEof() {
			break
		}
	}
	slog.Info("Total reviews received", slog.Int("total", total))
	return nil
}

func (g *Gateway) receiveCredits() error {
	total := 0
	for {
		credits, err := communication.RecvCredits(g.client)
		if err != nil {
			return fmt.Errorf("error receiving credits: %w", err)
		}

		err = g.publishBatch(credits, "credits")
		if err != nil {
			return fmt.Errorf("error publishing credits batch: %w", err)
		}

		total += int(credits.Header.Weight)

		if credits.IsEof() {
			break
		}
	}
	slog.Info("Total credits received", slog.Int("total", total))
	return nil
}

func (g *Gateway) publishBatch(batch models.RawBatch, batchType string) error {
	bodyBytes, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("error marshalling batch: %w", err)
	}

	rawBatch := common.ToProcessMsg{
		Type: batchType,
		Body: bodyBytes,
	}

	batchToSend, err := json.Marshal(rawBatch)
	if err != nil {
		return fmt.Errorf("error marshalling raw batch: %w", err)
	}

	g.toPreprocess <- batchToSend
	return nil
}

func (g *Gateway) handleConnection(deadChan chan bool) {
	slog.Info("Client connected", slog.String("address", g.client.RemoteAddr().String()))
	err := g.receiveMovies()
	if err != nil {
		if g.running {
			if !errors.Is(err, io.EOF) {
				slog.Error("error receiving movies", slog.String("error", err.Error()))
			} else {
				deadChan <- true
			}
		}
		return
	}

	err = g.receiveReviews()
	if err != nil {
		if g.running {
			if !errors.Is(err, io.EOF) {
				slog.Error("error receiving reviews", slog.String("error", err.Error()))
			} else {
				deadChan <- true
			}
		}
		return
	}

	err = g.receiveCredits()
	if err != nil {
		if g.running {
			if !errors.Is(err, io.EOF) {
				slog.Error("error receiving credits", slog.String("error", err.Error()))
			} else {
				deadChan <- true
			}
		}
		return
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

	wg.Add(1)
	go g.signalHandler(wg)
	g.listen()
	wg.Wait()

	slog.Info("Gateway shut down")
}

const TotalQueries uint8 = 5

func (g *Gateway) processMessages(deadChan chan bool) {
	defer func(client net.Conn) {
		err := client.Close()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Error("error closing client connection in process", slog.String("error", err.Error()))
			}
			return
		}
		slog.Info("client connection closed", slog.String("address", client.RemoteAddr().String()))
	}(g.client)

	for {
		if g.done == TotalQueries {
			slog.Info("All queries processed for client", slog.String("address", g.client.RemoteAddr().String()))
			break
		}
		var err error
		select {
		case <-deadChan:
			slog.Info("Client Disconnected while sending data")
			err := g.publishCleanupBatch()
			if err != nil {
				slog.Error("error publishing cleanup batch", slog.String("error", err.Error()))
			}
			return
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
			if errors.Is(err, io.EOF) {
				slog.Info("Client disconnected", slog.String("address", g.client.RemoteAddr().String()))
			} else {
				slog.Error("error processing message", slog.String("error", err.Error()))
			}
			return
		}
	}
	g.done = 0
}

func (g *Gateway) handleResult1(msg common.Message) error {
	batch, err := g.consumeBatch(msg.Body)
	if err != nil {
		return fmt.Errorf("error consuming results: %w", err)
	}

	if !(len(batch.Data) == 0 && !batch.IsEof()) {
		err = g.processResult1(batch)
		if err != nil {
			return fmt.Errorf("error processing result: %w", err)
		}
	}

	if err := msg.Ack(); err != nil {
		return fmt.Errorf("error acknowledging message: %w", err)
	}

	return nil
}

func (g *Gateway) processResult1(batch common.Batch[common.Movie]) error {
	if batch.IsEof() {
		slog.Info("EOF message received", slog.Any("headers", batch.Header))
		err := communication.SendQueryEof(g.client, 1)
		if err != nil {
			return fmt.Errorf("error sending EOF: %w", err)
		}
		g.done++
	} else {
		q1Movies := make([]models.QueryResult, len(batch.Data))
		for i, movie := range batch.Data {
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

func (g *Gateway) handleResults2(msg common.Message) ([]models.QueryResult, error) {
	var top5Countries common.Top5Countries
	if err := json.Unmarshal(msg.Body, &top5Countries); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 5 countries: %w", err)
	}

	if len(top5Countries.Countries) != 5 {
		return nil, fmt.Errorf("expected 5 countries, got %d", len(top5Countries.Countries))
	}

	slog.Info("Top 5 countries", slog.Any("top5Countries", top5Countries))

	q2Result := []models.QueryResult{
		models.Q2Country{Country: top5Countries.Countries[0].Country, Budget: top5Countries.Countries[0].Budget},
		models.Q2Country{Country: top5Countries.Countries[1].Country, Budget: top5Countries.Countries[1].Budget},
		models.Q2Country{Country: top5Countries.Countries[2].Country, Budget: top5Countries.Countries[2].Budget},
		models.Q2Country{Country: top5Countries.Countries[3].Country, Budget: top5Countries.Countries[3].Budget},
		models.Q2Country{Country: top5Countries.Countries[4].Country, Budget: top5Countries.Countries[4].Budget},
	}
	return q2Result, nil
}

func (g *Gateway) handleResults3(msg common.Message) ([]models.QueryResult, error) {
	var bestAndWorstMovies common.BestAndWorstMovies
	if err := json.Unmarshal(msg.Body, &bestAndWorstMovies); err != nil {
		return nil, fmt.Errorf("error unmarshalling best and worst movies: %w", err)
	}
	slog.Info("Best and worst movies", slog.Any("bestAndWorstMovies", bestAndWorstMovies))
	q3Result := []models.QueryResult{
		models.Q3Result{
			Best: models.Q3Movie{
				ID:     bestAndWorstMovies.BestMovie.MovieID,
				Title:  bestAndWorstMovies.BestMovie.Title,
				Rating: float32(bestAndWorstMovies.BestMovie.Rating),
			},
			Worst: models.Q3Movie{
				ID:     bestAndWorstMovies.WorstMovie.MovieID,
				Title:  bestAndWorstMovies.WorstMovie.Title,
				Rating: float32(bestAndWorstMovies.WorstMovie.Rating),
			},
		},
	}
	return q3Result, nil
}

func (g *Gateway) handleResults4(msg common.Message) ([]models.QueryResult, error) {
	var top10Actors common.Top10Actors
	if err := json.Unmarshal(msg.Body, &top10Actors); err != nil {
		return nil, fmt.Errorf("error unmarshalling top 10 actors: %w", err)
	}

	slog.Info("Top 10 actors", slog.Any("top10Actors", top10Actors))
	q4Result := make([]models.QueryResult, len(top10Actors.TopActors))
	for i, actor := range top10Actors.TopActors {
		q4Result[i] = models.Q4Actors{
			ActorId:     actor.ActorID,
			ActorName:   actor.ActorName,
			Appearances: actor.MoviesAmount,
		}
	}

	return q4Result, nil
}

func (g *Gateway) handleResults5(msg common.Message) ([]models.QueryResult, error) {
	var sentimentProfitRatio common.SentimentProfitRatioAverage
	if err := json.Unmarshal(msg.Body, &sentimentProfitRatio); err != nil {
		return nil, fmt.Errorf("error unmarshalling sentiment profit ratio: %w", err)
	}

	slog.Info("received query 5 results", slog.Any("sentiment profit ratio", sentimentProfitRatio))

	q5Result := []models.QueryResult{
		models.Q5Avg{
			PositiveAvgProfitRatio: sentimentProfitRatio.PositiveAvgProfitRatio,
			NegativeAvgProfitRatio: sentimentProfitRatio.NegativeAvgProfitRatio,
		},
	}

	return q5Result, nil
}

func (g *Gateway) handleResult(msg common.Message, query int) error {
	var results []models.QueryResult
	var err error
	switch query {
	case 1:
		// Case 1 is weird, TODO: change it
		err = g.handleResult1(msg)
		if err != nil {
			return fmt.Errorf("error handling results in query %d err: %s", query, err)
		}
		return nil
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

	err = communication.SendQueryResults(g.client, query, results)
	if err != nil {
		return fmt.Errorf("error sending query results: %w", err)
	}

	g.done++

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

func (g *Gateway) publishCleanupBatch() error {
	// TODO: tidy up this code
	cleanHeader := models.Header{
		Weight:      0,
		TotalWeight: -2,
	}

	rawMovies := models.RawMovieBatch{
		Header: cleanHeader,
		Movies: nil,
	}

	err := g.publishBatch(rawMovies, "movies")
	if err != nil {
		return fmt.Errorf("error publishing movies batch: %w", err)
	}

	rawReviews := models.RawReviewBatch{
		Header:  cleanHeader,
		Reviews: nil,
	}
	err = g.publishBatch(rawReviews, "reviews")
	if err != nil {
		return fmt.Errorf("error publishing reviews batch: %w", err)
	}

	rawCredits := models.RawCreditBatch{
		Header:  cleanHeader,
		Credits: nil,
	}

	err = g.publishBatch(rawCredits, "credits")
	if err != nil {
		return fmt.Errorf("error publishing credits batch: %w", err)
	}
	return nil
}
