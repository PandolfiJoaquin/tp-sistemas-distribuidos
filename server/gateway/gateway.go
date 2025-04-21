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

const (
	nextStep = "to-preprocess"

	qres1 = "q1-results"
	qres2 = "q2-results"
	qres3 = "q3-results"
	qres4 = "q4-results"
	qres5 = "q5-results"
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
	// Listener will only accept one connection at a time //TODO: check to do one connection per queries
	for g.running {
		conn, err := g.listener.Accept()
		if err != nil {
			if g.running { // only log if not shutting down
				slog.Error("error accepting connection", slog.String("error", err.Error()))
			}
			return
		}
		g.client = conn
		go g.handleConnection()
		g.processMessages()
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

func (g *Gateway) handleConnection() {
	slog.Info("Client connected", slog.String("address", g.client.RemoteAddr().String()))
	err := g.receiveMovies()
	if err != nil {
		slog.Error("error receiving movies", slog.String("error", err.Error()))
		return
	}

	err = g.receiveReviews()
	if err != nil {
		slog.Error("error receiving reviews", slog.String("error", err.Error()))
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

	wg.Add(1)
	go g.signalHandler(wg)
	g.listen()
	wg.Wait()
}

var total_queries = 1

func (g *Gateway) processMessages() {
	defer func(client net.Conn) {
		err := client.Close()
		if err != nil {

		}
	}(g.client)

	for {
		select {
		case <-g.ctx.Done():
			return

		case msg := <-g.resultsQueues[1]:
			if err := g.handleResult1(msg); err != nil {
				slog.Error("error handling result 1", slog.String("error", err.Error()))
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		case msg := <-g.resultsQueues[2]:
			if err := g.handleResults2(msg); err != nil {
				slog.Error("error handling result 2", slog.String("error", err.Error()))
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		case msg := <-g.resultsQueues[3]:
			err := g.handleResults3(msg)
			if err != nil {
				slog.Error("error handling result 3", slog.String("error", err.Error()))
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		case _ = <-g.resultsQueues[4]:
			// TODO: Handle query 4 results
		case msg := <-g.resultsQueues[5]:
			if err := g.handleResult5(msg); err != nil {
				slog.Error("error handling result 5", slog.String("error", err.Error()))
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		}
	}
}

func (g *Gateway) handleResult1(msg common.Message) error {
	batch, err := g.consumeBatch(msg.Body)
	if err != nil {
		return fmt.Errorf("error consuming results: %w", err)
	}

	if len(batch.Data) == 0 && !batch.IsEof() {
		return nil
	}

	err = g.processResult1(batch)
	if err != nil {
		return fmt.Errorf("error processing result: %w", err)
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

func (g *Gateway) handleResults2(msg common.Message) error {
	var top5Countries common.Top5Countries
	if err := json.Unmarshal(msg.Body, &top5Countries); err != nil {
		return fmt.Errorf("error unmarshalling top 5 countries: %w", err)
	}

	if len(top5Countries.Countries) != 5 {
		return fmt.Errorf("expected 5 countries, got %d", len(top5Countries.Countries))
	}

	slog.Info("Top 5 countries", slog.Any("top5Countries", top5Countries))

	q2Result := []models.QueryResult{
		models.Q2Country{Country: top5Countries.Countries[0].Country, Budget: top5Countries.Countries[0].Budget},
		models.Q2Country{Country: top5Countries.Countries[1].Country, Budget: top5Countries.Countries[1].Budget},
		models.Q2Country{Country: top5Countries.Countries[2].Country, Budget: top5Countries.Countries[2].Budget},
		models.Q2Country{Country: top5Countries.Countries[3].Country, Budget: top5Countries.Countries[3].Budget},
		models.Q2Country{Country: top5Countries.Countries[4].Country, Budget: top5Countries.Countries[4].Budget},
	}

	err := communication.SendQueryResults(g.client, 2, q2Result)
	if err != nil {
		return fmt.Errorf("error sending query results: %w", err)
	}
	return nil
}

func (g *Gateway) handleResults3(msg common.Message) error {
	var bestAndWorstMovies common.BestAndWorstMovies
	if err := json.Unmarshal(msg.Body, &bestAndWorstMovies); err != nil {
		return fmt.Errorf("error unmarshalling best and worst movies: %w", err)
	}

	slog.Info("Best and worst movies", slog.Any("bestAndWorstMovies", bestAndWorstMovies))
	q3Result := []models.QueryResult{
		models.Q3Movie{ID: bestAndWorstMovies.BestMovie.ID, Title: bestAndWorstMovies.BestMovie.Title},
		models.Q3Movie{ID: bestAndWorstMovies.WorstMovie.ID, Title: bestAndWorstMovies.WorstMovie.Title},
	}

	err := communication.SendQueryResults(g.client, 3, q3Result)
	if err != nil {
		return fmt.Errorf("error sending query results: %w", err)
	}

	return nil
}

func (g *Gateway) handleResult5(msg common.Message) error {
	var sentimentProfitRatio common.SentimentProfitRatioAverage
	if err := json.Unmarshal(msg.Body, &sentimentProfitRatio); err != nil {
		return fmt.Errorf("error unmarshalling sentiment profit ratio: %w", err)
	}

	slog.Info("received query 5 results", slog.Any("sentiment profit ratio", sentimentProfitRatio))

	q5Result := []models.QueryResult{
		models.Q5Avg{
			PositiveAvgProfitRatio: sentimentProfitRatio.PositiveAvgProfitRatio,
			NegativeAvgProfitRatio: sentimentProfitRatio.NegativeAvgProfitRatio,
		},
	}

	err := communication.SendQueryResults(g.client, 5, q5Result)
	if err != nil {
		return fmt.Errorf("error sending query results: %w", err)
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

const TotalQueries = 1
