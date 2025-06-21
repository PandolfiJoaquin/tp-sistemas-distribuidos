package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"pkg/models"
	"syscall"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/middleware"
)

const (
	rabbitHost      = "rabbitmq"
	toPreProcess    = "to-preprocess"
	filterMoviesQ1  = "filter-year-q1"
	filterMoviesQ2  = "filter-production-q2"
	filterMovieQ3Q4 = "filter-year-q3q4"
	AnalyzerQueue   = "sentiment-analyzer"
	reviewsTopic    = "reviews-to-join-%d"
	reviewsExchange = "reviews-exchange"
	creditsTopic    = "credits-to-join-%d"
	creditsExchange = "credits-exchange"
)

type PreprocessorConfig struct {
	RabbitUser string
	RabbitPass string
}

type Preprocessor struct {
	config           PreprocessorConfig
	m                *middleware.Middleware
	toProcessChan    <-chan middleware.Message
	shards           int
	reviewsQueues    map[int]middleware.SenderQueue
	creditsQueues    map[int]middleware.SenderQueue
	moviesQueues     []middleware.SenderQueue
	pesoTotalQuePaso int
}

func NewPreprocessor(rabbitUser string, rabbitPass string, shards int) *Preprocessor {
	config := PreprocessorConfig{
		RabbitUser: rabbitUser,
		RabbitPass: rabbitPass,
	}

	Preprocessor := &Preprocessor{
		config:        config,
		reviewsQueues: map[int]middleware.SenderQueue{},
		creditsQueues: map[int]middleware.SenderQueue{},
		shards:        shards,
	}

	err := Preprocessor.middlewareSetup()
	if err != nil {
		slog.Error("error setting up middleware", slog.String("error", err.Error()))
		return nil
	}

	return Preprocessor
}

func (p *Preprocessor) middlewareSetup() error {
	// Setup m connection
	m, err := middleware.NewMiddleware(p.config.RabbitUser, p.config.RabbitPass, rabbitHost)
	if err != nil {
		return fmt.Errorf("error creating middleware: %s", err)
	}

	for i := range p.shards {
		shard := i + 1
		slog.Info("Creating channel to send reviews", slog.Int("shard", shard))
		p.reviewsQueues[shard], err = m.GetQueueWithTopicToSend(reviewsExchange, fmt.Sprintf(reviewsTopic, shard))
		if err != nil {
			return fmt.Errorf("error getting channel to send reviews: %s", err)
		}
		p.creditsQueues[shard], err = m.GetQueueWithTopicToSend(creditsExchange, fmt.Sprintf(creditsTopic, shard))
		if err != nil {
			return fmt.Errorf("error getting channel to send credits: %s", err)
		}
	}

	moviesQueues := make([]middleware.SenderQueue, 0, 4)
	queues := []string{
		filterMoviesQ1,
		filterMoviesQ2,
		filterMovieQ3Q4,
		AnalyzerQueue,
	}

	for _, q := range queues {
		ch, err := m.GetQueueToSend(q)
		if err != nil {
			return fmt.Errorf("error getting channel to send movies: %s", err)
		}
		moviesQueues = append(moviesQueues, ch)
	}

	toProcess, err := m.GetChanToRecv(toPreProcess)
	if err != nil {
		return fmt.Errorf("error getting channel to receive: %s", err)
	}

	p.m = m
	p.toProcessChan = toProcess
	p.moviesQueues = moviesQueues

	return nil
}

func (p *Preprocessor) close() {
	if err := p.m.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
}

func (p *Preprocessor) Start() {
	defer p.close()

	// Sigterm , sigint
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	p.processMessages(ctx)
}

func (p *Preprocessor) processMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("Received shutdown signal, stopping...")
			return
		case msg := <-p.toProcessChan:
			var batch common.ToProcessMsg

			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				return
			}

			if err := p.preprocessBatch(batch); err != nil {
				slog.Error("error preprocessing batch", slog.String("error", err.Error()))
				return
			}

			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
				return
			}
		}
	}
}

func (p *Preprocessor) preprocessBatch(msg common.ToProcessMsg) error {
	switch msg.Type {
	case "movies":
		var mb models.RawBatch[models.RawMovie]
		if err := json.Unmarshal(msg.Body, &mb); err != nil {
			return fmt.Errorf("movies unmarshal: %w", err)
		}

		var payload any
		if mb.IsEof() {
			payload = makeEOFBatch[common.Movie](mb.Header.TotalWeight, msg.ClientId, mb.Header.BatchID)
		} else {
			payload = preprocessMovies(mb, msg.ClientId)
		}

		data, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal movies: %w", err)
		}
		for _, ch := range p.moviesQueues {
			// Check Q5 maybe
			if err := ch.Send(data); err != nil {
				return fmt.Errorf("sending movies: %w", err)
			}
		}
		//slog.Debug("preprocessing movies", slog.Int("size", int(mb.Header.Weight)))

	case "reviews":
		var rb models.RawBatch[models.RawReview]
		if err := json.Unmarshal(msg.Body, &rb); err != nil {
			return fmt.Errorf("reviews unmarshal: %w", err)
		}

		batch := preprocessReviews(rb, msg.ClientId)
		if err := sendBatchMap(
			batch,
			p.shards,
			p.reviewsQueues,
			func(r common.Review) string { return r.MovieID },
		); err != nil {
			return fmt.Errorf("sending reviews: %w", err)
		}
		//slog.Debug("preprocessing reviews", slog.Int("size", int(rb.Header.Weight)))

	case "credits":
		var cb models.RawBatch[models.RawCredits]
		if err := json.Unmarshal(msg.Body, &cb); err != nil {
			return fmt.Errorf("credits unmarshal: %w", err)
		}

		batch := preprocessCredits(cb, msg.ClientId)
		if err := sendBatchMap(
			batch,
			p.shards,
			p.creditsQueues,
			func(c common.Credit) string { return c.MovieId },
		); err != nil {
			return fmt.Errorf("sending credits: %w", err)
		}
		//slog.Debug("preprocessing credits", slog.Int("size", int(cb.Header.Weight)))

	default:
		return fmt.Errorf("unknown batch type %q", msg.Type)
	}

	return nil
}

func makeEOFBatch[T any](totalWeight int32, clientID string, batchID int) common.Batch[T] {
	return common.Batch[T]{
		Header: common.Header{
			Weight:      0,
			TotalWeight: totalWeight,
			ClientID:    clientID,
			MessageID: common.BatchID{
				ID:       batchID,
				JoinerID: -1,
			},
		},
		Data: []T{},
	}
}

func makeBatchMsg[T any](weight uint32, data []T, totalWeight int32, clientID string, batchID int) common.Batch[T] {
	return common.Batch[T]{
		Header: common.Header{
			Weight:      weight,
			TotalWeight: totalWeight,
			ClientID:    clientID,
			MessageID: common.BatchID{
				ID:       batchID,
				JoinerID: -1,
			},
		},
		Data: data,
	}
}

func divideBatchInShards[T any](batch common.Batch[T], shards int, getKey func(T) string) []common.Batch[T] {
	bucketShards := make([]common.Batch[T], shards)
	for i := 0; i < shards; i++ {
		bucketShards[i] = common.Batch[T]{
			Header: batch.Header,
			Data: common.Filter(batch.Data, func(item T) bool {
				shardID := common.GetShard(getKey(item), shards)
				return shardID == i+1
			}),
		}
	}
	return bucketShards
}

// sendBatchMap marshals either an EOF batch or normal sharded batches and sends them
// to chans[1]...chans[shards]. Assumes map keys 1..shards exist.
func sendBatchMap[T any](batch common.Batch[T], shards int, chans map[int]middleware.SenderQueue, getKey func(T) string) error {
	if batch.IsEof() {
		data, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("marshal EOF: %w", err)
		}
		for id := 1; id <= shards; id++ {
			ch, ok := chans[id]
			if !ok {
				return fmt.Errorf("missing chan for shard %d", id)
			}
			if err := ch.Send(data); err != nil {
				return fmt.Errorf("sending batch: %w", err)
			}
		}
		return nil
	}

	shardsBatches := divideBatchInShards(batch, shards, getKey)
	for id := 1; id <= shards; id++ {
		data, err := json.Marshal(shardsBatches[id-1])
		if err != nil {
			return fmt.Errorf("marshal shard %d: %w", id, err)
		}
		ch, ok := chans[id]
		if !ok {
			return fmt.Errorf("missing chan for shard %d", id)
		}
		if err := ch.Send(data); err != nil {
			return fmt.Errorf("sending batch: %w", err)
		}
	}
	return nil
}
