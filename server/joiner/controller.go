package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"tp-sistemas-distribuidos/server/common"
	"tp-sistemas-distribuidos/server/common/middleware"
	"tp-sistemas-distribuidos/server/common/persistency"
)

const (
	rabbitHost = "rabbitmq"
	//rabbitHost           = "127.0.0.1"
	moviesExchange       = "movies-exchange"
	moviestopic          = "movies-to-join-%d"
	reviewsExchange      = "reviews-exchange"
	reviewsTopic         = "reviews-to-join-%d"
	creditExchange       = "credits-exchange"
	creditTopic          = "credits-to-join-%d"
	q3ToReduceQueue      = "q3-to-reduce"
	q4ToReduceQueue      = "q4-to-reduce"
	maxTransactionsOnLog = 500
	separator            = ";"
	blacklistDuration    = 300 // 5 minutes
	flushExchange        = "flush-exchange"
	heartbeatInterval    = 1 * time.Second
)

type LogOperations string

const ( //TODO: Optimize encoding
	UpdateMoviesWeightsOp  = "update-movies-weights"
	SaveMoviesOp           = "save-movies"
	UpdateReviewsWeightsOp = "update-reviews-weights"
	StoreReviewBatchOp     = "store-review-batch"
	StoreCreditBatchOp     = "store-credit-batch"
	JoinStoredBatchesOp    = "join-stored-batches"
	UpdateCreditsWeightsOp = "update-credits-weights"
	FilterMovieOP          = "filter-movie"
	FilterReviewsOP        = "filter-reviews"
	FilterCreditsOP        = "filter-credits"
	DeleteClientOP         = "delete-client"
)

type JoinerController struct {
	joinerId            int
	m                   *middleware.Middleware
	Sessions            map[string]*JoinerSession                `json:"sessions"`
	StoredReviewBatches map[string][]common.Batch[common.Review] `json:"stored_review_batches"`
	StoredCreditBatches map[string][]common.Batch[common.Credit] `json:"stored_credit_batches"`
	BlacklistedClients  map[string]int64                         `json:"blacklisted_clients"`
	flushQueue          <-chan middleware.Message
	transactionsOnLog   int
	persistencyHandler  *persistency.PersistencyHandler[JoinerController]
	q3ToReduce          middleware.SenderQueue
	q4ToReduce          middleware.SenderQueue
	moviesChan          <-chan middleware.Message
	reviewsChan         <-chan middleware.Message
	creditChan          <-chan middleware.Message
}

func (j JoinerController) ApplyFunc(entry persistency.TransactionEntry) (JoinerController, error) {
	switch entry.Op {
	case UpdateMoviesWeightsOp:
		header, err := common.HeaderFromString(entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing header %v: %w", UpdateMoviesWeightsOp, err)
		}
		session := j.getSession(header.ClientID)
		session.UpdateMoviesWeights(header)
		return j, nil
	case SaveMoviesOp:
		batch, err := common.GetBatchFromString[common.Movie](entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing batch %v: %w", SaveMoviesOp, err)
		}
		session := j.getSession(batch.Header.ClientID)
		session.SaveMovies(batch.Data)
		return j, nil
	case UpdateReviewsWeightsOp:
		header, err := common.HeaderFromString(entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing header %v: %w", UpdateReviewsWeightsOp, err)
		}
		session := j.getSession(header.ClientID)
		session.UpdateReviewsWeights(header)
		return j, nil
	case StoreReviewBatchOp:
		batch, err := common.GetBatchFromString[common.Review](entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing batch %v: %w", StoreReviewBatchOp, err)
		}
		j.storeReviewBatch(batch.Header.ClientID, batch.AsBatch())
		return j, nil
	case JoinStoredBatchesOp:
		clientId := entry.Args
		for _, batch := range j.StoredReviewBatches[clientId] {
			session := j.getSession(clientId)
			session.UpdateReviewsWeights(batch.Header)
		}
		delete(j.StoredReviewBatches, clientId)
		for _, batch := range j.StoredCreditBatches[clientId] {
			session := j.getSession(clientId)
			session.UpdateCreditsWeights(batch.Header)
		}
		delete(j.StoredCreditBatches, clientId)
		return j, nil
	case UpdateCreditsWeightsOp:
		header, err := common.HeaderFromString(entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing header %v: %w", UpdateCreditsWeightsOp, err)
		}
		session := j.getSession(header.ClientID)
		session.UpdateCreditsWeights(header)
		return j, nil
	case StoreCreditBatchOp:
		batch, err := common.GetBatchFromString[common.Credit](entry.Args)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error deserializing batch %v: %w", StoreCreditBatchOp, err)
		}
		j.storeCreditsBatch(batch.Header.ClientID, batch.AsBatch())
		return j, nil
	case FilterMovieOP:
		clientId, messageId, err := extractIds(entry)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error extracting ids %w", err)
		}
		session := j.getSession(clientId)
		session.FilterMoviesMsg(messageId)
		return j, nil
	case FilterReviewsOP:
		clientId, messageId, err := extractIds(entry)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error extracting ids %w", err)
		}
		session := j.getSession(clientId)
		session.FilterReviewsMsg(messageId)
		return j, nil
	case FilterCreditsOP:
		clientId, messageId, err := extractIds(entry)
		if err != nil {
			return JoinerController{}, fmt.Errorf("error extracting ids %w", err)
		}
		session := j.getSession(clientId)
		session.FilterCreditsMsg(messageId)
		return j, nil
	case DeleteClientOP:
		clientId := entry.Args
		j.deleteClient(clientId)
		return j, nil
	default:
		return JoinerController{}, fmt.Errorf("unknown log operation %v", entry.Op)
	}
}

func extractIds(entry persistency.TransactionEntry) (string, int, error) {
	args := strings.Split(entry.Args, separator)
	clientId := args[0]
	messageId, err := strconv.Atoi(args[1])
	if err != nil {
		return "", 0, err
	}
	return clientId, messageId, nil
}

func NewJoinerController(joinerId int, rabbitUser, rabbitPass string) (*JoinerController, error) {
	middleware, err := middleware.NewMiddleware(rabbitUser, rabbitPass, rabbitHost)
	if err != nil {
		return nil, fmt.Errorf("error creating middleware: %w", err)
	}

	ph, err := persistency.NewPersistencyHandler[JoinerController]()
	if err != nil {
		return nil, fmt.Errorf("error creating persistency handler: %w", err)
	}

	controller := JoinerController{
		joinerId:            joinerId,
		m:                   middleware,
		persistencyHandler:  ph,
		Sessions:            make(map[string]*JoinerSession),
		StoredReviewBatches: make(map[string][]common.Batch[common.Review]),
		StoredCreditBatches: make(map[string][]common.Batch[common.Credit]),
		BlacklistedClients:  make(map[string]int64),
	}

	if err := controller.initializeChannels(); err != nil {
		return nil, fmt.Errorf("error initializing channels: %w", err)
	}

	fromBytes := func(data []byte) (JoinerController, error) {
		if len(data) != 0 {
			if err = json.Unmarshal(data, &controller); err != nil {
				return JoinerController{}, fmt.Errorf("error unmarshalling persistency: %w", err)
			}
		}
		return controller, nil
	}

	controller, err = ph.RecoverFromLogs(fromBytes)
	if err != nil {
		return nil, fmt.Errorf("error recovering persistency: %w", err)
	}

	if err := controller.saveCheckpoint(); err != nil {
		return nil, fmt.Errorf("error saving checkpoint: %w", err)
	}

	return &controller, nil
}

func (j *JoinerController) initializeChannels() error {
	var err error
	j.moviesChan, err = j.m.GetChanWithTopicToRecv(moviesExchange, fmt.Sprintf(moviestopic, j.joinerId))
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", moviesExchange, err)
	}

	j.reviewsChan, err = j.m.GetChanWithTopicToRecv(reviewsExchange, fmt.Sprintf(reviewsTopic, j.joinerId))
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", reviewsExchange, err)
	}

	j.creditChan, err = j.m.GetChanWithTopicToRecv(creditExchange, fmt.Sprintf(creditTopic, j.joinerId))
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", creditExchange, err)
	}

	j.q3ToReduce, err = j.m.GetQueueToSend(q3ToReduceQueue)
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", q3ToReduceQueue, err)
	}

	j.q4ToReduce, err = j.m.GetQueueToSend(q4ToReduceQueue)
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", q4ToReduceQueue, err)
	}

	flushChan, err := j.m.GetChanWithFanoutToRecv(flushExchange, fmt.Sprintf(flushExchange+"-"+"joiner-%d", j.joinerId))
	if err != nil {
		return fmt.Errorf("error creating channel %s: %w", flushExchange, err)
	}
	j.flushQueue = flushChan

	return nil
}

func (j *JoinerController) Start() {
	defer j.stop()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	j.run(ctx)
}

func (j *JoinerController) addJoinerIDToHeader(header common.Header) common.Header {
	header.MessageID.JoinerID = j.joinerId
	return header
}

func (j *JoinerController) joinReviewBatch(clientId string, batch common.Batch[common.Review]) {
	session := j.getSession(clientId)
	session.UpdateReviewsWeights(batch.Header)

	reviewXMovies := session.Join(batch.Data)
	reviewsXMoviesBatch := common.Batch[common.MovieReview]{
		Header: j.addJoinerIDToHeader(batch.Header),
		Data:   reviewXMovies,
	}

	response, err := json.Marshal(reviewsXMoviesBatch)
	if err != nil {
		slog.Error("error marshalling batch", slog.String("error", err.Error()))
	}
	if err := j.q3ToReduce.Send(response); err != nil {
		slog.Error("error sending batch", slog.String("error", err.Error()))
	}
}

func (j *JoinerController) filterCreditBatch(clientId string, batch common.Batch[common.Credit]) {
	session := j.getSession(clientId)
	session.UpdateCreditsWeights(batch.Header)
	credits := session.filterCredits(batch.Data)

	actorsBatch := common.Batch[common.Credit]{
		Header: j.addJoinerIDToHeader(batch.Header),
		Data:   credits,
	}

	response, err := json.Marshal(actorsBatch)
	if err != nil {
		slog.Error("error marshalling batch", slog.String("error", err.Error()))
	}
	if err := j.q4ToReduce.Send(response); err != nil {
		slog.Error("error sending batch", slog.String("error", err.Error()))
	}
}

func (j *JoinerController) storeReviewBatch(clientId string, batch common.Batch[common.Review]) {
	j.StoredReviewBatches[clientId] = append(j.StoredReviewBatches[clientId], batch)
}
func (j *JoinerController) storeCreditsBatch(clientId string, batch common.Batch[common.Credit]) {
	j.StoredCreditBatches[clientId] = append(j.StoredCreditBatches[clientId], batch)
}

func (j *JoinerController) joinStoredBatches(clientId string) {
	reviewBatches := j.StoredReviewBatches[clientId]
	delete(j.StoredReviewBatches, clientId)
	for _, batch := range reviewBatches {
		j.joinReviewBatch(clientId, batch)
	}

	creditBatches := j.StoredCreditBatches[clientId]
	j.StoredCreditBatches[clientId] = []common.Batch[common.Credit]{}
	delete(j.StoredCreditBatches, clientId)
	for _, batch := range creditBatches {
		j.filterCreditBatch(clientId, batch)
	}
}

func (j *JoinerController) saveCheckpoint() error {
	jsonData, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("error marshalling internal state: %w", err)
	}
	if err = j.persistencyHandler.SaveCheckpoint(jsonData); err != nil {
		return fmt.Errorf("error saving checkpoint: %w", err)
	}
	j.transactionsOnLog = 0
	return nil
}

func (j *JoinerController) save(transaction persistency.Transaction) error {
	if err := j.persistencyHandler.Commit(transaction); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	j.transactionsOnLog++

	if j.transactionsOnLog == maxTransactionsOnLog {
		return j.saveCheckpoint()
	}
	return nil
}

func (j *JoinerController) run(ctx context.Context) {
	slog.Info("starting joiner", slog.Int("joinerID", j.joinerId))
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		var msg middleware.Message
		var clientId string
		transaction := persistency.NewTransaction()
		select {
		case <-ctx.Done():
			slog.Info("received termination signal, stopping joiner")
			return
		case <-ticker.C:
			j.m.SendHeartbeat()
			continue
		case msg = <-j.flushQueue:
			var flushClient common.FlushClient
			if err := json.Unmarshal(msg.Body, &flushClient); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}
			transaction := persistency.NewTransaction()
			transaction.Do(DeleteClientOP, flushClient.ClientID)
			j.deleteClient(flushClient.ClientID)
			slog.Info("flushing client", slog.String("clientID", flushClient.ClientID))
			j.save(transaction)
			if err := msg.Ack(); err != nil {
				slog.Error("error acknowledging message", slog.String("error", err.Error()))
			}
			continue
		case msg = <-j.moviesChan:
			var batch common.LoggableBatch[common.Movie]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			if j.isBlacklisted(batch.GetClientID()) {
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				slog.Info("Discarding message from blacklisted client", slog.String("clientID", batch.GetClientID()))
				continue
			}

			clientId = batch.GetClientID()
			session := j.getSession(clientId)
			if !session.FilterMoviesMsg(batch.Header.MessageID.ID) {
				slog.Info("Filtering Msg", slog.Any("header", batch.Header))
				break
			}
			transaction.Do(FilterMovieOP, batch.ClientID+separator+strconv.Itoa(batch.MessageID.ID))

			session.UpdateMoviesWeights(batch.Header)
			transaction.Do(UpdateMoviesWeightsOp, batch.Header.ToString())

			session.SaveMovies(batch.Data)
			transaction.Do(SaveMoviesOp, batch.ToString())

			if session.AllMoviesReceived() {
				slog.Info("Received all movies", slog.String("client id", clientId))
				j.joinStoredBatches(clientId) // Joins all reviews stored
				transaction.Do(JoinStoredBatchesOp, clientId)
			}

		case msg = <-j.reviewsChan:
			var batch common.LoggableBatch[common.Review]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			if j.isBlacklisted(batch.GetClientID()) {
				slog.Info("Discarding message from blacklisted client", slog.String("clientID", batch.GetClientID()))
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			clientId = batch.GetClientID()
			session := j.getSession(clientId)
			if !session.FilterReviewsMsg(batch.MessageID.ID) {
				slog.Info("Filtering Msg", slog.Any("header", batch.Header))
				break
			}
			transaction.Do(FilterReviewsOP, batch.ClientID+separator+strconv.Itoa(batch.MessageID.ID))

			if !session.AllMoviesReceived() {
				j.storeReviewBatch(clientId, batch.AsBatch())
				transaction.Do(StoreReviewBatchOp, batch.ToString())
			} else {
				j.joinReviewBatch(clientId, batch.AsBatch())
				transaction.Do(UpdateReviewsWeightsOp, batch.Header.ToString())
			}

		case msg = <-j.creditChan:
			var batch common.LoggableBatch[common.Credit]
			if err := json.Unmarshal(msg.Body, &batch); err != nil {
				slog.Error("error unmarshalling message", slog.String("error", err.Error()))
				continue
			}

			if j.isBlacklisted(batch.GetClientID()) {
				slog.Info("Discarding message from blacklisted client", slog.String("clientID", batch.GetClientID()))
				if err := msg.Ack(); err != nil {
					slog.Error("error acknowledging message", slog.String("error", err.Error()))
				}
				continue
			}

			clientId = batch.GetClientID()
			session := j.getSession(clientId)
			if !session.FilterCreditsMsg(batch.MessageID.ID) {
				slog.Info("Filtering Msg", slog.Any("header", batch.Header))
				break
			}
			transaction.Do(FilterCreditsOP, batch.ClientID+separator+strconv.Itoa(batch.MessageID.ID))

			if !session.AllMoviesReceived() {
				j.storeCreditsBatch(clientId, batch.AsBatch())
				transaction.Do(StoreCreditBatchOp, batch.ToString())
			} else {
				j.filterCreditBatch(clientId, batch.AsBatch())
				transaction.Do(UpdateCreditsWeightsOp, batch.Header.ToString())
			}
		}

		if j.Sessions[clientId].IsDone() {
			slog.Info("Done for client, deleting session", slog.String("clientId", clientId))
			j.deleteClient(clientId)
			transaction.Do(DeleteClientOP, clientId)
		}
		j.save(transaction)
		if err := msg.Ack(); err != nil {
			slog.Error("error acknowledging message", slog.String("error", err.Error()))
		}
		j.m.SendHeartbeat()
	}
}

func (j *JoinerController) getSession(clientId string) *JoinerSession {
	if _, ok := j.Sessions[clientId]; !ok {
		slog.Info("New client detected. creating session", slog.String("clientId", string(clientId)))
		j.Sessions[clientId] = NewJoinerSession()
	}
	return j.Sessions[clientId]
}

func (j *JoinerController) deleteClient(id string) {
	delete(j.Sessions, id)
	j.BlacklistedClients[id] = time.Now().Unix() + 10
}

func (j *JoinerController) isBlacklisted(id string) bool {
	j.purgeBlacklist()
	_, exists := j.BlacklistedClients[id]
	return exists
}

func (j *JoinerController) purgeBlacklist() {
	now := time.Now().Unix()
	for clientID, timestamp := range j.BlacklistedClients {
		if now-timestamp >= blacklistDuration {
			delete(j.BlacklistedClients, clientID)
		}
	}
}

func (j *JoinerController) stop() {
	if err := j.m.Close(); err != nil {
		slog.Error("error closing middleware", slog.String("error", err.Error()))
	}
	slog.Info("joiner stopped")
}
