package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
	"strconv"
)

const (
	server       = "gateway:12345"
	MoviesBatch  = 300
	ReviewsBatch = 5120
	CreditsBatch = 50
	sleep        = 1
)

func main() {
	logger, err := log.SetupLogger("client", nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	moviesFile := os.Getenv("MOVIES_FILE")
	reviewsFile := os.Getenv("REVIEWS_FILE")
	creditsFile := os.Getenv("CREDITS_FILE")

	//creditsFile := "empty_credits.csv"
	//creditsReader, err := utils.NewCreditsReader(creditsFile, CreditsBatch)
	//if err != nil {
	//	slog.Error("error creating credits reader", slog.String("error", err.Error()))
	//	return
	//}
	//
	//batch, err := creditsReader.ReadBatch()
	//if err != nil {
	//	slog.Error("error reading credits batch", slog.String("error", err.Error()))
	//	return
	//}
	//
	//slog.Info("read credits batch successfully", slog.Int("batch_size", len(batch)), slog.Int("total_read", creditsReader.TotalRead()), slog.Any("batch", batch))
	if moviesFile == "" || reviewsFile == "" || creditsFile == "" {
		slog.Error("env variables MOVIES_FILE, REVIEWS_FILE and CREDITS_FILE must be set")
		return
	}

	cliID := os.Getenv("CLI_ID")
	id, err := strconv.Atoi(cliID)
	if err != nil {
		slog.Error("env variable CLI_ID is invalid", slog.String("error", err.Error()))
		return
	}

	config := NewClientConfig(id, server, moviesFile, reviewsFile, creditsFile, MoviesBatch, ReviewsBatch, CreditsBatch, sleep)
	client := NewClient(config)

	slog.Info("client created successfully")

	client.Start()
}
