package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"pkg/log"
)

const (
	server       = "gateway:12345"
	MoviesBatch  = 30
	ReviewsBatch = 300
	CreditsBatch = 30
	sleep = 1
)

func main() {
	logger, err := log.SetupLogger("client", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	// read cli_id from env
	cliID := os.Getenv("CLI_ID")
	id, err := strconv.Atoi(cliID)
	if err != nil {
		slog.Error("env variable CLI_ID is invalid", slog.String("error", err.Error()))
		return
	}

	config := NewClientConfig(id, server, MoviesBatch, ReviewsBatch, CreditsBatch, sleep)
	client := NewClient(config)

	slog.Info("client created successfully")

	client.Start()
}
