package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
	"strconv"
)

func main() {
	logger, err := log.SetupLogger("preprocessor", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	joinerShards := os.Getenv("JOINER_SHARDS")
	if rabbitUser == "" || rabbitPass == "" {
		slog.Error("environment validation failed", slog.String("error", "RABBITMQ_DEFAULT_USER and RABBITMQ_DEFAULT_PASS must be set"))
		return
	}

	shards, err := strconv.Atoi(joinerShards)
	if err != nil {
		slog.Error("error converting JOINER_SHARDS env var to int", slog.String("error", err.Error()))
	}

	preprocessor := NewPreprocessor(rabbitUser, rabbitPass, shards)
	preprocessor.Start()
}
