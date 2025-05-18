package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
	"strconv"
)

func main() {
	logger, err := log.SetupLogger("joiner", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	joinerId := os.Getenv("JOINER_ID")

	if rabbitUser == "" || rabbitPass == "" || joinerId == "" {
		slog.Error("environment validation failed", slog.String("error", "RABBITMQ_DEFAULT_USER and RABBITMQ_DEFAULT_PASS and JOINER_ID must be set"))
		return
	}

	joinerIdInt, err := strconv.Atoi(joinerId)
	if err != nil {
		slog.Error("error converting JOINER_ID env var to int", slog.String("error", err.Error()))
		return
	}

	joiner, err := NewJoinerController(joinerIdInt, rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating joiner", slog.String("error", err.Error()))
		return
	}

	joiner.Start()
}
