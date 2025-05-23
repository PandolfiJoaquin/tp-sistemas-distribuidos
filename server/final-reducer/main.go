package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
	"strconv"
)

func main() {
	logger, err := log.SetupLogger("reducer", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	rabbitUser := os.Getenv("RABBITMQ_DEFAULT_USER")
	rabbitPass := os.Getenv("RABBITMQ_DEFAULT_PASS")
	queryNum := os.Getenv("QUERY_NUM")
	joinerShards := os.Getenv("JOINER_SHARDS")

	if rabbitUser == "" || rabbitPass == "" || queryNum == "" {
		slog.Error("environment validation failed", slog.String("error", "RABBITMQ_DEFAULT_USER and RABBITMQ_DEFAULT_PASS and QUERY_NUM must be set"))
		return
	}

	queryNumInt, err := strconv.Atoi(queryNum)
	if err != nil {
		slog.Error("error converting query number to int", slog.String("error", err.Error()))
		return
	}

	joinerAmt, err := strconv.Atoi(joinerShards)
	if err != nil {
		slog.Error("error converting joiner shards number to int", slog.String("error", err.Error()))
		return
	}

	reducer, err := NewFinalReducer(queryNumInt, rabbitUser, rabbitPass, joinerAmt)
	if err != nil {
		slog.Error("error creating reducer", slog.String("error", err.Error()))
		return
	}

	reducer.Start()
}
