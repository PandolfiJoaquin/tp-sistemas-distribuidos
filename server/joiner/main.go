package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
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

	if rabbitUser == "" || rabbitPass == "" {
		slog.Error("environment validation failed", slog.String("error", "RABBITMQ_DEFAULT_USER and RABBITMQ_DEFAULT_PASS must be set"))
		return
	}

	joiner, err := NewJoiner(rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating joiner", slog.String("error", err.Error()))
		return
	}

	defer func(joiner *Joiner) {
		err := joiner.Close()
		if err != nil {
			slog.Error("error closing joiner", slog.String("error", err.Error()))
		}
	}(joiner)

	joiner.Start()
}
