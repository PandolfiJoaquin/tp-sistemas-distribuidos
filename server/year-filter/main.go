package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
)

func main() {
	logger, err := log.SetupLogger("year-filter", false, nil)
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

	filter, err := NewYearFilter(rabbitUser, rabbitPass)
	if err != nil {
		slog.Error("error creating year filter", slog.String("error", err.Error()))
		return
	}

	filter.Start()
	
	if err := filter.Stop(); err != nil {
		slog.Error("error stopping year filter", slog.String("error", err.Error()))
		return
	}
}
