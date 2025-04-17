package main

import (
	"fmt"
	"log/slog"
	"os"
	"pkg/log"
)


func main() {
	logger, err := log.SetupLogger("production-filter", false, nil)
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

	filter := NewProductionFilter(rabbitUser, rabbitPass)
	filter.Start()
}
