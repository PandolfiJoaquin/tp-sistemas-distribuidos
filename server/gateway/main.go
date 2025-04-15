package main

import (
	"fmt"
	"log/slog"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

func main() {
	logger, err := common.SetupLogger("gateway", false, nil)
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

	gateway := NewGateway(rabbitUser, rabbitPass)
	gateway.Start()
}
