package main

import (
	"fmt"
	"log/slog"
	"pkg/log"
)

const (
	server       = "gateway:12345"
	MoviesBatch  = 30
	ReviewsBatch = 300
	CreditsBatch = 30
)

func main() {
	logger, err := log.SetupLogger("client", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	config := NewClientConfig(server, MoviesBatch, ReviewsBatch, CreditsBatch)
	client := NewClient(config)

	slog.Info("client created successfully")

	client.Start()
}
