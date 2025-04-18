package main

import (
	"fmt"
	"log/slog"
	"pkg/log"
)

const (
	SERVER = "gateway:12345"
	BATCH  = 1000
)

func main() {
	logger, err := log.SetupLogger("client", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	config := NewClientConfig(SERVER, BATCH)
	client := NewClient(config)

	slog.Info("client created successfully")

	client.Start()
}
