package main

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"pkg/log"
	"syscall"
	"tp-sistemas-distribuidos/server/common/middleware"
)

func main() {
	logger, err := log.SetupLogger("healer", nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.SetDefault(logger)
	proc, err := NewProcess()
	if err != nil {
		slog.Error("Error creating process", err)
		return
	}

	heartbeatChan := make(chan struct{}, 1)
	_, err = middleware.StartHealthCheck(heartbeatChan)
	if err != nil {
		slog.Error("Error starting health check", err)
		return
	}

	proc.StartProcess(ctx, heartbeatChan)
	<-ctx.Done()
	slog.Info("Healer service stopped")
}
