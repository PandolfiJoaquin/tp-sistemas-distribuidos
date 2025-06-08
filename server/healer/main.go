package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"pkg/log"
)

func main() {
	logger, err := log.SetupLogger("healer", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	healerName := os.Getenv("HEALER_NAME")

	toMonitor, err := readContainerToMonitor(healerName)
	if err != nil {
		slog.Error("Error reading YAML file", slog.String("file", filepath), slog.String("error", err.Error()))
		return
	}

	slog.Info("Containers to monitor", slog.Any("containers", toMonitor))

	healer := NewHealer(toMonitor)

	// Declares a cancelable context to allow graceful shutdown with signasl
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	healer.Start(ctx)

	<-ctx.Done()
	slog.Info("Healer service stopped")
}
