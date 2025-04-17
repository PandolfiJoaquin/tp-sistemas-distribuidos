package main

import (
	"fmt"
	"log/slog"
	"pkg/log"
)

func main() {
	logger, err := log.SetupLogger("client", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)
}
