package middleware

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"pkg/communication"
	"time"
)

const (
	port            = "1500"
	healthyResponse = "K"
	watchdogTimeout = 30 * time.Second
)

type HealthCheck struct {
	listener *net.Listener
}

func StartHealthCheck(hb <-chan struct{}) (*HealthCheck, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		return nil, err
	}

	// Goroutine that answers incoming health-check connections.
	go func(listener net.Listener) {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					slog.Error("Error accepting connection at healthcheck:", slog.String("error", err.Error()))
				}
				return
			}

			if err = answerHealthy(conn); err != nil {
				slog.Error("Error answering health check:", slog.String("error", err.Error()))
				_ = conn.Close()
				return
			}

			if err := conn.Close(); err != nil {
				slog.Error("Error closing connection:", slog.String("error", err.Error()))
				return
			}
		}
	}(listener)

	go watchDog(hb, &listener)

	return &HealthCheck{listener: &listener}, nil
}

// watchdog expects heartbeats at least every watchdogTimeout.
// if it doesn't receive a heartbeat, it closes the listener.
func watchDog(hb <-chan struct{}, listener *net.Listener) {
	lastHeartbeat := time.Now()
	checkInterval := 1 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	// Update lastHeartbeat when heartbeats arrive
	go func() {
		for range hb {
			lastHeartbeat = time.Now()
		}
	}()

	// check for timeouts
	for range ticker.C {
		if time.Since(lastHeartbeat) > watchdogTimeout {
			slog.Error("health-check watchdog timeout: no heartbeat, terminating",
				slog.Duration("elapsed", time.Since(lastHeartbeat)),
				slog.Duration("timeout", watchdogTimeout))
			_ = (*listener).Close()
			os.Exit(1)
		}
	}
}

func answerHealthy(conn net.Conn) error {
	response := []byte(healthyResponse)
	err := communication.SendAll(conn, response)
	if err != nil {
		return err
	}
	return nil
}

func (h *HealthCheck) Stop() error {
	if err := (*h.listener).Close(); err != nil {
		return fmt.Errorf("error closing health check listener: %w", err)
	}
	return nil
}
