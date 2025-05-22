package common

import (
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
)

const (
	port            = "1500"
	healthyResponse = "K"
)

type HealthCheck struct {
	listener net.Listener
}

func StartHealthCheck() (*HealthCheck, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("Error starting health check server: %w", err)
	}

	go func(listener net.Listener) {
		for {
			conn, err := listener.Accept()
			if err != nil {
				slog.Error("Error accepting connection at healthcheck:", slog.String("error", err.Error()))
				return
			}

			err = answerHealthy(conn)
			if err != nil {
				slog.Error("Error answering health check:", slog.String("error", err.Error()))
				return
			}

			if err := conn.Close(); err != nil {
				slog.Error("Error closing connection:", slog.String("error", err.Error()))
				return
			}
		}
	}(listener)

	return &HealthCheck{listener: listener}, nil

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
	if err := h.listener.Close(); err != nil {
		return fmt.Errorf("error closing health check listener: %w", err)
	}
	return nil
}
