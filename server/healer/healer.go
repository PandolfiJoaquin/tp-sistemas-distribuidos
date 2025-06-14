package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"pkg/communication"
	"time"
)

const (
	heartBeatTimer    = 2 * time.Second
	maxRetries        = 3
	connectionTimeout = 5 * time.Second
	healthCheckPort   = ":1500"
)

type Healer struct {
	containerNames []string
}

func NewHealer(containerNames []string) *Healer {
	return &Healer{
		containerNames: containerNames,
	}
}

func (h *Healer) Start(ctx context.Context) {
	slog.Info("Starting healer service")

	for _, containerName := range h.containerNames {
		go h.monitorContainer(ctx, containerName)
	}
}

func (h *Healer) monitorContainer(ctx context.Context, containerName string) {
	ticker := time.NewTicker(heartBeatTimer)
	defer ticker.Stop()

	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			failureCount = h.checkAndHandleHealth(containerName, failureCount)
		}
	}
}

func (h *Healer) checkAndHandleHealth(containerName string, currentFailures int) int {
	if h.isHealthy(containerName) {
		if currentFailures > 0 {
			slog.Info("Container is healthy again", "container", containerName, "failures", currentFailures)
		}
		return 0 // Reset failure count if healthy
	}

	return h.handleUnhealthyContainer(containerName, currentFailures)
}

func (h *Healer) handleUnhealthyContainer(containerName string, failures int) int {
	failures++
	slog.Warn("Health check failed",
		"name", containerName,
		"failures", failures,
		"max_retries", maxRetries)

	if failures >= maxRetries {
		slog.Info("Max retries reached, attempting to restart container", "name", containerName)
		if err := h.restartContainer(containerName); err != nil {
			slog.Error("Failted To restart container", "error", err)
			// If restart fails, we can try again
			return failures
		}
		return 0
	}

	return failures
}

func (h *Healer) isHealthy(containerName string) bool {
	conn, err := net.DialTimeout("tcp", containerName+healthCheckPort, connectionTimeout)
	if err != nil {
		slog.Warn("dial error", "error", err)
		return false
	}
	defer conn.Close()

	response, err := communication.RecvAll(conn, 1)
	if err != nil {
		slog.Warn("recv error", "error", err)
	}

	return err == nil && string(response) == "K"
}

func (h *Healer) restartContainer(containerName string) error {
	cmd := exec.Command("docker", "restart", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("restart command failed: %w, output: %s", err, string(output))
	}

	slog.Info("Restart command executed", "container", containerName, "output", string(output))
	return nil
}
