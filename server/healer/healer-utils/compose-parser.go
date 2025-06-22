package healer_utils

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const filepath = "./docker-compose.yaml"

type DockerCompose struct {
	Services map[string]Service `yaml:"services"`
}

type Service struct {
	ContainerName string `yaml:"container_name"`
}

func ReadContainerToMonitor(myName string) ([]string, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {

		if err = f.Close(); err != nil {
			slog.Error("Error closing file", slog.String("file", filepath))
		}
	}(f)

	var dockerCompose DockerCompose
	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&dockerCompose); err != nil {
		return nil, fmt.Errorf("error decoding YAML: %w", err)
	}

	var containerNames []string
	for _, service := range dockerCompose.Services {
		if service.ContainerName == myName || service.ContainerName == "rabbitmq" {
			continue
		}
		if strings.HasPrefix(service.ContainerName, "client") {
			continue
		}
		containerNames = append(containerNames, service.ContainerName)
	}
	return containerNames, nil
}
