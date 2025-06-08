package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const filepath = "./docker-compose.yaml"

type DockerCompose struct {
	Services map[string]Service `yaml:"services"`
}

type Service struct {
	ContainerName string `yaml:"container_name"`
}

func readContainerToMonitor(myName string) ([]string, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

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
		containerNames = append(containerNames, service.ContainerName)
	}
	return containerNames, nil
}
