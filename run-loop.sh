#!/bin/bash

# Habilitar el modo de error
set -e

echo "Starting infinite loop of make run..."
echo "Press Ctrl+C to stop"

counter=1
while true; do
    echo "----------------------------------------"
    echo "Starting iteration #$counter at $(date)"
    echo "----------------------------------------"
    make run
    echo "----------------------------------------"
    echo "Iteration #$counter completed at $(date)"
    echo "----------------------------------------"
    counter=$((counter + 1))
    sleep 1  # Pequeña pausa entre iteraciones
done