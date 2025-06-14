#!/usr/bin/env bash
set -euo pipefail

trap 'echo -e "\nStopping…"; exit' INT TERM   # clean exit on Ctrl-C

while true; do
  for i in $(shuf -e 1 2 3); do
    name="joiner-$i"
    echo "Restarting $name …"
    docker kill "$name" || true     # prints any “no such container” errors, then moves on
    sleep 1
    docker compose up -d "$name"
    sleep 3
  done
done

