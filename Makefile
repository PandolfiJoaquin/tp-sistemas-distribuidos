.PHONY: run

run:
	@if [ ! -f config-script.json ]; then \
		echo "Error: config-script.json not found"; \
		exit 1; \
	fi
	docker compose down -v
	docker compose build
	docker compose up -d --remove-orphans

run-no-cache:
	@if [ ! -f config-script.json ]; then \
		echo "Error: config-script.json not found"; \
		exit 1; \
	fi
	docker compose down -v
	docker compose build --no-cache
	docker compose up -d --remove-orphans
