.PHONY: run

run:
	@if [ ! -f config-script.json ]; then \
		echo "Error: config-script.json not found"; \
		exit 1; \
	fi
	docker compose down -v 
	docker compose up --build -d --remove-orphans
	docker compose logs -f client1
