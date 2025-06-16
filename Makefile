.PHONY: run

run:
	@if [ ! -f config-script.json ]; then \
		echo "Error: config-script.json not found"; \
		exit 1; \
	fi
	./generate-compose.sh config-script.json
	docker compose down -v
	sudo rm -rf data
	docker compose build
	docker compose up -d --remove-orphans
down:
	docker compose down -v

run-no-cache:
	@if [ ! -f config-script.json ]; then \
		echo "Error: config-script.json not found"; \
		exit 1; \
	fi
	docker compose down -v
	docker compose build --no-cache
	docker compose up -d --remove-orphans

run-local:
	sudo rm -rf server/data
	docker compose down -v
	docker compose up rabbitmq -d
	@echo "Esperando que RabbitMQ esté healthy..."
	@bash -c '\
	for i in {1..60}; do \
	    status=$$(docker inspect --format="{{.State.Health.Status}}" $$(docker compose ps -q rabbitmq)); \
	    if [ "$$status" = "healthy" ]; then \
	        echo "✅ RabbitMQ está listo"; \
	        exit 0; \
	    fi; \
	    printf "."; \
	    sleep 1; \
	done; \
	echo "❌ Timeout esperando a RabbitMQ"; \
	exit 1'
	cd server && RABBITMQ_DEFAULT_USER=monke RABBITMQ_DEFAULT_PASS=joaco1 JOINER_SHARDS=2 QUERY_NUM=3 go run final-reducer/*.go

run-local-no-reset:
	cd server && RABBITMQ_DEFAULT_USER=monke RABBITMQ_DEFAULT_PASS=joaco1 JOINER_SHARDS=2 QUERY_NUM=3 go run final-reducer/*.go

run-kill: run
	./kill-containers.sh

run-kill-inf:
	@while true; do \
		echo "Starting new iteration..."; \
		make run-kill; \
		echo "Checking results..."; \
		if ! ./compare-outputs.sh config-script.json | tee /tmp/compare-output.txt; then \
			echo "❌ Results check failed!"; \
			exit 1; \
		fi; \
		if grep -q "❌" /tmp/compare-output.txt; then \
			echo "❌ Found negative results in output!"; \
			exit 1; \
		fi; \
		rm /tmp/compare-output.txt; \
		echo "✅ Results check passed!"; \
		echo "Waiting 5 seconds before next iteration..."; \
		sleep 5; \
	done
	
