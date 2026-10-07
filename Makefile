PROFILES := cockroach monitoring
COMPOSE  := docker compose $(addprefix --profile ,$(PROFILES))

.PHONY: up down init status logs clean schema presplit reset bench build

up: ## Start the cluster + Prometheus/Grafana monitoring
	$(COMPOSE) up -d

down: ## Stop and remove all containers (cluster + monitoring)
	$(COMPOSE) down

init: ## Initialize the cluster (run once after first `make up`)
	docker exec roach1 ./cockroach init --insecure

status: ## Show cluster node status
	docker exec roach1 ./cockroach node status --insecure

logs: ## Tail logs from all nodes
	$(COMPOSE) logs -f

clean: ## Stop everything and delete all data volumes (cluster + monitoring)
	$(COMPOSE) down -v

schema: ## Create the bench database + client/client_status/client_address tables
	docker exec -i roach1 ./cockroach sql --insecure < sql/schema.sql

presplit: ## Pre-split/scatter the bench tables across nodes (run once after `make schema`, before high-tps `make bench`)
	docker exec -i roach1 ./cockroach sql --insecure < sql/presplit.sql

reset: ## Drop everything and start fresh with no tables
	docker exec -i roach1 ./cockroach sql --insecure < sql/reset.sql

build: ## Compile the benchmark app
	cd app && go build -o bin/bench .

bench: ## Run the benchmark app at TARGET_TPS (default 4000) — requires `make schema` first
	cd app && go run .
