# LowFarm LLM — developer entry points.
#
# The compose stack reads its variables from secrets/lowfarm.env, so every
# target that starts containers goes through that file.  `make secrets`
# creates it with cryptographically random values.

SHELL := /bin/bash
COMPOSE := docker compose --env-file secrets/lowfarm.env
PYTHON ?= python3
VENV := .venv
GATEWAY_PY := $(VENV)/bin/python

.DEFAULT_GOAL := help

.PHONY: help secrets check venv up down restart logs obs ps test lint fmt clean

help: ## Show this help
	@echo ""
	@echo "  LowFarm LLM"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
	@echo ""

secrets: ## Generate secrets/lowfarm.env (idempotent)
	@$(PYTHON) scripts/generate_secrets.py

check: ## Validate the secrets and scan the git history for leaks
	@$(PYTHON) scripts/check_secrets.py

venv: ## Create the virtualenv and install the dev dependencies
	@$(PYTHON) -m venv $(VENV)
	@$(VENV)/bin/pip install -q --upgrade pip
	@$(VENV)/bin/pip install -q -r gateway/requirements-dev.txt
	@echo "  venv ready at $(VENV)"

up: secrets ## Start the stack (gateway, redis, mock fleet)
	@$(COMPOSE) up -d --build
	@echo "  gateway: http://localhost:8000  grafana: http://localhost:3000 (obs)"

obs: secrets ## Start with Prometheus and Grafana
	@$(COMPOSE) --profile observability up -d --build

down: ## Stop the stack
	@$(COMPOSE) down

restart: ## Restart the gateway
	@$(COMPOSE) restart gateway

logs: ## Follow the gateway logs
	@$(COMPOSE) logs -f gateway

ps: ## Show container status
	@$(COMPOSE) ps

test: ## Run the gateway test suite
	@cd gateway && ../$(GATEWAY_PY) -m pytest

lint: ## Static check (pyflakes)
	@cd gateway && ../$(GATEWAY_PY) -m pyflakes app/ tests/ mock_worker.py
	@echo "  no findings"

fmt: ## Auto-format the code
	@cd gateway && ../$(GATEWAY_PY) -m ruff format app/ tests/ mock_worker.py 2>/dev/null \
		|| echo "  ruff not installed; skipping"

clean: ## Remove the virtualenv and caches
	@rm -rf $(VENV)
	@find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -prune -exec rm -rf {} + 2>/dev/null || true
