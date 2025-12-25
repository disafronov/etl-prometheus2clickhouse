# Variables
DOCKER_IMAGE = etl-prometheus2clickhouse
PYTEST_CMD = uv run python -m pytest -v
COVERAGE_OPTS = --cov --cov-report=term-missing --cov-report=html

# Include environment files
ifneq (,$(wildcard .env))
    ifneq (,$(wildcard env.example))
        include env.example
    endif
    include .env
else
    include env.example
endif
export

.PHONY: all clean dead-code docker docker-build docker-run format help install lint run test test-coverage

help: ## Show this help message
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

################################################################################

install: ## Install dependencies
	@echo "Installing dependencies..."
	uv sync
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install

format: ## Format code
	@echo "Formatting code..."
	uv run black . && uv run isort .

lint: ## Run linting tools
	@echo "Running linting tools..."
	uv run black --check . && uv run isort --check-only . && uv run flake8 . && uv run mypy . && uv run bandit -r -c pyproject.toml .

dead-code: ## Check for dead code using vulture
	@echo "Checking for dead code..."
	uv run vulture

test: ## Run tests
	@echo "Running tests..."
	$(PYTEST_CMD)

test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	$(PYTEST_CMD) $(COVERAGE_OPTS)

all: format lint test dead-code ## Run format, lint, test, and dead-code check
	@echo "All checks completed successfully!"

run: ## Run the application locally
	@echo "Running application locally..."
	uv run python main.py

clean: ## Clean cache and temporary files
	@echo "Cleaning cache and temporary files..."
	rm -rf .mypy_cache/ .pytest_cache/ .venv/ build/ dist/ htmlcov/ .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

################################################################################

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE) .

docker-run: ## Run Docker container
	@echo "Running Docker container..."
	docker run --rm \
		--env-file env.example \
		--env-file env.docker \
		$(if $(wildcard .env),--env-file .env) \
		$(DOCKER_IMAGE)

docker: docker-build docker-run ## Build and run Docker container
	@echo "Docker container built and running!"
