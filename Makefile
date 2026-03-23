# CodeMed AI — Developer Makefile
# =================================
# Usage: make <target>

.PHONY: help install lint test test-unit test-integration coverage \
        run run-demo docker-up docker-down clean

help:
	@echo "CodeMed AI — available targets:"
	@echo ""
	@echo "  install          Install all Python dependencies"
	@echo "  lint             Run ruff linter (check only)"
	@echo "  lint-fix         Run ruff linter with auto-fix"
	@echo "  test             Run all tests (excluding integration)"
	@echo "  test-unit        Alias for test"
	@echo "  test-integration Run integration tests (requires live DB / Stripe)"
	@echo "  coverage         Run tests with HTML coverage report"
	@echo "  run              Start API server (port 8001)"
	@echo "  run-demo         Start Streamlit demo (port 8501)"
	@echo "  docker-up        Start full stack via Docker Compose"
	@echo "  docker-down      Stop and remove Docker Compose containers"
	@echo "  clean            Remove .pyc, __pycache__, and coverage artefacts"

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

install:
	pip install -r requirements.txt

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

lint:
	ruff check .

lint-fix:
	ruff check . --fix

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	pytest tests/ -q --tb=short -m "not integration"

test-unit: test

test-integration:
	pytest tests/ -q --tb=short -m "integration"

coverage:
	pytest tests/ -q --tb=short -m "not integration" \
	    --cov=codemed --cov=billing --cov=ingestion \
	    --cov-report=term-missing --cov-report=html:htmlcov
	@echo "\nCoverage report: htmlcov/index.html"

# ---------------------------------------------------------------------------
# Local dev servers
# ---------------------------------------------------------------------------

run:
	uvicorn codemed.api:app --reload --port 8001

run-demo:
	streamlit run demo/app.py --server.port 8501

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker-up:
	docker compose up --build -d
	@echo "\nAPI:  http://localhost:8001/docs"
	@echo "Demo: http://localhost:8501"

docker-down:
	docker compose down

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov coverage.xml .pytest_cache
