#!/usr/bin/env bash
# =============================================================================
# CodeMed AI — One-Command Launch Script
# =============================================================================
# Usage:
#   chmod +x demo.sh
#   ./demo.sh
#
# What it does:
#   1. Checks prerequisites (Python 3.11+, pip)
#   2. Creates a virtual environment if one doesn't exist
#   3. Installs Python dependencies
#   4. Starts Redis via Docker if available (optional — API runs without it)
#   5. Starts the FastAPI backend on port 8001
#   6. Starts the Streamlit demo on port 8501
#   7. Opens the demo in your browser
#
# After launch:
#   API:  http://localhost:8001      (Swagger docs: http://localhost:8001/docs)
#   Demo: http://localhost:8501
#
# Press Ctrl+C to stop both services.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║          CodeMed AI — Healthcare Intelligence        ║${NC}"
echo -e "${BOLD}║  V28 HCC · MEAT · NLQ Code Search · Appeals         ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
info "Checking prerequisites..."

if ! command -v python3 &>/dev/null; then
    error "Python 3 is not installed. Please install Python 3.11+."
    exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

if [[ "$PYTHON_MAJOR" -lt 3 ]] || [[ "$PYTHON_MAJOR" -eq 3 && "$PYTHON_MINOR" -lt 11 ]]; then
    error "Python 3.11+ required (found $PYTHON_VERSION)."
    exit 1
fi

success "Python $PYTHON_VERSION"

# ---------------------------------------------------------------------------
# Virtual environment
# ---------------------------------------------------------------------------
VENV_DIR=".venv"

if [[ ! -d "$VENV_DIR" ]]; then
    info "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    success "Virtual environment created at $VENV_DIR"
else
    info "Using existing virtual environment at $VENV_DIR"
fi

# Activate
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# ---------------------------------------------------------------------------
# Install dependencies
# ---------------------------------------------------------------------------
info "Installing dependencies (this may take a minute on first run)..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
success "Dependencies installed"

# ---------------------------------------------------------------------------
# Redis (optional)
# ---------------------------------------------------------------------------
REDIS_STARTED=false

if command -v docker &>/dev/null; then
    if ! docker ps --format '{{.Image}}' 2>/dev/null | grep -q "redis"; then
        info "Starting Redis via Docker (optional caching)..."
        if docker run -d --name codemed-redis -p 6379:6379 redis:7-alpine &>/dev/null; then
            REDIS_STARTED=true
            success "Redis started on port 6379"
        else
            warn "Could not start Redis — API will run without caching"
        fi
    else
        success "Redis already running"
    fi
else
    warn "Docker not found — running without Redis cache (fully functional)"
fi

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        cp .env.example .env
        info "Created .env from .env.example"
    fi
fi

export CODEMED_API_KEYS="${CODEMED_API_KEYS:-dev-key-codemed}"
export CODEMED_API_URL="${CODEMED_API_URL:-http://localhost:8001}"

# ---------------------------------------------------------------------------
# Start FastAPI backend
# ---------------------------------------------------------------------------
info "Starting FastAPI backend on http://localhost:8001 ..."
uvicorn codemed.api:app --host 0.0.0.0 --port 8001 --log-level warning &
API_PID=$!

# Wait for API to be ready
MAX_WAIT=15
WAITED=0
until curl -sf -H "X-API-Key: $CODEMED_API_KEYS" http://localhost:8001/v1/health &>/dev/null; do
    sleep 1
    WAITED=$((WAITED + 1))
    if [[ $WAITED -ge $MAX_WAIT ]]; then
        error "API did not start within ${MAX_WAIT}s. Check logs above."
        kill "$API_PID" 2>/dev/null || true
        exit 1
    fi
done
success "API ready → http://localhost:8001 (docs: http://localhost:8001/docs)"

# ---------------------------------------------------------------------------
# Start Streamlit demo
# ---------------------------------------------------------------------------
info "Starting Streamlit demo on http://localhost:8501 ..."
streamlit run demo/app.py \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false \
    --server.address 0.0.0.0 &
STREAMLIT_PID=$!

sleep 3
success "Demo ready → http://localhost:8501"

# ---------------------------------------------------------------------------
# Open browser
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  CodeMed AI is running!${NC}"
echo ""
echo -e "  Demo:      ${BLUE}http://localhost:8501${NC}"
echo -e "  API:       ${BLUE}http://localhost:8001${NC}"
echo -e "  API Docs:  ${BLUE}http://localhost:8001/docs${NC}"
echo ""
echo -e "  API Key:   ${YELLOW}$CODEMED_API_KEYS${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "  Press Ctrl+C to stop all services."
echo ""

# Open browser (best-effort)
if command -v open &>/dev/null; then
    open http://localhost:8501 2>/dev/null || true
elif command -v xdg-open &>/dev/null; then
    xdg-open http://localhost:8501 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Wait and handle shutdown
# ---------------------------------------------------------------------------
cleanup() {
    echo ""
    info "Shutting down..."
    kill "$API_PID" 2>/dev/null || true
    kill "$STREAMLIT_PID" 2>/dev/null || true
    if [[ "$REDIS_STARTED" == "true" ]]; then
        docker stop codemed-redis &>/dev/null || true
        docker rm codemed-redis &>/dev/null || true
        info "Redis container stopped"
    fi
    success "All services stopped. Goodbye!"
}

trap cleanup INT TERM

# Keep script alive until Ctrl+C
wait "$API_PID" "$STREAMLIT_PID" 2>/dev/null || true
