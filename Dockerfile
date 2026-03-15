# =============================================================================
# CodeMed AI — API Container
# =============================================================================
# Builds the FastAPI backend (codemed.api).
# Streamlit demo is intentionally excluded — run it separately or via
# docker-compose.yml which has a dedicated `demo` service.
#
# Build:
#   docker build -t codemed-api .
#
# Run (standalone):
#   docker run -p 8001:8001 \
#     -e CODEMED_API_KEYS=your-secret-key \
#     -e REDIS_URL=redis://your-redis:6379/0 \
#     codemed-api
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: dependency builder
# Installs compiled wheels in a separate layer for cache efficiency.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps for psycopg2-binary and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 2: runtime image
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Labels
LABEL org.opencontainers.image.title="CodeMed AI API"
LABEL org.opencontainers.image.description="V28 HCC Enforcement · MEAT Extraction · NLQ Code Search · Prior Auth Appeals"
LABEL org.opencontainers.image.source="https://github.com/nexus-pre-auth/nexus-auth"

# Runtime system deps (libpq for psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Create non-root user
RUN groupadd -r codemed && useradd -r -g codemed -d /app -s /sbin/nologin codemed

WORKDIR /app

# Copy application code
COPY codemed/ codemed/
COPY ingestion/ ingestion/
COPY tagging/ tagging/

# Ownership
RUN chown -R codemed:codemed /app

USER codemed

# ---------------------------------------------------------------------------
# Runtime configuration
# ---------------------------------------------------------------------------

# API port
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -sf -H "X-API-Key: ${CODEMED_API_KEYS:-dev-key-codemed}" \
        http://localhost:8001/v1/health || exit 1

# Environment variable defaults (override at runtime)
ENV CODEMED_API_KEYS=dev-key-codemed \
    REDIS_URL=redis://redis:6379/0 \
    LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
CMD ["uvicorn", "codemed.api:app", \
     "--host", "0.0.0.0", \
     "--port", "8001", \
     "--workers", "2", \
     "--log-level", "info"]
