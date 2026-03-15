# CLAUDE.md — NexusAuthAI Codebase Guide

## Project Overview

**NexusAuthAI** is a clinical NLP ingestion pipeline for healthcare prior-authorization automation. It scrapes CMS Medicare coverage data, deduplicates it, classifies documents, and generates pgvector embeddings for semantic search. The codebase is a pure backend ETL system — no REST API layer; execution is via CLI.

---

## Repository Structure

```
nexus-auth/
├── ingestion/                   # 4-stage ingestion pipeline (main package)
│   ├── pipeline.py              # Orchestrator — CLI entry point
│   ├── deduplicator.py          # SHA-256 dedup + raw_documents insertion
│   ├── embedder.py              # pgvector embedding via OpenAI API
│   ├── tagger_integration.py    # raw_documents → knowledge_documents bridge
│   └── scrapers/
│       └── cms_scraper.py       # CMS LCD/NCD bulk export downloader
├── tagging/
│   ├── tagger.py                # DocumentTagger — keyword-scoring classifier
│   └── taxonomy.yaml            # Single source of truth for classification rules
├── database/
│   └── migrations/
│       ├── 001_initial_schema.sql   # Core schema + payer seed data
│       └── 002_session2_additions.sql  # Embedding status, views, indexes
├── tests/
│   └── test_session2_pipeline.py    # pytest suite (~50 unit + integration tests)
├── docker/
│   ├── docker-compose.yml       # PostgreSQL 16+pgvector, Redis 7, pgAdmin 4
│   └── pgadmin_servers.json
├── .env.example                 # Required env var template
├── requirements.txt
└── index.html                   # Standalone SPA demo (no build step)
```

---

## Development Environment Setup

### Prerequisites
- Python 3.11+
- Docker + Docker Compose

### First-time setup

```bash
# 1. Start infrastructure
cd docker && docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — at minimum set OPENAI_API_KEY

# 4. Apply database migrations (in order)
psql $DATABASE_URL -f database/migrations/001_initial_schema.sql
psql $DATABASE_URL -f database/migrations/002_session2_additions.sql
```

### Default service URLs (from docker-compose)
| Service    | URL / Port                      | Credentials                         |
|------------|---------------------------------|--------------------------------------|
| PostgreSQL | `localhost:5432`                | `nexusauth / nexusauth`              |
| Redis      | `localhost:6379`                | none                                 |
| pgAdmin    | `http://localhost:5050`         | `admin@nexusauth.dev / nexusauth_admin` |

### Default DATABASE_URL
```
postgresql://nexusauth:nexusauth@localhost:5432/nexusauth
```

---

## Running the Pipeline

```bash
# Run all 4 stages end-to-end
python -m ingestion.pipeline --all

# Individual stages
python -m ingestion.pipeline --scrape    # Stage 1+2: Scrape + Deduplicate
python -m ingestion.pipeline --tag       # Stage 3: Tag → knowledge_documents
python -m ingestion.pipeline --embed     # Stage 4: Generate pgvector embeddings

# Useful flags
python -m ingestion.pipeline --all --max-docs 50        # Limit for testing
python -m ingestion.pipeline --all --dry-run            # Scrape only, no DB writes
python -m ingestion.pipeline --all --log-level DEBUG    # Verbose logging
python -m ingestion.pipeline --all --output-json        # JSON summary to stdout
python -m ingestion.pipeline --all --database-url $URL  # Override connection string
```

---

## Running Tests

```bash
# All unit tests (no Docker required)
pytest tests/ -v

# Skip integration tests that require a live database
pytest tests/ -k "not integration"

# Run a specific test class
pytest tests/ -k "TestDeduplication" -v

# With coverage
pytest tests/ --cov=ingestion --cov=tagging
```

Integration tests are marked `@pytest.mark.integration` and require the Docker stack to be running.

---

## Pipeline Architecture

### 4-Stage ETL Flow

```
CMS Bulk Exports (ZIP)
       │
       ▼
[Stage 1+2] cms_scraper.py → deduplicator.py
       │  SHA-256 dedup, insert into raw_documents
       ▼
[Stage 3]  tagger_integration.py → tagging/tagger.py
       │  Classify doc type, extract codes, insert into knowledge_documents
       ▼
[Stage 4]  embedder.py → OpenAI text-embedding-3-small
           Chunk text, generate 1536-dim vectors, insert into document_embeddings
```

### Processing Status Lifecycle

`raw_documents.processing_status`:
```
pending → processing → tagged → (done)
                     → failed
```

`knowledge_documents.embedding_status`:
```
pending → embedded
        → failed
        → skipped
```

All stages are **idempotent** — safe to re-run. Each stage selects only records in the appropriate pending state.

---

## Database Schema Conventions

- **Primary keys**: `UUID DEFAULT uuid_generate_v4()` — always named `id`
- **Timestamps**: All tables have `created_at` and `updated_at TIMESTAMPTZ`, with auto-update triggers
- **Arrays**: Medical codes and specialties stored as `TEXT[]` with GIN indexes
- **Flexible metadata**: `JSONB` columns for scraper metadata, tagger audit trails, job configs
- **Audit log**: All mutations to `knowledge_documents` and `routing_rules` are captured in `audit_log` (HIPAA requirement)
- **Vector index**: HNSW index on `document_embeddings.embedding` (`cosine_ops`, m=16, ef_construction=64)

### Key Tables

| Table                  | Purpose                                      |
|------------------------|----------------------------------------------|
| `raw_documents`        | Landing zone for scraped content             |
| `knowledge_documents`  | Classified, structured document layer        |
| `document_embeddings`  | pgvector chunks for semantic search          |
| `payers`               | Master payer registry (CMS, Aetna, UHC…)    |
| `routing_rules`        | document_type → tool routing configuration  |
| `scraper_jobs`         | Pipeline run tracking                        |
| `audit_log`            | HIPAA-compliant change history               |

### Useful Views (from migration 002)
- `pipeline_status` — Row counts by layer and processing status
- `documents_needing_review` — Human review queue (confidence < 0.40)

---

## Module Conventions

### Batch Processing Pattern
All processing modules follow the same pattern:
1. Fetch a batch of `pending` records
2. Mark as `processing` to prevent concurrent re-processing
3. Process each record, collecting results
4. Mark as `tagged`/`embedded`/`failed` based on outcome
5. Return a `*Stats` dataclass with counts and elapsed time

Default batch size: **100 documents**. Commits every **100 docs** to avoid large transactions.

### Error Handling
- **Duplicates**: Counted and skipped silently (not an error)
- **Processing errors**: Record marked `failed`, error stored in `processing_error` column, pipeline continues
- **Fatal errors**: Logged and propagated to `main()` which returns exit code 1

### Logging
Configured centrally in `pipeline.py`. Suppress noisy third-party loggers (urllib3, openai, httpx). Format: `YYYY-MM-DD HH:MM:SS [LEVEL] module: message`.

### Configuration
All runtime config comes from environment variables (via `python-dotenv`). Never hardcode credentials. See `.env.example` for all available variables.

---

## Taxonomy & Classification

**`tagging/taxonomy.yaml` is the single source of truth** for all classification behavior:
- Document type definitions and their scoring keywords
- Payer domain mappings (e.g., `cms.gov` → `CMS`)
- Medical specialty keyword lists
- Routing matrix: `document_type → target_tool` (NexusAuth, CODEMED, REVIEW)
- Confidence thresholds

To change classification behavior, **edit `taxonomy.yaml`**, not `tagger.py`.

### Confidence Scoring
- `confidence_score` is a float in `[0.0, 1.0]`
- Documents with `confidence_score < 0.40` are flagged `requires_review = True`
- These appear in the `documents_needing_review` view

---

## Embedding Details

- **Model**: `text-embedding-3-small` (1536 dimensions) — cost-efficient default
- **Alternative**: `text-embedding-3-large` (3072 dims) for higher quality
- **Chunk size**: 512 tokens (~1800 chars) with 64-token (~230 char) overlap
- **Chunk strategy**: Boundary-aware — breaks at `\n\n`, `. `, or word boundaries
- **Rate limiting**: 0.5s sleep between OpenAI API calls
- **Truncation**: Input capped at 25,000 chars (~7,000 tokens) before embedding

### Semantic Search
`embedder.semantic_search(conn, query, limit, document_type, payer_id)` embeds the query and uses pgvector cosine distance to return matching chunks with similarity scores.

---

## CMS Data Sources

| Data      | URL                                                                          |
|-----------|------------------------------------------------------------------------------|
| LCD (zip) | `https://downloads.cms.gov/medicare-coverage-database/downloads/exports/current_lcd.zip` |
| NCD (zip) | `https://downloads.cms.gov/medicare-coverage-database/downloads/exports/ncd.zip` |

Scraper downloads the outer ZIP, extracts the inner `*_csv.zip`, and reads CSV files. Filters LCDs to status `A` (Active) and `F` (Future) only.

---

## Key Conventions for AI Assistants

1. **Never modify `taxonomy.yaml` structure** without also updating `tagger.py` parsing logic — they are tightly coupled.
2. **Migrations are append-only** — never edit existing migration files. Add new `00N_*.sql` files for schema changes.
3. **All pipeline stages must remain idempotent** — selecting on status fields and using `ON CONFLICT DO NOTHING` is the pattern.
4. **Test coverage is mandatory for new pipeline stages** — follow the existing pattern in `test_session2_pipeline.py` with unit tests for pure functions and `@pytest.mark.integration` for DB-dependent tests.
5. **No REST API** — this is a CLI pipeline. Do not add Flask/FastAPI unless explicitly requested.
6. **Environment variables only** — never hardcode URLs, credentials, or API keys. Add new config to `.env.example` with a comment.
7. **Batch size defaults**: Keep default batch sizes at 100 documents unless there's a specific reason. Changing them affects memory usage and transaction size.
8. **HIPAA audit compliance**: Any new writes to `knowledge_documents` or `routing_rules` will automatically be captured by existing audit triggers. Do not bypass triggers.
9. **UUID primary keys everywhere** — never use integer auto-increment PKs for new tables.
10. **Array columns for multi-value medical data** — use `TEXT[]` with GIN indexes for CPT, ICD-10, HCPCS, and specialties columns.
