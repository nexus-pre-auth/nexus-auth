# CLAUDE.md — NexusAuthAI Codebase Guide

## Project Overview

**NexusAuthAI** is a clinical NLP ingestion pipeline for healthcare prior-authorization automation. It scrapes CMS Medicare coverage data, deduplicates it, classifies documents, and generates pgvector embeddings for semantic search. The codebase is a pure backend ETL system — no REST API layer; execution is via CLI.

Downstream routing sends documents to one of two tools:
- **NexusAuth** — handles prior-auth criteria, LCDs, NCDs, clinical policies, medical necessity, ICD-10 guidelines, transmittals
- **CODEMED** — handles fee schedules, billing guidelines, formularies, NCCI edits, remittance codes, modifier policies, DRG/OPPS/ASP payment data

---

## Repository Structure

```
nexus-auth/
├── ingestion/                   # 5-stage ingestion pipeline (main package)
│   ├── __init__.py
│   ├── pipeline.py              # Orchestrator — CLI entry point
│   ├── deduplicator.py          # SHA-256 dedup + raw_documents insertion
│   ├── embedder.py              # pgvector embedding via OpenAI API
│   ├── tagger_integration.py    # raw_documents → knowledge_documents bridge
│   └── scrapers/
│       ├── __init__.py
│       ├── cms_scraper.py           # CMS LCD/NCD bulk export downloader
│       ├── cms_mpfs_scraper.py      # Medicare Physician Fee Schedule (MPFS)
│       ├── cms_ncci_scraper.py      # NCCI PTP + MUE edits (quarterly)
│       ├── cms_opps_scraper.py      # OPPS / APC Addendum A & B
│       ├── cms_asp_scraper.py       # ASP quarterly drug pricing
│       ├── cms_drg_scraper.py       # MS-DRG weights (annual)
│       ├── cms_carc_scraper.py      # CARC + RARC remittance codes
│       └── payer_policy_scraper.py  # Aetna/UHC/Cigna/Humana policies
├── tagging/
│   ├── __init__.py
│   ├── tagger.py                # DocumentTagger — keyword-scoring classifier
│   └── taxonomy.yaml            # Single source of truth for classification rules (v2.0.0)
├── database/
│   └── migrations/
│       ├── 001_initial_schema.sql       # Core schema + payer seed data
│       ├── 002_session2_additions.sql   # Embedding status, views, indexes
│       └── 003_moat_expansion.sql       # 9 new doc types, 4 payers, structured rate tables
├── tests/
│   ├── test_session2_pipeline.py        # pytest suite — core pipeline (~39 tests)
│   └── test_moat_expansion.py           # pytest suite — moat expansion (~71 tests)
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
# Migrations in database/migrations/ are auto-applied via docker-entrypoint-initdb.d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — at minimum set OPENAI_API_KEY

# 4. If running against a pre-existing DB (not fresh Docker), apply migrations manually
psql $DATABASE_URL -f database/migrations/001_initial_schema.sql
psql $DATABASE_URL -f database/migrations/002_session2_additions.sql
psql $DATABASE_URL -f database/migrations/003_moat_expansion.sql
```

### Default service URLs (from docker-compose)
| Service    | URL / Port              | Credentials                             |
|------------|-------------------------|-----------------------------------------|
| PostgreSQL | `localhost:5432`        | `nexusauth / nexusauth_dev_pass`        |
| Redis      | `localhost:6379`        | none                                    |
| pgAdmin    | `http://localhost:5050` | `admin@nexusauth.dev / nexusauth_admin` |

### Default DATABASE_URL (from `.env.example`)
```
postgresql://nexusauth:nexusauth@localhost:5432/nexusauth
```

> **Note**: The docker-compose password is `nexusauth_dev_pass` but `.env.example` uses `nexusauth`. Keep these in sync if you change either.

---

## Running the Pipeline

```bash
# Run all 5 stages end-to-end (core + extended scrape)
python -m ingestion.pipeline --all

# Individual stages
python -m ingestion.pipeline --scrape     # Stage 1+2: LCD/NCD Scrape + Deduplicate
python -m ingestion.pipeline --extended   # Stage 3: Extended scrape (MPFS/NCCI/OPPS/ASP/DRG/CARC/payers)
python -m ingestion.pipeline --tag        # Stage 4: Tag → knowledge_documents
python -m ingestion.pipeline --embed      # Stage 5: Generate pgvector embeddings

# Combine stages selectively
python -m ingestion.pipeline --scrape --tag
python -m ingestion.pipeline --extended --tag --embed

# Useful flags (work with any stage combination)
python -m ingestion.pipeline --all --max-docs 50          # Limit for testing
python -m ingestion.pipeline --all --dry-run              # Scrape only, no DB writes
python -m ingestion.pipeline --all --log-level DEBUG      # Verbose logging
python -m ingestion.pipeline --all --log-file logs/run.log  # Also write to file
python -m ingestion.pipeline --all --batch-size 50        # Override batch size (default: 100)
python -m ingestion.pipeline --all --output-json out.json # JSON summary to file
python -m ingestion.pipeline --all --database-url $URL    # Override connection string

# CMS LCD/NCD scraper flags
python -m ingestion.pipeline --scrape --no-lcds    # Skip LCD downloads
python -m ingestion.pipeline --scrape --no-ncds    # Skip NCD downloads

# Extended scraper flags (--extended or --all)
python -m ingestion.pipeline --extended --no-mpfs            # Skip MPFS fee schedule
python -m ingestion.pipeline --extended --no-ncci            # Skip NCCI PTP + MUE edits
python -m ingestion.pipeline --extended --no-opps            # Skip OPPS/APC addenda
python -m ingestion.pipeline --extended --no-asp             # Skip ASP drug pricing
python -m ingestion.pipeline --extended --no-drg             # Skip MS-DRG weights
python -m ingestion.pipeline --extended --no-carc            # Skip CARC/RARC codes
python -m ingestion.pipeline --extended --no-payer-policies  # Skip Aetna/UHC/Cigna/Humana
```

The embed stage is silently skipped if `OPENAI_API_KEY` is not set.

---

## Pipeline Architecture

### 5-Stage ETL Flow

```
CMS Bulk Exports (LCD/NCD ZIP)
       │
       ▼
[Stage 1+2] cms_scraper.py → deduplicator.py
       │  SHA-256 dedup, insert into raw_documents
       │
       ▼
[Stage 3]  Extended scrapers → deduplicator.py
       │  MPFS, NCCI, OPPS, ASP, DRG, CARC, payer policies
       │  Also inserts into raw_documents (same dedup pattern)
       │
       ▼
[Stage 4]  tagger_integration.py → tagging/tagger.py
       │  Classify doc type, extract codes, insert into knowledge_documents
       │
       ▼
[Stage 5]  embedder.py → OpenAI text-embedding-3-small
           Chunk text, generate 1536-dim vectors, insert into document_embeddings
```

### Processing Status Lifecycle

`raw_documents.processing_status`:
```
pending → processing → tagged
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

## Module Reference

### `ingestion/pipeline.py` — Orchestrator

CLI entry point. Parses args, connects to DB, runs selected stages in sequence, prints a summary.

Key functions:
- `setup_logging(level, log_file)` — configures structured logging, suppresses urllib3/openai/httpx noise
- `get_db_connection(database_url)` — returns a psycopg2 connection (`autocommit=False`)
- `stage_scrape_and_ingest(conn, include_lcds, include_ncds, dry_run, max_docs)` → stats dict
- `stage_scrape_extended(conn, dry_run, max_docs, include_mpfs, include_ncci, include_opps, include_asp, include_drg, include_carc, include_payer_policies)` → stats dict
- `stage_tag(conn, batch_size, max_docs)` → stats dict
- `stage_embed(conn, batch_size, max_docs)` → stats dict (embed batch capped at 50)
- `build_parser()` — returns the argparse ArgumentParser
- `main(argv)` → exit code (0 = success, 1 = error, 130 = KeyboardInterrupt)

---

### `ingestion/deduplicator.py` — SHA-256 Deduplication

Handles content deduplication and insertion into `raw_documents`.

Key functions/classes:
- `sha256_content(content: str) → str` — SHA-256 hex digest of UTF-8 string
- `is_duplicate(conn, content_hash: str) → bool` — checks DB for existing hash
- `get_existing_id(conn, content_hash: str) → int | None` — returns existing row ID
- `insert_raw_document(conn, doc: dict) → int | None` — inserts with `ON CONFLICT (content_hash) DO NOTHING`, returns new ID or None for duplicates
- `process_batch(conn, documents, batch_size=100, commit_every=100) → DeduplicationStats`
- `mark_processing_status(conn, raw_document_id, status, error_message=None)` — updates status + `processed_at`
- `get_pending_documents(conn, limit=100) → list[dict]` — fetches `processing_status = 'pending'` rows

`DeduplicationStats` fields: `total_seen`, `inserted`, `duplicates`, `errors`, `elapsed_seconds`

---

### `ingestion/tagger_integration.py` — Tagger Bridge

Bridges `raw_documents` → `knowledge_documents` via `DocumentTagger`.

Key functions/classes:
- `tag_and_insert(conn, raw_doc, tagger) → int | None` — runs tagger, resolves payer FK, extracts dates from metadata, inserts into `knowledge_documents` with `ON CONFLICT (raw_document_id) DO UPDATE`
- `process_pending_documents(conn, tagger=None, batch_size=100, max_documents=None) → TaggingStats`
- `_lookup_payer_id(conn, payer_code) → int | None`
- `_extract_effective_date(metadata)` — checks `rev_eff_date`, `orig_det_eff_date`, `ncd_eff_date`, `NCD_efctv_dt`, `mcd_publish_date`
- `_extract_last_updated(metadata)` — checks `last_updated`, `last_updt_tmstmp`, `last_reviewed_on`

`TaggingStats` fields: `total`, `tagged`, `failed`, `needs_review`, `elapsed`

Document `status` in `knowledge_documents`:
- `'active'` — confidence ≥ 0.40
- `'needs_review'` — confidence < 0.40

---

### `ingestion/embedder.py` — pgvector Embedding

Generates and stores OpenAI embeddings for knowledge documents.

Key functions/classes:
- `chunk_text(text, chunk_size=1800, overlap=230, min_size=100) → list[dict]` — returns list of `{chunk_index, chunk_text, char_start, char_end}`. Breaks at `\n\n`, `. `, or word boundaries.
- `embed_texts(texts, model=EMBEDDING_MODEL, client=None) → list[list[float]]` — calls OpenAI API; truncates inputs to 25,000 chars
- `embed_document(conn, doc, client=None) → int` — chunks + embeds one knowledge doc, returns chunk count
- `process_unembedded_documents(conn, client=None, batch_size=50, max_documents=None, rate_limit_sleep=0.5) → EmbeddingStats`
- `semantic_search(conn, query, limit=10, document_type=None, payer_id=None, client=None) → list[dict]` — embeds query, runs pgvector cosine distance search

Constants: `EMBEDDING_MODEL = "text-embedding-3-small"`, `EMBEDDING_DIMENSIONS = 1536`

`EmbeddingStats` fields: `total_docs`, `embedded_docs`, `total_chunks`, `failed_docs`, `api_calls`, `elapsed`

---

### `ingestion/scrapers/cms_scraper.py` — CMS LCD/NCD Scraper

Downloads and parses CMS Medicare Coverage Database bulk exports.

Key functions:
- `scrape_all_cms(include_lcds=True, include_ncds=True)` — generator yielding `RawDocument` dicts
- `_download_zip(url) → BytesIO` — downloads with 120s timeout and `Mozilla/5.0` User-Agent
- `_build_lcd_text(row, contractors) → str` — assembles full-text from LCD CSV fields, stripping HTML
- `_build_ncd_text(row) → str` — assembles full-text from NCD CSV fields
- `_html_to_text(html) → str` — strips HTML tags, collapses whitespace (via BeautifulSoup)
- `_parse_date(value)` — handles `%Y-%m-%d %H:%M:%S.%f`, `%Y-%m-%d %H:%M:%S`, `%Y-%m-%d`

LCD filtering: only status `A` (Active) and `F` (Future) are included; `R` (Retired) and `I` (Inactive) are skipped.

---

### `ingestion/scrapers/cms_mpfs_scraper.py` — MPFS Fee Schedule Scraper

Downloads the annual CMS Medicare Physician Fee Schedule national payment amounts file.

Key functions:
- `scrape_cms_mpfs(url=None, payment_year=None)` — generator yielding batched `fee_schedule` documents (200 HCPCS codes/doc)
- Column name normalisation handles CMS naming variations across years via `_normalise_headers()`
- Env var: `CMS_MPFS_URL` — override for local test fixtures

Each document covers a HCPCS code range and includes RVU components (work, PE, MP) and facility/non-facility national unadjusted payment rates.

---

### `ingestion/scrapers/cms_ncci_scraper.py` — NCCI Edits Scraper

Downloads CMS NCCI Procedure-to-Procedure (PTP) and Medically Unlikely Edit (MUE) files.

Key functions:
- `scrape_cms_ncci_ptp(url=None, data_year=None, data_quarter=None)` — yields batched PTP `ncci_edit` documents (500 pairs/doc)
- `scrape_cms_ncci_mue(url=None, data_year=None, data_quarter=None)` — yields batched MUE `ncci_edit` documents (300 entries/doc)
- `scrape_all_ncci(ptp_url=None, mue_url=None, include_ptp=True, include_mue=True)` — unified entry point
- Handles nested ZIP files and multiple CSV column naming conventions
- Env vars: `CMS_NCCI_PTP_URL`, `CMS_NCCI_MUE_URL`

Modifier indicator meanings: `0`=always bundled, `1`=modifier may allow separate payment, `9`=not applicable.

---

### `ingestion/scrapers/cms_opps_scraper.py` — OPPS/APC Scraper

Downloads CMS Outpatient Prospective Payment System Addendum A and B files.

Key functions:
- `scrape_cms_opps(url=None, payment_year=None)` — generator yielding batched `opps_apc` documents (300 codes/doc)
- Addendum A: APC group payment rates and status indicators
- Addendum B: HCPCS/CPT code → APC mapping with payment rates
- Env var: `CMS_OPPS_URL`

---

### `ingestion/scrapers/cms_asp_scraper.py` — ASP Drug Pricing Scraper

Downloads CMS quarterly Average Sales Price drug pricing files for Medicare Part B drugs.

Key functions:
- `scrape_cms_asp(url=None, effective_year=None, effective_quarter=None)` — generator yielding batched `asp_drug_pricing` documents (150 codes/doc)
- Payment limit for most drugs is ASP + 6%
- URL is derived automatically from current year/quarter; override with `CMS_ASP_URL`

---

### `ingestion/scrapers/cms_drg_scraper.py` — MS-DRG Scraper

Downloads CMS MS-DRG Definitions Manual data for Medicare inpatient hospital payments.

Key functions:
- `scrape_cms_drg(url=None, fiscal_year=None)` — generator yielding batched `drg_policy` documents (100 DRGs/doc)
- Documents grouped by MDC (Major Diagnostic Category) when possible
- Columns: DRG number, title, MDC, type, relative weight, geometric/arithmetic mean LOS
- Env var: `CMS_DRG_URL`

---

### `ingestion/scrapers/cms_carc_scraper.py` — CARC/RARC Scraper

Scrapes CMS Remittance Advice Remark Codes (RARC) and Claim Adjustment Reason Codes (CARC).

Key functions:
- `scrape_rarc_codes(url=None)` — yields `remittance_policy` documents from CMS HTML page
- `scrape_carc_codes(url=None)` — yields `remittance_policy` documents from X12/WPC HTML page
- `scrape_all_remittance_codes(include_rarc=True, include_carc=True)` — unified entry point
- Env vars: `CMS_RARC_URL`, `CMS_CARC_URL`

RARC codes explain claim adjustments on ERA 835 transactions. CARC codes (CO-, PR-, OA-, PI-) define the adjustment category.

---

### `ingestion/scrapers/payer_policy_scraper.py` — Commercial Payer Policy Scraper

Scrapes publicly available clinical policies from major commercial payers.

Key functions:
- `scrape_aetna_cpbs(url=None)` — Aetna Clinical Policy Bulletins → `clinical_policy`
- `scrape_uhc_policies(url=None)` — UHC Medical Policies → `medical_necessity_policy`
- `scrape_cigna_policies(url=None)` — Cigna Coverage Policies → `medical_necessity_policy`
- `scrape_humana_policies(url=None)` — Humana Coverage Determinations → `medical_necessity_policy`
- `scrape_all_payer_policies(include_aetna, include_uhc, include_cigna, include_humana)` — unified entry point
- Respects a 2-second delay between requests; caps at 200 policies per payer
- Env vars: `AETNA_CPB_URL`, `UHC_POLICY_URL`, `CIGNA_POLICY_URL`, `HUMANA_POLICY_URL`

Non-fatal — if a payer site blocks access, that payer is skipped with a warning.

---

### `tagging/tagger.py` — Document Classifier

Keyword-scoring NLP classifier.

**`TaggingResult`** dataclass fields:
```python
payer_code: Optional[str]       # CMS, AETNA, UHC, BCBS, CIGNA, HUMANA, CENTENE, MAGELLAN, or None
document_type: str              # see taxonomy document types below
document_subtype: Optional[str]
specialties: list[str]          # detected medical specialties
cpt_codes: list[str]            # extracted CPT codes
icd10_codes: list[str]          # extracted ICD-10 codes
hcpcs_codes: list[str]          # extracted HCPCS Level II codes
routing_targets: list[str]      # ["NexusAuth"], ["CODEMED"], or ["REVIEW"]
confidence_score: float         # [0.0, 1.0]
requires_review: bool           # True if confidence_score < 0.40
raw_scores: dict[str, float]    # per-type raw scores (not in to_dict())
```

**`DocumentTagger`** class:
- Loads `taxonomy.yaml` on `__init__` (once per instance)
- `tag(text, source_url="") → TaggingResult` — single public method
- Payer detection: URL domain first, then text keywords
- Document type scoring: `(keyword_hits / total_keywords) * weight`, normalised via `score / (score + 0.3)`
- Code extraction regexes:
  - CPT: `\b(\d{5}[A-Z]?)\b` (filters out pure zip codes)
  - ICD-10: `\b([A-Z]\d{2}(?:\.\d{1,4})?)\b`
  - HCPCS: `\b([A-Z]\d{4})\b`

---

## Taxonomy & Classification

**`tagging/taxonomy.yaml` is the single source of truth** for all classification behavior. Current version: **2.0.0** (18 document types, 17 specialties, 8 payers).

### Document Types (18 total)

| Type                       | Label                           | Weight | Routes To  |
|----------------------------|---------------------------------|--------|------------|
| `prior_auth_criteria`      | Prior Authorization Criteria    | 1.5    | NexusAuth  |
| `lcd`                      | Local Coverage Determination    | 2.0    | NexusAuth  |
| `ncd`                      | National Coverage Determination | 2.0    | NexusAuth  |
| `clinical_policy`          | Clinical Policy Bulletin        | 1.3    | NexusAuth  |
| `coverage_determination`   | Coverage Determination          | 1.2    | NexusAuth  |
| `medical_necessity_policy` | Medical Necessity Policy        | 1.6    | NexusAuth  |
| `icd10_guidelines`         | ICD-10 Coding Guidelines        | 1.4    | NexusAuth  |
| `transmittal`              | CMS Transmittal / Change Request| 1.2    | NexusAuth  |
| `fee_schedule`             | Fee Schedule                    | 1.5    | CODEMED    |
| `billing_guidelines`       | Billing Guidelines              | 1.3    | CODEMED    |
| `formulary`                | Drug Formulary                  | 1.4    | CODEMED    |
| `ncci_edit`                | NCCI Edit                       | 1.8    | CODEMED    |
| `remittance_policy`        | Remittance / Denial Code Policy | 1.4    | CODEMED    |
| `modifier_policy`          | Modifier Policy                 | 1.5    | CODEMED    |
| `drg_policy`               | MS-DRG Policy                   | 1.6    | CODEMED    |
| `opps_apc`                 | OPPS / APC Payment              | 1.6    | CODEMED    |
| `asp_drug_pricing`         | ASP Drug Pricing                | 1.5    | CODEMED    |
| `unknown`                  | Unknown / Unclassified          | 0.0    | REVIEW     |

### Routing Matrix

```yaml
NexusAuth:  prior_auth_criteria (high), lcd (high), ncd (high),
            medical_necessity_policy (high), clinical_policy (medium),
            coverage_determination (medium), icd10_guidelines (medium),
            transmittal (low)
CODEMED:    ncci_edit (high), remittance_policy (high), modifier_policy (high),
            drg_policy (high), opps_apc (high), asp_drug_pricing (high),
            fee_schedule (high), billing_guidelines (high), formulary (medium)
REVIEW:     unknown (low)
```

### Confidence Thresholds

| Threshold         | Value | Behaviour                            |
|-------------------|-------|--------------------------------------|
| `auto_accept`     | 0.75  | Accept classification automatically  |
| `review_required` | 0.40  | Flag `requires_review = True`        |
| `reject`          | 0.40  | Classify as `unknown`, flag for review |

### Detected Medical Specialties (17 total)

cardiology, oncology, orthopedics, neurology, radiology, gastroenterology, pulmonology, rheumatology, endocrinology, behavioral_health, nephrology, dermatology, ophthalmology, physical_therapy, infectious_disease, hematology, allergy_immunology

### Payer Domain Mappings (8 payers)

| Payer    | Domains                                                                                          |
|----------|--------------------------------------------------------------------------------------------------|
| CMS      | cms.gov, medicare.gov, medicaid.gov, lcd.cms.gov, coverage.cms.gov                              |
| AETNA    | aetna.com, aetnabetterhealth.com                                                                 |
| UHC      | uhcprovider.com, unitedhealthcare.com, optum.com, myuhc.com                                     |
| BCBS     | bcbs.com, bcbsa.com, anthem.com, highmark.com, carefirst.com, premera.com, regence.com          |
| CIGNA    | cigna.com, evernorth.com, cignahealthspring.com                                                  |
| HUMANA   | humana.com, humanaone.com, humanamilitary.com                                                    |
| CENTENE  | centene.com, wellcare.com, meridianhealth.com, healthnet.com                                     |
| MAGELLAN | magellanhealth.com, magellanrx.com, magelanprovider.com                                         |

To change classification behavior, **edit `taxonomy.yaml`**, not `tagger.py`.

---

## Database Schema Conventions

- **Primary keys**: `UUID DEFAULT uuid_generate_v4()` — always named `id`
- **Timestamps**: All tables have `created_at` and `updated_at TIMESTAMPTZ`, with auto-update triggers
- **Arrays**: Medical codes and specialties stored as `TEXT[]` with GIN indexes
- **Flexible metadata**: `JSONB` columns for scraper metadata, tagger audit trails, job configs
- **Audit log**: All mutations to `knowledge_documents` and `routing_rules` are captured in `audit_log` (HIPAA requirement)
- **Vector index**: HNSW index on `document_embeddings.embedding` (`cosine_ops`, m=16, ef_construction=64)

### Key Tables

| Table                  | Purpose                                           |
|------------------------|---------------------------------------------------|
| `raw_documents`        | Landing zone for scraped content                  |
| `knowledge_documents`  | Classified, structured document layer             |
| `document_embeddings`  | pgvector chunks for semantic search               |
| `payers`               | Master payer registry (8 payers)                  |
| `routing_rules`        | document_type → tool routing configuration        |
| `scraper_jobs`         | Pipeline run tracking                             |
| `audit_log`            | HIPAA-compliant change history                    |
| `cpt_codes`            | Reference table for CPT procedure codes           |
| `icd10_codes`          | Reference table for ICD-10 diagnosis codes        |
| `hcpcs_codes`          | Reference table for HCPCS Level II codes          |
| `ncci_edits`           | Structured NCCI PTP/MUE edit data (O(1) lookups)  |
| `mpfs_rates`           | Structured MPFS per-code RVU and payment rates    |
| `drg_rates`            | Structured MS-DRG relative weights and LOS data   |
| `asp_drug_rates`       | Structured ASP quarterly drug pricing             |

### ENUM Types (from migrations 001 + 003)

- `document_type_enum`: prior_auth_criteria, fee_schedule, clinical_policy, coverage_determination, formulary, billing_guidelines, ncd, lcd, unknown, ncci_edit, remittance_policy, modifier_policy, drg_policy, opps_apc, asp_drug_pricing, icd10_guidelines, medical_necessity_policy, transmittal
- `processing_status_enum`: pending, processing, tagged, embedded, failed, review_required
- `job_status_enum`: queued, running, completed, failed, cancelled
- `audit_action_enum`: INSERT, UPDATE, DELETE, SCRAPE, TAG, EMBED, ROUTE

### knowledge_documents — Additional Columns (migration 003)

- `data_year SMALLINT` — CMS data year (especially relevant for fee schedules, NCCI, ASP — updated annually/quarterly)
- `data_quarter SMALLINT CHECK (1–4)` — CMS data quarter

### Useful Views

From migration 002:
- `pipeline_status` — Row counts by layer and processing status (extended in migration 003 to cover ncci_edits, mpfs_rates, drg_rates, asp_drug_rates)
- `documents_needing_review` — Human review queue (confidence < 0.40)

From migration 003:
- `codemed_documents` — All CODEMED-routed documents with payer and data period
- `nexusauth_documents` — All NexusAuth-routed documents with codes and specialties
- `current_mpfs_rates` — Latest MPFS rate per HCPCS code/modifier (`DISTINCT ON payment_year DESC`)
- `current_drg_rates` — Latest DRG weight per DRG number (`DISTINCT ON fiscal_year DESC`)
- `current_asp_rates` — Latest ASP price per HCPCS code (`DISTINCT ON year/quarter DESC`)

### Structured Rate Table Schemas

**`ncci_edits`**: `edit_type` (PTP/MUE), `column1_code`, `column2_code`, `modifier_indicator` (0/1/9), `mue_value`, `mue_adjudication_indicator` (1=claim/2=DOS/3=RA), `effective_date`, `deletion_date`, `data_year`, `data_quarter`. Unique on `(column1_code, column2_code, edit_type, data_year, data_quarter)`.

**`mpfs_rates`**: `hcpcs_code`, `modifier`, `description`, `work_rvu`, `non_fac_pe_rvu`, `fac_pe_rvu`, `mp_rvu`, `non_fac_total_rvu`, `fac_total_rvu`, `non_fac_payment`, `fac_payment`, `global_period`, `status_code`, `payment_year`. Unique on `(hcpcs_code, modifier, payment_year)`.

**`drg_rates`**: `drg_number`, `drg_title`, `mdc`, `drg_type` (MEDSURG/SURGICAL/MEDICAL), `relative_weight`, `geometric_mean_los`, `arithmetic_mean_los`, `fiscal_year`. Unique on `(drg_number, fiscal_year)`.

**`asp_drug_rates`**: `hcpcs_code`, `short_description`, `dosage_form`, `asp_price`, `asp_plus_6_payment`, `effective_year`, `effective_quarter`. Unique on `(hcpcs_code, effective_year, effective_quarter)`.

### Migration Notes
- Migrations are mounted as `docker-entrypoint-initdb.d` and auto-apply on fresh Docker containers
- Migration 003 adds 9 new `document_type_enum` values, 4 new payers (CIGNA, HUMANA, CENTENE, MAGELLAN), 9 new routing rules, 4 structured rate tables, `data_year`/`data_quarter` to `knowledge_documents`, and 5 new operational views
- All migration DDL uses `IF NOT EXISTS` / `ON CONFLICT DO NOTHING` — safe to re-run

---

## Module Conventions

### Batch Processing Pattern
All processing modules follow the same pattern:
1. Fetch a batch of `pending` records
2. Mark as `processing` to prevent concurrent re-processing
3. Process each record, collecting results
4. Mark as `tagged`/`embedded`/`failed` based on outcome
5. Return a `*Stats` dataclass with counts and elapsed time

Default batch sizes:
- Deduplicator: **100 documents**, commits every 100
- Tagger: **100 documents**, logs progress every 50
- Embedder: **50 documents**, logs progress every 10, 0.5s sleep between API calls
- Extended scrapers: batched internally (200 HCPCS/doc for MPFS, 500 pairs/doc for NCCI PTP, 300 entries/doc for NCCI MUE, 300 codes/doc for OPPS, 150 codes/doc for ASP, 100 DRGs/doc for DRG)

### Extended Scraper Error Handling
Each extended scraper source is wrapped in an individual `try/except` — a failed source logs a warning and is skipped, allowing the others to proceed. This makes the extended stage resilient to CMS site changes or temporary network issues.

### Error Handling
- **Duplicates**: Counted and skipped silently via `ON CONFLICT DO NOTHING`
- **Processing errors**: Record marked `failed`, error stored in `processing_error` column, pipeline continues
- **Transaction failures**: `conn.rollback()` then continue to next record
- **Fatal errors**: Logged and propagated to `main()` which returns exit code 1
- **KeyboardInterrupt**: Rolls back, exits with code 130

### Logging
Configured centrally in `pipeline.py` via `setup_logging()`. Suppresses urllib3, openai, httpx loggers. Format: `YYYY-MM-DD HH:MM:SS [LEVEL   ] module: message`.

### Configuration
All runtime config comes from environment variables (via `python-dotenv`). Never hardcode credentials. See `.env.example` for all available variables:

| Variable              | Default                                   | Purpose                              |
|-----------------------|-------------------------------------------|--------------------------------------|
| `DATABASE_URL`        | `postgresql://nexusauth:nexusauth@...`    | PostgreSQL connection string         |
| `REDIS_URL`           | `redis://localhost:6379/0`               | Redis job queue                      |
| `OPENAI_API_KEY`      | *(required for embed stage)*             | OpenAI embeddings                    |
| `OPENAI_BASE_URL`     | *(optional)*                             | Custom OpenAI-compatible endpoint    |
| `PIPELINE_MAX_DOCS`   | *(no limit)*                             | Max docs per run                     |
| `PIPELINE_BATCH_SIZE` | `100`                                    | DB batch size                        |
| `LOG_LEVEL`           | `INFO`                                   | Logging verbosity                    |
| `LOG_FILE`            | `logs/pipeline.log`                      | Log file path                        |
| `CMS_LCD_URL`         | CMS bulk export URL                      | Override for local test files        |
| `CMS_NCD_URL`         | CMS bulk export URL                      | Override for local test files        |
| `CMS_MPFS_URL`        | *(derived from current year)*            | MPFS ZIP override                    |
| `CMS_NCCI_PTP_URL`    | *(derived from current year)*            | NCCI PTP ZIP override                |
| `CMS_NCCI_MUE_URL`    | CMS MUE practitioner URL                 | NCCI MUE ZIP override                |
| `CMS_OPPS_URL`        | CMS OPPS addenda ZIP URL                 | OPPS ZIP override                    |
| `CMS_ASP_URL`         | *(derived from current year/quarter)*    | ASP ZIP override                     |
| `CMS_DRG_URL`         | *(derived from current year)*            | DRG ZIP override                     |
| `CMS_RARC_URL`        | CMS RARC HTML page URL                   | RARC source override                 |
| `CMS_CARC_URL`        | X12/WPC CARC HTML page URL               | CARC source override                 |
| `AETNA_CPB_URL`       | Aetna CPB index page URL                 | Aetna policy index override          |
| `UHC_POLICY_URL`      | UHC provider policy index URL            | UHC policy index override            |
| `CIGNA_POLICY_URL`    | Cigna clinical coverage policy URL       | Cigna policy index override          |
| `HUMANA_POLICY_URL`   | Humana coverage guidelines URL           | Humana policy index override         |

---

## Embedding Details

- **Model**: `text-embedding-3-small` (1536 dimensions) — cost-efficient default
- **Alternative**: `text-embedding-3-large` (3072 dims) for higher quality
- **Chunk size**: ~1800 chars (~512 tokens at 3.5 chars/token) with 230-char (~64 token) overlap
- **Chunk strategy**: Boundary-aware — prefers `\n\n`, then `. `, then word boundaries
- **Min chunk size**: 100 chars — tiny trailing chunks are discarded
- **Rate limiting**: 0.5s sleep between OpenAI API calls
- **Truncation**: Input capped at 25,000 chars before embedding (7,000 tokens, safe below the 8,191 limit)
- **Title prepending**: `f"{title}\n\n{content}"` is embedded, not just content

### Semantic Search
```python
embedder.semantic_search(
    conn, query,
    limit=10,
    document_type=None,   # filter by doc type
    payer_id=None,        # filter by payer FK
    client=None
)
```
Returns list of dicts with knowledge_document fields + `chunk_text` + `similarity` score.

---

## CMS Data Sources

| Data           | URL / Notes                                                                                              |
|----------------|----------------------------------------------------------------------------------------------------------|
| LCD (zip)      | `https://downloads.cms.gov/medicare-coverage-database/downloads/exports/current_lcd.zip`                |
| NCD (zip)      | `https://downloads.cms.gov/medicare-coverage-database/downloads/exports/ncd.zip`                        |
| MPFS (zip)     | CMS Physician Fee Schedule Downloads — annual Jan update; URL derived from current year                 |
| NCCI PTP (zip) | `https://www.cms.gov/files/zip/ncci-practitioner-py{yy}.zip` — quarterly                               |
| NCCI MUE (zip) | `https://www.cms.gov/files/zip/ncci-mue-practitioner.zip` — quarterly                                  |
| OPPS (zip)     | CMS OPPS Addenda and Updates — annual                                                                    |
| ASP (zip)      | CMS ASP Files — quarterly; URL derived from current year/quarter                                        |
| DRG (zip)      | CMS IPPS/MS-DRG Grouper — annual Oct update; URL derived from current fiscal year                      |
| RARC (HTML)    | `https://www.cms.gov/medicare/claim-based-resources/remittance-advice-remark-codes`                    |
| CARC (HTML)    | `https://x12.org/codes/claim-adjustment-reason-codes`                                                  |

The LCD/NCD scraper downloads the outer ZIP, extracts an inner `*_csv.zip`, and reads CSV files.
- LCD CSVs joined: `lcd.csv` + `contractor.csv` + `lcd_x_contractor.csv`
- Only LCD statuses `A` (Active) and `F` (Future) are ingested; `R`/`I` are skipped

Extended scrapers derive current-year/quarter URLs automatically from `datetime.utcnow()`. Set the corresponding env var (e.g. `CMS_MPFS_URL`) to a `file:///` path for local testing.

---

## Test Structure

Tests live in two files:

### `tests/test_session2_pipeline.py` — Core Pipeline (~39 tests)

| Class                      | Count | Type                  | Docker? |
|----------------------------|-------|-----------------------|---------|
| `TestDeduplication`        | 6     | Unit (mock DB)        | No      |
| `TestTextChunker`          | 8     | Unit (pure functions) | No      |
| `TestCMSScraper`           | 9     | Unit (fixture data)   | No      |
| `TestTaggerIntegration`    | 8     | Unit (real tagger)    | No      |
| `TestPipelineOrchestrator` | 5     | Unit (CLI parsing)    | No      |
| `TestIntegrationSmoke`     | 2     | Integration           | Yes     |

### `tests/test_moat_expansion.py` — Moat Expansion (~71 tests)

| Class                      | Count | Type                    | Docker? |
|----------------------------|-------|-------------------------|---------|
| `TestNewDocumentTypes`     | 9     | Unit (real tagger)      | No      |
| `TestNewSpecialties`       | 7     | Unit (real tagger)      | No      |
| `TestNewPayers`            | 6     | Unit (real tagger)      | No      |
| `TestMPFSScraper`          | 7     | Unit (fixture data)     | No      |
| `TestNCCIScraper`          | 8     | Unit (fixture data)     | No      |
| `TestASPScraper`           | 5     | Unit (fixture data)     | No      |
| `TestDRGScraper`           | 5     | Unit (fixture data)     | No      |
| `TestCARCScraper`          | 7     | Unit (fixture data)     | No      |
| `TestTaxonomyStructure`    | 8     | Unit (taxonomy.yaml)    | No      |
| `TestPipelineCLIExtended`  | 9     | Unit (CLI parsing)      | No      |

### Running Tests

```bash
# All unit tests (no Docker required)
pytest tests/ -v

# Skip integration tests
pytest tests/ -k "not integration"

# Core pipeline tests only
pytest tests/test_session2_pipeline.py -v

# Moat expansion tests only
pytest tests/test_moat_expansion.py -v

# Specific test class
pytest tests/ -k "TestMPFSScraper" -v
pytest tests/ -k "TestNCCIScraper" -v
pytest tests/ -k "TestTaxonomyStructure" -v
pytest tests/ -k "TestPipelineCLIExtended" -v

# With coverage
pytest tests/ --cov=ingestion --cov=tagging

# Integration smoke tests (requires Docker stack)
pytest tests/ -k "integration" -v
```

Integration tests are marked `@pytest.mark.integration` and require the Docker stack. They will `pytest.skip()` gracefully if the DB is unavailable.

Test fixtures in `test_session2_pipeline.py`:
- `SAMPLE_LCD_ROW` — realistic LCD CSV row dict (psychiatric diagnostic evaluation)
- `SAMPLE_NCD_ROW` — realistic NCD CSV row dict (clinical trials coverage)

New pipeline stages require tests following this pattern: pure-function unit tests in a new `Test*` class, plus `@pytest.mark.integration` DB tests in `TestIntegrationSmoke`.

---

## Key Conventions for AI Assistants

1. **Never modify `taxonomy.yaml` structure** without also updating `tagger.py` parsing logic — they are tightly coupled. `DocumentTagger.__init__` directly accesses `taxonomy["document_types"]`, `taxonomy["payer_domains"]`, `taxonomy["specialties"]`, `taxonomy["routing_matrix"]`, `taxonomy["confidence_thresholds"]`.

2. **Migrations are append-only** — never edit existing migration files. Add new `00N_*.sql` files for schema changes. Docker auto-applies migrations from `database/migrations/` on first start.

3. **All pipeline stages must remain idempotent** — selecting on status fields and using `ON CONFLICT DO NOTHING` / `ON CONFLICT ... DO UPDATE` is the pattern.

4. **Test coverage is mandatory for new pipeline stages** — follow the existing pattern in `test_moat_expansion.py` with unit tests for pure functions and `@pytest.mark.integration` for DB-dependent tests.

5. **No REST API** — this is a CLI pipeline. Do not add Flask/FastAPI unless explicitly requested.

6. **Environment variables only** — never hardcode URLs, credentials, or API keys. Add new config to `.env.example` with a comment.

7. **Batch size defaults**: Tag/dedup default to 100; embed defaults to 50 (OpenAI rate limit concern). Keep these defaults unless there's a specific reason to change them.

8. **HIPAA audit compliance**: Any new writes to `knowledge_documents` or `routing_rules` will automatically be captured by existing audit triggers. Do not bypass triggers.

9. **UUID primary keys everywhere** — never use integer auto-increment PKs for new tables.

10. **Array columns for multi-value medical data** — use `TEXT[]` with GIN indexes for CPT, ICD-10, HCPCS, and specialties columns.

11. **Routing targets are strings, not enums** — routing_targets column is `TEXT[]` containing `"NexusAuth"`, `"CODEMED"`, or `"REVIEW"`. Match these exact strings when filtering.

12. **CODEMED handles financial/coding documents** — fee schedules, billing guidelines, formularies, NCCI edits, remittance/modifier/DRG/OPPS/ASP documents all route to CODEMED. Do not merge these routing paths with NexusAuth.

13. **The tagger runs twice per document** in `process_pending_documents` — once in `tag_and_insert()` and once to check `requires_review` for stats. This is a known implementation detail, not a bug to fix unless performance is a concern.

14. **Extended scrapers are individually non-fatal** — each source in `stage_scrape_extended` is wrapped in `try/except`. A single scraper failure logs a warning and is skipped; it never aborts the entire extended stage.

15. **Structured rate tables are supplemental** — `ncci_edits`, `mpfs_rates`, `drg_rates`, and `asp_drug_rates` provide O(1) code lookups without full-text search. They have their own unique constraints and are separate from the `knowledge_documents` embedding flow.

16. **taxonomy.yaml version** — the current taxonomy is v2.0.0 with 18 doc types and 17 specialties. When adding new doc types, increment the version comment in the file and add the new enum value in a new migration file.
