-- ============================================================
-- NexusAuth Knowledge Layer — Initial Schema
-- Migration: 001_initial_schema.sql
-- PostgreSQL 16 + pgvector
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgvector";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- trigram search on text fields

-- ============================================================
-- ENUM TYPES
-- ============================================================

CREATE TYPE document_type_enum AS ENUM (
    'prior_auth_criteria',
    'fee_schedule',
    'clinical_policy',
    'coverage_determination',
    'formulary',
    'billing_guidelines',
    'ncd',
    'lcd',
    'unknown'
);

CREATE TYPE processing_status_enum AS ENUM (
    'pending',
    'processing',
    'tagged',
    'embedded',
    'failed',
    'review_required'
);

CREATE TYPE job_status_enum AS ENUM (
    'queued',
    'running',
    'completed',
    'failed',
    'cancelled'
);

CREATE TYPE audit_action_enum AS ENUM (
    'INSERT',
    'UPDATE',
    'DELETE',
    'SCRAPE',
    'TAG',
    'EMBED',
    'ROUTE'
);

-- ============================================================
-- PAYERS — Master payer registry
-- ============================================================

CREATE TABLE payers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payer_code      VARCHAR(20) UNIQUE NOT NULL,   -- e.g. 'CMS', 'AETNA', 'UHC'
    payer_name      VARCHAR(255) NOT NULL,
    payer_type      VARCHAR(50),                   -- 'federal', 'commercial', 'medicaid'
    domains         TEXT[],                        -- source domains for auto-detection
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed core payers
INSERT INTO payers (payer_code, payer_name, payer_type, domains) VALUES
    ('CMS',   'Centers for Medicare & Medicaid Services', 'federal',
     ARRAY['cms.gov', 'medicare.gov', 'medicaid.gov', 'lcd.cms.gov']),
    ('AETNA', 'Aetna Inc.',                              'commercial',
     ARRAY['aetna.com', 'aetnabetterhealth.com']),
    ('UHC',   'UnitedHealthcare',                        'commercial',
     ARRAY['uhcprovider.com', 'unitedhealthcare.com', 'optum.com']),
    ('BCBS',  'Blue Cross Blue Shield',                  'commercial',
     ARRAY['bcbs.com', 'bcbsa.com', 'anthem.com', 'highmark.com']);

-- ============================================================
-- CODE REFERENCE TABLES
-- ============================================================

CREATE TABLE cpt_codes (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code        VARCHAR(10) UNIQUE NOT NULL,
    description TEXT,
    category    VARCHAR(100),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE icd10_codes (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code        VARCHAR(10) UNIQUE NOT NULL,
    description TEXT,
    category    VARCHAR(100),
    billable    BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE hcpcs_codes (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code        VARCHAR(10) UNIQUE NOT NULL,
    description TEXT,
    level       CHAR(1),   -- 'I' = Level I (CPT), 'II' = Level II
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- RAW DOCUMENTS — Ingestion landing zone
-- Everything hits here first before processing
-- ============================================================

CREATE TABLE raw_documents (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_url      TEXT NOT NULL,
    source_domain   VARCHAR(255),
    content_hash    VARCHAR(64) UNIQUE NOT NULL,   -- SHA-256 for deduplication
    raw_html        TEXT,
    raw_text        TEXT,
    content_type    VARCHAR(100),                  -- 'text/html', 'application/pdf', etc.
    byte_size       INTEGER,
    http_status     INTEGER,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    scraper_job_id  UUID,
    processing_status processing_status_enum DEFAULT 'pending',
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX idx_raw_documents_hash         ON raw_documents (content_hash);
CREATE INDEX idx_raw_documents_domain       ON raw_documents (source_domain);
CREATE INDEX idx_raw_documents_status       ON raw_documents (processing_status);
CREATE INDEX idx_raw_documents_scraped_at   ON raw_documents (scraped_at DESC);

-- ============================================================
-- KNOWLEDGE DOCUMENTS — Structured, tagged, queryable layer
-- ============================================================

CREATE TABLE knowledge_documents (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_document_id     UUID REFERENCES raw_documents(id) ON DELETE SET NULL,
    payer_id            UUID REFERENCES payers(id) ON DELETE SET NULL,

    -- Document identity
    title               TEXT,
    document_type       document_type_enum DEFAULT 'unknown',
    document_subtype    VARCHAR(100),
    source_url          TEXT,
    source_domain       VARCHAR(255),

    -- CMS-specific identifiers
    lcd_id              VARCHAR(20),    -- e.g. 'L33822'
    ncd_id              VARCHAR(20),    -- e.g. '20.4'
    policy_number       VARCHAR(50),

    -- Content
    content_text        TEXT,
    content_summary     TEXT,
    effective_date      DATE,
    revision_date       DATE,
    expiration_date     DATE,

    -- Tagging outputs
    specialties         TEXT[],
    cpt_codes           TEXT[],
    icd10_codes         TEXT[],
    hcpcs_codes         TEXT[],
    routing_targets     TEXT[],        -- ['NexusAuth', 'CODEMED']
    confidence_score    FLOAT,
    requires_review     BOOLEAN DEFAULT FALSE,

    -- Processing state
    processing_status   processing_status_enum DEFAULT 'tagged',
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    metadata            JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX idx_knowledge_docs_payer        ON knowledge_documents (payer_id);
CREATE INDEX idx_knowledge_docs_type         ON knowledge_documents (document_type);
CREATE INDEX idx_knowledge_docs_lcd          ON knowledge_documents (lcd_id);
CREATE INDEX idx_knowledge_docs_ncd          ON knowledge_documents (ncd_id);
CREATE INDEX idx_knowledge_docs_specialties  ON knowledge_documents USING GIN (specialties);
CREATE INDEX idx_knowledge_docs_cpt          ON knowledge_documents USING GIN (cpt_codes);
CREATE INDEX idx_knowledge_docs_icd10        ON knowledge_documents USING GIN (icd10_codes);
CREATE INDEX idx_knowledge_docs_routing      ON knowledge_documents USING GIN (routing_targets);
CREATE INDEX idx_knowledge_docs_status       ON knowledge_documents (processing_status);
CREATE INDEX idx_knowledge_docs_review       ON knowledge_documents (requires_review) WHERE requires_review = TRUE;
CREATE INDEX idx_knowledge_docs_title_trgm   ON knowledge_documents USING GIN (title gin_trgm_ops);

-- ============================================================
-- DOCUMENT EMBEDDINGS — pgvector storage for semantic search
-- ============================================================

CREATE TABLE document_embeddings (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    knowledge_document_id   UUID REFERENCES knowledge_documents(id) ON DELETE CASCADE,
    chunk_index             INTEGER NOT NULL,   -- 0-based chunk position
    chunk_text              TEXT NOT NULL,
    chunk_tokens            INTEGER,
    embedding               VECTOR(1536),       -- OpenAI text-embedding-3-small dimension
    embedding_model         VARCHAR(100) DEFAULT 'text-embedding-3-small',
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (knowledge_document_id, chunk_index)
);

-- HNSW index for fast approximate nearest-neighbor search
CREATE INDEX idx_embeddings_hnsw ON document_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_embeddings_doc_id ON document_embeddings (knowledge_document_id);

-- ============================================================
-- ROUTING RULES — Configurable routing logic
-- ============================================================

CREATE TABLE routing_rules (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name       VARCHAR(100) UNIQUE NOT NULL,
    document_type   document_type_enum,
    payer_code      VARCHAR(20),
    specialty       VARCHAR(100),
    target_tool     VARCHAR(50) NOT NULL,   -- 'NexusAuth', 'CODEMED', 'BOTH', 'REVIEW'
    priority        INTEGER DEFAULT 100,
    is_active       BOOLEAN DEFAULT TRUE,
    conditions      JSONB DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default routing rules (mirrors taxonomy.yaml routing matrix)
INSERT INTO routing_rules (rule_name, document_type, target_tool, priority) VALUES
    ('prior_auth_to_nexusauth',     'prior_auth_criteria',      'NexusAuth', 10),
    ('lcd_to_nexusauth',            'lcd',                      'NexusAuth', 10),
    ('ncd_to_nexusauth',            'ncd',                      'NexusAuth', 10),
    ('clinical_policy_to_nexusauth','clinical_policy',          'NexusAuth', 20),
    ('coverage_det_to_nexusauth',   'coverage_determination',   'NexusAuth', 20),
    ('fee_schedule_to_codemed',     'fee_schedule',             'CODEMED',   10),
    ('billing_to_codemed',          'billing_guidelines',       'CODEMED',   10),
    ('formulary_to_codemed',        'formulary',                'CODEMED',   20),
    ('unknown_to_review',           'unknown',                  'REVIEW',    99);

-- ============================================================
-- SCRAPER JOBS — Job tracking for the pipeline
-- ============================================================

CREATE TABLE scraper_jobs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name        VARCHAR(100) NOT NULL,
    scraper_type    VARCHAR(50) NOT NULL,    -- 'cms_lcd', 'cms_ncd', 'aetna_cpb', etc.
    target_url      TEXT,
    status          job_status_enum DEFAULT 'queued',
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    documents_found INTEGER DEFAULT 0,
    documents_new   INTEGER DEFAULT 0,
    documents_dupe  INTEGER DEFAULT 0,
    documents_failed INTEGER DEFAULT 0,
    error_message   TEXT,
    config          JSONB DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_scraper_jobs_status  ON scraper_jobs (status);
CREATE INDEX idx_scraper_jobs_type    ON scraper_jobs (scraper_type);
CREATE INDEX idx_scraper_jobs_created ON scraper_jobs (created_at DESC);

-- ============================================================
-- AUDIT LOG — HIPAA-compliant change tracking
-- ============================================================

CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    record_id       UUID,
    action          audit_action_enum NOT NULL,
    old_values      JSONB,
    new_values      JSONB,
    changed_by      VARCHAR(100) DEFAULT 'system',
    changed_at      TIMESTAMPTZ DEFAULT NOW(),
    ip_address      INET,
    session_id      VARCHAR(100),
    notes           TEXT
);

CREATE INDEX idx_audit_log_table     ON audit_log (table_name, record_id);
CREATE INDEX idx_audit_log_action    ON audit_log (action);
CREATE INDEX idx_audit_log_changed   ON audit_log (changed_at DESC);

-- ============================================================
-- AUDIT TRIGGER FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION audit_trigger_fn()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, record_id, action, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', row_to_json(NEW)::JSONB);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE',
                row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', row_to_json(OLD)::JSONB);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Attach audit triggers to key tables
CREATE TRIGGER audit_knowledge_documents
    AFTER INSERT OR UPDATE OR DELETE ON knowledge_documents
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn();

CREATE TRIGGER audit_routing_rules
    AFTER INSERT OR UPDATE OR DELETE ON routing_rules
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn();

-- ============================================================
-- UPDATED_AT AUTO-UPDATE FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at_payers
    BEFORE UPDATE ON payers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER set_updated_at_knowledge_documents
    BEFORE UPDATE ON knowledge_documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER set_updated_at_routing_rules
    BEFORE UPDATE ON routing_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
