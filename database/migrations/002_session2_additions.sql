-- =============================================================================
-- NexusAuth — Migration 002: Session 2 Additions
-- =============================================================================
-- Adds columns and indexes needed by the Session 2 ingestion pipeline.
-- Safe to run multiple times (uses IF NOT EXISTS / DO NOTHING patterns).
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- raw_documents: add processing_error column for failed document tracking
-- ---------------------------------------------------------------------------
ALTER TABLE raw_documents
    ADD COLUMN IF NOT EXISTS processing_error TEXT;

ALTER TABLE raw_documents
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- knowledge_documents: add embedding_status and embedded_at columns
-- ---------------------------------------------------------------------------
ALTER TABLE knowledge_documents
    ADD COLUMN IF NOT EXISTS embedding_status VARCHAR(20)
        NOT NULL DEFAULT 'pending'
        CHECK (embedding_status IN ('pending', 'embedded', 'failed', 'skipped'));

ALTER TABLE knowledge_documents
    ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- knowledge_documents: add tagger_metadata JSONB column for audit trail
-- ---------------------------------------------------------------------------
ALTER TABLE knowledge_documents
    ADD COLUMN IF NOT EXISTS tagger_metadata JSONB;

-- ---------------------------------------------------------------------------
-- knowledge_documents: ensure routing_targets is an array column
-- ---------------------------------------------------------------------------
-- (Already defined in 001 as TEXT[], this is a safety check)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'knowledge_documents'
          AND column_name = 'routing_targets'
    ) THEN
        ALTER TABLE knowledge_documents ADD COLUMN routing_targets TEXT[];
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- document_embeddings: add unique constraint on (knowledge_document_id, chunk_index)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_embeddings_doc_chunk_unique'
    ) THEN
        ALTER TABLE document_embeddings
            ADD CONSTRAINT document_embeddings_doc_chunk_unique
            UNIQUE (knowledge_document_id, chunk_index);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Indexes for ingestion pipeline performance
-- ---------------------------------------------------------------------------

-- raw_documents: fast lookup of pending documents
CREATE INDEX IF NOT EXISTS idx_raw_documents_processing_status
    ON raw_documents (processing_status)
    WHERE processing_status IN ('pending', 'processing');

-- raw_documents: fast lookup by scraped_at for ordering
CREATE INDEX IF NOT EXISTS idx_raw_documents_scraped_at
    ON raw_documents (scraped_at DESC);

-- raw_documents: fast lookup by source_domain
CREATE INDEX IF NOT EXISTS idx_raw_documents_source_domain
    ON raw_documents (source_domain);

-- knowledge_documents: fast lookup of unembedded documents
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_embedding_status
    ON knowledge_documents (embedding_status)
    WHERE embedding_status = 'pending';

-- knowledge_documents: fast lookup by document_type
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_document_type
    ON knowledge_documents (document_type);

-- knowledge_documents: fast lookup by payer_id
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_payer_id
    ON knowledge_documents (payer_id)
    WHERE payer_id IS NOT NULL;

-- knowledge_documents: full-text search index on title
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_title_fts
    ON knowledge_documents USING gin(to_tsvector('english', title));

-- knowledge_documents: full-text search index on content_text
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_content_fts
    ON knowledge_documents USING gin(to_tsvector('english', content_text));

-- document_embeddings: pgvector HNSW index for fast ANN search
-- HNSW is preferred over IVFFlat for production (better recall, no training needed)
CREATE INDEX IF NOT EXISTS idx_document_embeddings_hnsw
    ON document_embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- document_embeddings: fast lookup by knowledge_document_id
CREATE INDEX IF NOT EXISTS idx_document_embeddings_knowledge_doc_id
    ON document_embeddings (knowledge_document_id);

-- ---------------------------------------------------------------------------
-- scraper_jobs: add new job type values
-- ---------------------------------------------------------------------------
-- Extend the job_type check if it exists as a constraint
DO $$
BEGIN
    -- Drop old constraint if it's too restrictive
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'scraper_jobs_job_type_check'
    ) THEN
        ALTER TABLE scraper_jobs DROP CONSTRAINT scraper_jobs_job_type_check;
        ALTER TABLE scraper_jobs ADD CONSTRAINT scraper_jobs_job_type_check
            CHECK (job_type IN (
                'cms_lcd',
                'cms_ncd',
                'cms_lcd_all',
                'aetna_cpb',
                'uhc_policy',
                'bcbs_policy',
                'full_pipeline',
                'tag_only',
                'embed_only'
            ));
    END IF;
END $$;

-- scraper_jobs: add result_summary JSONB for pipeline output
ALTER TABLE scraper_jobs
    ADD COLUMN IF NOT EXISTS result_summary JSONB;

-- ---------------------------------------------------------------------------
-- Seed: ensure CMS payer exists in payers table
-- ---------------------------------------------------------------------------
INSERT INTO payers (payer_code, payer_name, payer_type, website_url)
VALUES ('CMS', 'Centers for Medicare & Medicaid Services', 'government', 'https://www.cms.gov')
ON CONFLICT (payer_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- View: pipeline_status — quick overview of ingestion progress
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
    'raw_documents'::TEXT AS layer,
    processing_status AS status,
    COUNT(*) AS count,
    MAX(scraped_at) AS latest_activity
FROM raw_documents
GROUP BY processing_status

UNION ALL

SELECT
    'knowledge_documents'::TEXT AS layer,
    embedding_status AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM knowledge_documents
GROUP BY embedding_status

ORDER BY layer, status;

-- ---------------------------------------------------------------------------
-- View: documents_needing_review — for human review queue
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW documents_needing_review AS
SELECT
    kd.id,
    kd.title,
    kd.document_type,
    kd.confidence_score,
    kd.source_url,
    kd.source_domain,
    p.payer_name,
    kd.created_at,
    kd.tagger_metadata->>'raw_scores' AS raw_scores
FROM knowledge_documents kd
LEFT JOIN payers p ON p.id = kd.payer_id
WHERE kd.requires_review = TRUE
  AND kd.status = 'needs_review'
ORDER BY kd.confidence_score ASC, kd.created_at DESC;

COMMIT;

-- =============================================================================
-- Verify migration
-- =============================================================================
DO $$
DECLARE
    v_raw_count INT;
    v_knowledge_count INT;
    v_embedding_count INT;
BEGIN
    SELECT COUNT(*) INTO v_raw_count FROM raw_documents;
    SELECT COUNT(*) INTO v_knowledge_count FROM knowledge_documents;
    SELECT COUNT(*) INTO v_embedding_count FROM document_embeddings;

    RAISE NOTICE 'Migration 002 complete.';
    RAISE NOTICE '  raw_documents: % rows', v_raw_count;
    RAISE NOTICE '  knowledge_documents: % rows', v_knowledge_count;
    RAISE NOTICE '  document_embeddings: % rows', v_embedding_count;
END $$;
