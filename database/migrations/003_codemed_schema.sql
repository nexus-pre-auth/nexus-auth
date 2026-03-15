-- =============================================================================
-- CodeMed AI — Migration 003: CodeMed Schema
-- =============================================================================
-- Adds tables for:
--   - HCC V28 crosswalk and hierarchy enforcement audit trail
--   - MEAT evidence extraction results
--   - Natural language coding query history
--   - Prior authorization appeal letters
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- HCC codes: CMS-HCC V28 model code registry
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hcc_codes (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hcc_number          INTEGER NOT NULL,
    hcc_description     TEXT NOT NULL,
    hierarchy_group     VARCHAR(50),        -- e.g. 'DIABETES', 'CKD', 'HEART_FAILURE'
    hierarchy_rank      INTEGER,            -- 1 = highest severity in group
    raf_weight          NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
    model_version       VARCHAR(10) NOT NULL DEFAULT 'V28',
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (hcc_number, model_version)
);

CREATE INDEX IF NOT EXISTS idx_hcc_codes_number
    ON hcc_codes (hcc_number);
CREATE INDEX IF NOT EXISTS idx_hcc_codes_group
    ON hcc_codes (hierarchy_group)
    WHERE hierarchy_group IS NOT NULL;

-- ---------------------------------------------------------------------------
-- ICD-10 to HCC crosswalk: maps ICD-10 codes to HCC numbers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS icd10_hcc_crosswalk (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    icd10_code          VARCHAR(10) NOT NULL,
    hcc_number          INTEGER NOT NULL,
    model_version       VARCHAR(10) NOT NULL DEFAULT 'V28',
    payment_year        INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (icd10_code, model_version)
);

CREATE INDEX IF NOT EXISTS idx_crosswalk_icd10
    ON icd10_hcc_crosswalk (icd10_code);
CREATE INDEX IF NOT EXISTS idx_crosswalk_hcc
    ON icd10_hcc_crosswalk (hcc_number);

-- ---------------------------------------------------------------------------
-- HCC enforcement audit log: records hierarchy enforcement decisions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hcc_enforcement_log (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Input context
    encounter_id        VARCHAR(100),       -- External encounter/claim ID
    patient_id          VARCHAR(100),
    provider_npi        VARCHAR(20),
    service_date        DATE,

    -- Input codes
    input_icd10_codes   TEXT[] NOT NULL,

    -- Results
    active_hccs         JSONB,              -- List of active HCC objects
    suppressed_hccs     JSONB,              -- List of trumped HCC objects
    hierarchy_conflicts JSONB,              -- Conflict details
    unmapped_codes      TEXT[],

    -- RAF scores
    raf_before          NUMERIC(8, 4),
    raf_after           NUMERIC(8, 4),
    raf_delta           NUMERIC(8, 4),

    -- Metadata
    model_version       VARCHAR(10) DEFAULT 'V28',
    processed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hcc_log_encounter
    ON hcc_enforcement_log (encounter_id)
    WHERE encounter_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_hcc_log_patient
    ON hcc_enforcement_log (patient_id)
    WHERE patient_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_hcc_log_processed_at
    ON hcc_enforcement_log (processed_at DESC);

-- ---------------------------------------------------------------------------
-- MEAT extraction results: clinical note evidence audit trail
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meat_extraction_results (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Source context
    encounter_id        VARCHAR(100),
    patient_id          VARCHAR(100),
    provider_npi        VARCHAR(20),
    service_date        DATE,

    -- Input
    note_length         INTEGER,
    sentence_count      INTEGER,
    icd10_codes         TEXT[],

    -- Results
    diagnoses_supported INTEGER DEFAULT 0,
    diagnoses_total     INTEGER DEFAULT 0,
    overall_defensibility_score  NUMERIC(5, 2),
    unsupported_diagnoses        TEXT[],
    diagnosis_results   JSONB,              -- Full per-diagnosis MEAT results

    -- Metadata
    processed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meat_encounter
    ON meat_extraction_results (encounter_id)
    WHERE encounter_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_meat_defensibility
    ON meat_extraction_results (overall_defensibility_score DESC);
CREATE INDEX IF NOT EXISTS idx_meat_processed_at
    ON meat_extraction_results (processed_at DESC);

-- ---------------------------------------------------------------------------
-- NLQ query history: log of natural language coding queries
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nlq_queries (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_text          TEXT NOT NULL,
    query_mode          VARCHAR(20) DEFAULT 'keyword',   -- 'keyword' | 'semantic'
    code_types          TEXT[],                          -- ['ICD-10', 'CPT', 'HCPCS']
    result_count        INTEGER DEFAULT 0,
    top_results         JSONB,                           -- Top 5 results
    user_id             VARCHAR(100),
    session_id          VARCHAR(100),
    latency_ms          INTEGER,
    queried_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nlq_queried_at
    ON nlq_queries (queried_at DESC);
CREATE INDEX IF NOT EXISTS idx_nlq_user
    ON nlq_queries (user_id)
    WHERE user_id IS NOT NULL;

-- Full-text search on query text
CREATE INDEX IF NOT EXISTS idx_nlq_query_fts
    ON nlq_queries USING gin(to_tsvector('english', query_text));

-- ---------------------------------------------------------------------------
-- Appeal letters: generated prior auth appeal letter records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS appeal_letters (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Patient / claim context
    patient_name        VARCHAR(255),
    patient_dob         DATE,
    patient_id          VARCHAR(100),
    insurance_member_id VARCHAR(100),
    claim_number        VARCHAR(100),
    payer_name          VARCHAR(255),
    service_date        DATE,
    denial_date         DATE,

    -- Provider context
    provider_name       VARCHAR(255),
    provider_npi        VARCHAR(20),

    -- Denied codes
    denied_cpt_codes    TEXT[],
    diagnosis_codes     TEXT[],
    denial_reason       TEXT,

    -- Generated letter
    letter_text         TEXT NOT NULL,
    word_count          INTEGER,
    policy_citations    JSONB,              -- List of {policy_id, title, url}
    regulatory_citations TEXT[],

    -- Status tracking
    status              VARCHAR(20) DEFAULT 'draft'
                        CHECK (status IN ('draft', 'submitted', 'approved', 'denied', 'archived')),
    submitted_at        TIMESTAMPTZ,
    outcome_date        DATE,
    outcome_notes       TEXT,

    -- Metadata
    generated_at        TIMESTAMPTZ DEFAULT NOW(),
    generated_by        VARCHAR(100) DEFAULT 'codemed_ai'
);

CREATE INDEX IF NOT EXISTS idx_appeals_claim
    ON appeal_letters (claim_number)
    WHERE claim_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_appeals_patient
    ON appeal_letters (patient_id)
    WHERE patient_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_appeals_payer
    ON appeal_letters (payer_name);
CREATE INDEX IF NOT EXISTS idx_appeals_status
    ON appeal_letters (status);
CREATE INDEX IF NOT EXISTS idx_appeals_generated_at
    ON appeal_letters (generated_at DESC);

-- ---------------------------------------------------------------------------
-- View: codemed_performance_metrics — aggregate performance dashboard
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW codemed_performance_metrics AS
SELECT
    -- HCC metrics (last 30 days)
    (SELECT COUNT(*) FROM hcc_enforcement_log WHERE processed_at > NOW() - INTERVAL '30 days')
        AS hcc_enforcements_30d,
    (SELECT ROUND(AVG(raf_delta), 4) FROM hcc_enforcement_log WHERE processed_at > NOW() - INTERVAL '30 days')
        AS avg_raf_delta_30d,
    (SELECT COUNT(*) FROM hcc_enforcement_log WHERE processed_at > NOW() - INTERVAL '30 days'
        AND jsonb_array_length(hierarchy_conflicts) > 0)
        AS hcc_conflicts_detected_30d,

    -- MEAT metrics (last 30 days)
    (SELECT COUNT(*) FROM meat_extraction_results WHERE processed_at > NOW() - INTERVAL '30 days')
        AS meat_extractions_30d,
    (SELECT ROUND(AVG(overall_defensibility_score), 1) FROM meat_extraction_results
        WHERE processed_at > NOW() - INTERVAL '30 days')
        AS avg_defensibility_score_30d,

    -- NLQ metrics (last 30 days)
    (SELECT COUNT(*) FROM nlq_queries WHERE queried_at > NOW() - INTERVAL '30 days')
        AS nlq_queries_30d,
    (SELECT ROUND(AVG(latency_ms), 0) FROM nlq_queries WHERE queried_at > NOW() - INTERVAL '30 days')
        AS avg_nlq_latency_ms_30d,

    -- Appeals metrics (all time)
    (SELECT COUNT(*) FROM appeal_letters)                           AS total_appeals_generated,
    (SELECT COUNT(*) FROM appeal_letters WHERE status = 'approved') AS appeals_approved,
    (SELECT COUNT(*) FROM appeal_letters WHERE status = 'denied')   AS appeals_denied,
    (SELECT ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'approved')
        / NULLIF(COUNT(*) FILTER (WHERE status IN ('approved', 'denied')), 0),
        1
    ) FROM appeal_letters) AS appeal_success_rate_pct;

-- ---------------------------------------------------------------------------
-- Seed: insert representative V28 HCC codes
-- ---------------------------------------------------------------------------
INSERT INTO hcc_codes (hcc_number, hcc_description, hierarchy_group, hierarchy_rank, raf_weight, model_version)
VALUES
    -- Diabetes
    (17,  'Diabetes with Acute Complications',           'DIABETES',     1, 0.302, 'V28'),
    (18,  'Diabetes with Chronic Complications',         'DIABETES',     2, 0.179, 'V28'),
    (19,  'Diabetes without Complications',              'DIABETES',     3, 0.118, 'V28'),
    -- CKD
    (329, 'End Stage Renal Disease',                     'CKD',          1, 0.493, 'V28'),
    (330, 'Chronic Kidney Disease Stage 5',              'CKD',          2, 0.289, 'V28'),
    (331, 'Chronic Kidney Disease Stage 4',              'CKD',          3, 0.200, 'V28'),
    (332, 'Chronic Kidney Disease Stage 3b',             'CKD',          4, 0.137, 'V28'),
    (333, 'Chronic Kidney Disease Stage 3a',             'CKD',          5, 0.074, 'V28'),
    (334, 'Chronic Kidney Disease Stages 1-2',           'CKD',          6, 0.000, 'V28'),
    -- Heart Failure
    (224, 'Acute Heart Failure',                         'HEART_FAILURE',1, 0.368, 'V28'),
    (225, 'Chronic Heart Failure',                       'HEART_FAILURE',2, 0.259, 'V28'),
    (226, 'Heart Failure, unspecified',                  'HEART_FAILURE',3, 0.201, 'V28'),
    -- COPD
    (280, 'COPD with Acute Exacerbation',                'COPD',         1, 0.346, 'V28'),
    (281, 'COPD with Acute Lower Respiratory Infection', 'COPD',         2, 0.259, 'V28'),
    (282, 'COPD, unspecified',                           'COPD',         3, 0.193, 'V28'),
    -- Liver
    (27,  'End Stage Liver Disease',                     'LIVER',        1, 0.487, 'V28'),
    (28,  'Cirrhosis of Liver',                          'LIVER',        2, 0.352, 'V28'),
    (29,  'Chronic Liver Disease',                       'LIVER',        3, 0.221, 'V28'),
    -- Dementia
    (52,  'Dementia with Behavioral Disturbances',       'DEMENTIA',     1, 0.346, 'V28'),
    (53,  'Dementia without Behavioral Disturbances',    'DEMENTIA',     2, 0.211, 'V28'),
    (54,  'Mild Cognitive Impairment',                   'DEMENTIA',     3, 0.138, 'V28'),
    -- Stroke
    (167, 'Stroke',                                      'STROKE',       1, 0.353, 'V28'),
    (168, 'Sequelae of Stroke',                          'STROKE',       2, 0.217, 'V28')
ON CONFLICT (hcc_number, model_version) DO NOTHING;

COMMIT;

-- =============================================================================
-- Verify migration 003
-- =============================================================================
DO $$
DECLARE
    v_hcc_count INT;
    v_enforcement_count INT;
    v_appeal_count INT;
BEGIN
    SELECT COUNT(*) INTO v_hcc_count FROM hcc_codes;
    SELECT COUNT(*) INTO v_enforcement_count FROM hcc_enforcement_log;
    SELECT COUNT(*) INTO v_appeal_count FROM appeal_letters;

    RAISE NOTICE 'Migration 003 (CodeMed) complete.';
    RAISE NOTICE '  hcc_codes seeded: % rows', v_hcc_count;
    RAISE NOTICE '  hcc_enforcement_log: % rows', v_enforcement_count;
    RAISE NOTICE '  appeal_letters: % rows', v_appeal_count;
END $$;
