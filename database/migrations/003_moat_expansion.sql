-- =============================================================================
-- NexusAuth — Migration 003: Moat Expansion
-- =============================================================================
-- Adds schema support for 9 new document types, 4 new payers, structured
-- NCCI/MPFS/DRG/ASP tables, and updated routing rules.
--
-- Safe to run multiple times (all DDL uses IF NOT EXISTS / ON CONFLICT).
-- Append-only — does not modify migrations 001 or 002.
-- =============================================================================

BEGIN;

-- =============================================================================
-- SECTION 1: New document_type_enum values
-- PostgreSQL requires each ALTER TYPE ... ADD VALUE to run outside a
-- transaction, BUT we use the DO block pattern to skip if already present.
-- =============================================================================

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'ncci_edit'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'ncci_edit';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'remittance_policy'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'remittance_policy';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'modifier_policy'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'modifier_policy';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'drg_policy'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'drg_policy';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'opps_apc'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'opps_apc';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'asp_drug_pricing'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'asp_drug_pricing';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'icd10_guidelines'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'icd10_guidelines';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'medical_necessity_policy'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'medical_necessity_policy';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel = 'transmittal'
  ) THEN
    ALTER TYPE document_type_enum ADD VALUE 'transmittal';
  END IF;
END $$;

COMMIT;

-- Note: PostgreSQL requires ADD VALUE to commit before the enum values can be
-- used in column constraints. The rest of the migration runs in a fresh block.

BEGIN;

-- =============================================================================
-- SECTION 2: knowledge_documents — data versioning columns
-- =============================================================================

-- Track which CMS data year/quarter a document belongs to
-- (especially important for fee schedules, NCCI, ASP — all updated annually/quarterly)
ALTER TABLE knowledge_documents
    ADD COLUMN IF NOT EXISTS data_year SMALLINT;

ALTER TABLE knowledge_documents
    ADD COLUMN IF NOT EXISTS data_quarter SMALLINT
        CHECK (data_quarter IS NULL OR data_quarter BETWEEN 1 AND 4);

-- Index for querying by data period
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_data_year
    ON knowledge_documents (data_year)
    WHERE data_year IS NOT NULL;


-- =============================================================================
-- SECTION 3: NCCI Edits table — structured PTP and MUE data
-- =============================================================================
-- Enables O(1) code-pair lookups without full-text search.
-- Each row = one NCCI edit entry extracted from a knowledge_document.

CREATE TABLE IF NOT EXISTS ncci_edits (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    knowledge_document_id       UUID REFERENCES knowledge_documents(id) ON DELETE CASCADE,

    -- Edit identification
    edit_type                   VARCHAR(5) NOT NULL,    -- 'PTP' or 'MUE'
    column1_code                VARCHAR(10),            -- PTP: Column 1 (comprehensive) code
    column2_code                VARCHAR(10),            -- PTP: Column 2 (component) code
    modifier_indicator          CHAR(1),                -- '0'=not allowed, '1'=allowed with mod, '9'=N/A

    -- MUE-specific
    mue_value                   SMALLINT,               -- max units per claim/DOS
    mue_adjudication_indicator  VARCHAR(3),             -- '1'=claim, '2'=line, '3'=date of service

    -- Temporal data
    effective_date              DATE,
    deletion_date               DATE,
    data_year                   SMALLINT NOT NULL,
    data_quarter                SMALLINT NOT NULL CHECK (data_quarter BETWEEN 1 AND 4),

    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (column1_code, column2_code, edit_type, data_year, data_quarter)
);

CREATE INDEX IF NOT EXISTS idx_ncci_col1
    ON ncci_edits (column1_code)
    WHERE column1_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ncci_col2
    ON ncci_edits (column2_code)
    WHERE column2_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ncci_pair
    ON ncci_edits (column1_code, column2_code)
    WHERE column1_code IS NOT NULL AND column2_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ncci_type_year
    ON ncci_edits (edit_type, data_year, data_quarter);


-- =============================================================================
-- SECTION 4: MPFS Rates table — structured Medicare physician fee schedule
-- =============================================================================
-- Enables direct per-code rate lookup for CODEMED payment verification.

CREATE TABLE IF NOT EXISTS mpfs_rates (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    knowledge_document_id       UUID REFERENCES knowledge_documents(id) ON DELETE CASCADE,

    hcpcs_code                  VARCHAR(10) NOT NULL,
    modifier                    VARCHAR(5),
    description                 TEXT,

    -- RVU components
    work_rvu                    NUMERIC(8, 4),
    non_fac_pe_rvu              NUMERIC(8, 4),
    fac_pe_rvu                  NUMERIC(8, 4),
    mp_rvu                      NUMERIC(8, 4),
    non_fac_total_rvu           NUMERIC(8, 4),
    fac_total_rvu               NUMERIC(8, 4),

    -- National unadjusted payment rates
    non_fac_payment             NUMERIC(10, 2),
    fac_payment                 NUMERIC(10, 2),

    -- Context
    global_period               VARCHAR(10),    -- '000', '010', '090', 'MMM', 'YYY', 'ZZZ'
    status_code                 VARCHAR(5),     -- 'A'=active, 'I'=inactive, etc.
    payment_year                SMALLINT NOT NULL,

    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (hcpcs_code, modifier, payment_year)
);

CREATE INDEX IF NOT EXISTS idx_mpfs_hcpcs
    ON mpfs_rates (hcpcs_code);

CREATE INDEX IF NOT EXISTS idx_mpfs_year
    ON mpfs_rates (payment_year);


-- =============================================================================
-- SECTION 5: MS-DRG Rates table — structured DRG weight data
-- =============================================================================

CREATE TABLE IF NOT EXISTS drg_rates (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    knowledge_document_id       UUID REFERENCES knowledge_documents(id) ON DELETE CASCADE,

    drg_number                  VARCHAR(5) NOT NULL,
    drg_title                   TEXT,
    mdc                         VARCHAR(5),
    drg_type                    VARCHAR(10),    -- 'MEDSURG', 'SURGICAL', 'MEDICAL'

    relative_weight             NUMERIC(8, 4),
    geometric_mean_los          NUMERIC(6, 2),
    arithmetic_mean_los         NUMERIC(6, 2),

    fiscal_year                 SMALLINT NOT NULL,

    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (drg_number, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_drg_number
    ON drg_rates (drg_number);

CREATE INDEX IF NOT EXISTS idx_drg_fiscal_year
    ON drg_rates (fiscal_year);

CREATE INDEX IF NOT EXISTS idx_drg_mdc
    ON drg_rates (mdc)
    WHERE mdc IS NOT NULL;


-- =============================================================================
-- SECTION 6: ASP Drug Pricing table — structured quarterly drug rates
-- =============================================================================

CREATE TABLE IF NOT EXISTS asp_drug_rates (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    knowledge_document_id       UUID REFERENCES knowledge_documents(id) ON DELETE CASCADE,

    hcpcs_code                  VARCHAR(10) NOT NULL,
    short_description           TEXT,
    dosage_form                 VARCHAR(200),

    asp_price                   NUMERIC(12, 4),      -- average sales price per unit
    asp_plus_6_payment          NUMERIC(12, 4),      -- ASP + 6% Medicare payment limit

    effective_year              SMALLINT NOT NULL,
    effective_quarter           SMALLINT NOT NULL CHECK (effective_quarter BETWEEN 1 AND 4),

    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (hcpcs_code, effective_year, effective_quarter)
);

CREATE INDEX IF NOT EXISTS idx_asp_hcpcs
    ON asp_drug_rates (hcpcs_code);

CREATE INDEX IF NOT EXISTS idx_asp_period
    ON asp_drug_rates (effective_year, effective_quarter);


-- =============================================================================
-- SECTION 7: New payers — CIGNA, HUMANA, CENTENE, MAGELLAN
-- =============================================================================

INSERT INTO payers (payer_code, payer_name, payer_type, domains) VALUES
    ('CIGNA',
     'Cigna Corporation',
     'commercial',
     ARRAY['cigna.com', 'evernorth.com', 'cignahealthspring.com'])
ON CONFLICT (payer_code) DO NOTHING;

INSERT INTO payers (payer_code, payer_name, payer_type, domains) VALUES
    ('HUMANA',
     'Humana Inc.',
     'commercial',
     ARRAY['humana.com', 'humanaone.com', 'humanamilitary.com'])
ON CONFLICT (payer_code) DO NOTHING;

INSERT INTO payers (payer_code, payer_name, payer_type, domains) VALUES
    ('CENTENE',
     'Centene Corporation',
     'commercial',
     ARRAY['centene.com', 'wellcare.com', 'meridianhealth.com', 'healthnet.com'])
ON CONFLICT (payer_code) DO NOTHING;

INSERT INTO payers (payer_code, payer_name, payer_type, domains) VALUES
    ('MAGELLAN',
     'Magellan Health',
     'commercial',
     ARRAY['magellanhealth.com', 'magellanrx.com'])
ON CONFLICT (payer_code) DO NOTHING;


-- =============================================================================
-- SECTION 8: New routing rules (mirrors taxonomy.yaml routing_matrix)
-- =============================================================================

-- CODEMED routes
INSERT INTO routing_rules (rule_name, document_type, target_tool, priority)
VALUES
    ('ncci_edit_to_codemed',            'ncci_edit',                'CODEMED',    10),
    ('remittance_policy_to_codemed',    'remittance_policy',        'CODEMED',    10),
    ('modifier_policy_to_codemed',      'modifier_policy',          'CODEMED',    10),
    ('drg_policy_to_codemed',           'drg_policy',               'CODEMED',    10),
    ('opps_apc_to_codemed',             'opps_apc',                 'CODEMED',    10),
    ('asp_drug_pricing_to_codemed',     'asp_drug_pricing',         'CODEMED',    10)
ON CONFLICT (rule_name) DO NOTHING;

-- NexusAuth routes
INSERT INTO routing_rules (rule_name, document_type, target_tool, priority)
VALUES
    ('icd10_guidelines_to_nexusauth',   'icd10_guidelines',         'NexusAuth',  20),
    ('med_necessity_to_nexusauth',      'medical_necessity_policy', 'NexusAuth',  10),
    ('transmittal_to_nexusauth',        'transmittal',              'NexusAuth',  30)
ON CONFLICT (rule_name) DO NOTHING;


-- =============================================================================
-- SECTION 9: Operational views
-- =============================================================================

-- CODEMED dashboard: all billing/payment documents
CREATE OR REPLACE VIEW codemed_documents AS
SELECT
    kd.id,
    kd.title,
    kd.document_type,
    kd.document_subtype,
    kd.source_url,
    kd.source_domain,
    p.payer_code,
    kd.confidence_score,
    kd.data_year,
    kd.data_quarter,
    kd.embedding_status,
    kd.created_at
FROM knowledge_documents kd
LEFT JOIN payers p ON p.id = kd.payer_id
WHERE 'CODEMED' = ANY(kd.routing_targets)
  AND kd.is_active = TRUE
ORDER BY kd.document_type, kd.data_year DESC NULLS LAST, kd.created_at DESC;

-- NexusAuth dashboard: all clinical coverage documents
CREATE OR REPLACE VIEW nexusauth_documents AS
SELECT
    kd.id,
    kd.title,
    kd.document_type,
    kd.document_subtype,
    kd.source_url,
    kd.source_domain,
    p.payer_code,
    kd.confidence_score,
    kd.cpt_codes,
    kd.icd10_codes,
    kd.specialties,
    kd.effective_date,
    kd.embedding_status,
    kd.created_at
FROM knowledge_documents kd
LEFT JOIN payers p ON p.id = kd.payer_id
WHERE 'NexusAuth' = ANY(kd.routing_targets)
  AND kd.is_active = TRUE
ORDER BY kd.document_type, kd.confidence_score DESC, kd.created_at DESC;

-- Fee schedule lookup view: latest rates by HCPCS code
CREATE OR REPLACE VIEW current_mpfs_rates AS
SELECT DISTINCT ON (hcpcs_code, modifier)
    hcpcs_code,
    modifier,
    description,
    work_rvu,
    non_fac_total_rvu,
    fac_total_rvu,
    non_fac_payment,
    fac_payment,
    global_period,
    payment_year
FROM mpfs_rates
ORDER BY hcpcs_code, modifier, payment_year DESC;

-- DRG weight lookup view: latest weights
CREATE OR REPLACE VIEW current_drg_rates AS
SELECT DISTINCT ON (drg_number)
    drg_number,
    drg_title,
    mdc,
    drg_type,
    relative_weight,
    geometric_mean_los,
    arithmetic_mean_los,
    fiscal_year
FROM drg_rates
ORDER BY drg_number, fiscal_year DESC;

-- ASP drug pricing lookup view: latest quarter rates
CREATE OR REPLACE VIEW current_asp_rates AS
SELECT DISTINCT ON (hcpcs_code)
    hcpcs_code,
    short_description,
    dosage_form,
    asp_price,
    asp_plus_6_payment,
    effective_year,
    effective_quarter
FROM asp_drug_rates
ORDER BY hcpcs_code, effective_year DESC, effective_quarter DESC;


-- =============================================================================
-- SECTION 10: Extended pipeline_status view (replaces migration 002 version)
-- =============================================================================

CREATE OR REPLACE VIEW pipeline_status AS
SELECT
    'raw_documents'::TEXT AS layer,
    processing_status::TEXT AS status,
    COUNT(*) AS count,
    MAX(scraped_at) AS latest_activity
FROM raw_documents
GROUP BY processing_status

UNION ALL

SELECT
    'knowledge_documents'::TEXT AS layer,
    embedding_status::TEXT AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM knowledge_documents
GROUP BY embedding_status

UNION ALL

SELECT
    'knowledge_documents_by_type'::TEXT AS layer,
    document_type::TEXT AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM knowledge_documents
GROUP BY document_type

UNION ALL

SELECT
    'ncci_edits'::TEXT AS layer,
    edit_type AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM ncci_edits
GROUP BY edit_type

UNION ALL

SELECT
    'mpfs_rates'::TEXT AS layer,
    payment_year::TEXT AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM mpfs_rates
GROUP BY payment_year

UNION ALL

SELECT
    'drg_rates'::TEXT AS layer,
    fiscal_year::TEXT AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM drg_rates
GROUP BY fiscal_year

UNION ALL

SELECT
    'asp_drug_rates'::TEXT AS layer,
    CONCAT(effective_year, ' Q', effective_quarter) AS status,
    COUNT(*) AS count,
    MAX(created_at) AS latest_activity
FROM asp_drug_rates
GROUP BY effective_year, effective_quarter

ORDER BY layer, status;

COMMIT;

-- =============================================================================
-- Verify migration 003
-- =============================================================================
DO $$
DECLARE
    v_new_types    INT;
    v_new_payers   INT;
    v_new_rules    INT;
BEGIN
    SELECT COUNT(*) INTO v_new_types
    FROM pg_enum
    WHERE enumtypid = 'document_type_enum'::regtype
      AND enumlabel IN (
        'ncci_edit', 'remittance_policy', 'modifier_policy', 'drg_policy',
        'opps_apc', 'asp_drug_pricing', 'icd10_guidelines',
        'medical_necessity_policy', 'transmittal'
      );

    SELECT COUNT(*) INTO v_new_payers
    FROM payers
    WHERE payer_code IN ('CIGNA', 'HUMANA', 'CENTENE', 'MAGELLAN');

    SELECT COUNT(*) INTO v_new_rules
    FROM routing_rules
    WHERE rule_name IN (
        'ncci_edit_to_codemed', 'remittance_policy_to_codemed',
        'modifier_policy_to_codemed', 'drg_policy_to_codemed',
        'opps_apc_to_codemed', 'asp_drug_pricing_to_codemed',
        'icd10_guidelines_to_nexusauth', 'med_necessity_to_nexusauth',
        'transmittal_to_nexusauth'
    );

    RAISE NOTICE 'Migration 003 complete.';
    RAISE NOTICE '  New document type enum values: %/9', v_new_types;
    RAISE NOTICE '  New payers seeded:             %/4', v_new_payers;
    RAISE NOTICE '  New routing rules:             %/9', v_new_rules;
    RAISE NOTICE '  New tables: ncci_edits, mpfs_rates, drg_rates, asp_drug_rates';
    RAISE NOTICE '  New views: codemed_documents, nexusauth_documents,';
    RAISE NOTICE '             current_mpfs_rates, current_drg_rates, current_asp_rates';
END $$;
