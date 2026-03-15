-- ============================================================
-- WebPT OAuth Integration Schema
-- Migration: 003_webpt_oauth.sql
-- Supports the full Connect-WebPT flow (OAuth → sync → patterns)
-- ============================================================

-- ============================================================
-- WEBPT CONNECTIONS — One row per clinic authorization
-- ============================================================

CREATE TYPE webpt_connection_status_enum AS ENUM (
    'pending',
    'connected',
    'syncing',
    'ready',
    'error',
    'disconnected'
);

CREATE TABLE webpt_connections (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clinic_id           VARCHAR(255) NOT NULL,
    status              webpt_connection_status_enum DEFAULT 'pending',
    error_message       TEXT,

    -- OAuth state
    oauth_state         VARCHAR(255),               -- CSRF nonce for OAuth flow
    webpt_practice_id   VARCHAR(255),               -- Returned by WebPT after auth

    -- Sync progress
    sync_queued_at      TIMESTAMPTZ,
    sync_started_at     TIMESTAMPTZ,
    sync_completed_at   TIMESTAMPTZ,
    claims_synced       INTEGER DEFAULT 0,

    -- Webhook
    webhook_id          VARCHAR(255),               -- WebPT webhook ID
    webhook_registered  BOOLEAN DEFAULT FALSE,

    -- Timestamps
    connected_at        TIMESTAMPTZ,
    disconnected_at     TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_webpt_connections_clinic ON webpt_connections (clinic_id)
    WHERE status != 'disconnected';
CREATE INDEX idx_webpt_connections_status ON webpt_connections (status);
CREATE INDEX idx_webpt_connections_state  ON webpt_connections (oauth_state)
    WHERE oauth_state IS NOT NULL;

CREATE TRIGGER set_updated_at_webpt_connections
    BEFORE UPDATE ON webpt_connections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- WEBPT TOKENS — Encrypted OAuth tokens per connection
-- ============================================================

CREATE TABLE webpt_tokens (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id       UUID NOT NULL REFERENCES webpt_connections(id) ON DELETE CASCADE,
    access_token_enc    TEXT NOT NULL,              -- Fernet-encrypted
    refresh_token_enc   TEXT,                       -- Fernet-encrypted
    token_type          VARCHAR(50) DEFAULT 'Bearer',
    expires_at          TIMESTAMPTZ,
    scope               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_webpt_tokens_connection ON webpt_tokens (connection_id);

CREATE TRIGGER set_updated_at_webpt_tokens
    BEFORE UPDATE ON webpt_tokens
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- WEBPT CLAIMS — Raw claims synced from WebPT
-- ============================================================

CREATE TABLE webpt_claims (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id       UUID NOT NULL REFERENCES webpt_connections(id) ON DELETE CASCADE,
    webpt_claim_id      VARCHAR(255) NOT NULL,
    patient_id          VARCHAR(255),
    provider_id         VARCHAR(255),
    service_date        DATE,
    cpt_codes           TEXT[] DEFAULT '{}',
    icd10_codes         TEXT[] DEFAULT '{}',
    claim_status        VARCHAR(50),
    amount              DECIMAL(12, 2),
    raw_payload         JSONB DEFAULT '{}',
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (connection_id, webpt_claim_id)
);

CREATE INDEX idx_webpt_claims_connection  ON webpt_claims (connection_id);
CREATE INDEX idx_webpt_claims_service_date ON webpt_claims (service_date DESC);
CREATE INDEX idx_webpt_claims_cpt         ON webpt_claims USING GIN (cpt_codes);
CREATE INDEX idx_webpt_claims_icd10       ON webpt_claims USING GIN (icd10_codes);

-- ============================================================
-- INTELLIGENCE PATTERNS — Pattern detection results
-- ============================================================

CREATE TYPE pattern_type_enum AS ENUM (
    'high_denial_rate',
    'missing_auth',
    'code_mismatch',
    'duplicate_billing',
    'frequency_anomaly',
    'documentation_gap'
);

CREATE TABLE intelligence_patterns (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id    UUID NOT NULL REFERENCES webpt_connections(id) ON DELETE CASCADE,
    pattern_type     pattern_type_enum NOT NULL,
    description      TEXT NOT NULL,
    affected_codes   TEXT[] DEFAULT '{}',
    occurrence_count INTEGER DEFAULT 0,
    confidence       FLOAT CHECK (confidence BETWEEN 0.0 AND 1.0),
    severity         VARCHAR(20) DEFAULT 'medium',   -- low | medium | high
    detected_at      TIMESTAMPTZ DEFAULT NOW(),
    metadata         JSONB DEFAULT '{}'
);

CREATE INDEX idx_intelligence_patterns_connection ON intelligence_patterns (connection_id);
CREATE INDEX idx_intelligence_patterns_type       ON intelligence_patterns (pattern_type);
CREATE INDEX idx_intelligence_patterns_severity   ON intelligence_patterns (severity);
