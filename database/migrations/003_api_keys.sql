-- =============================================================================
-- NexusAuth — Migration 003: API Keys & Tenants
-- =============================================================================
-- Adds multi-tenant API key authentication for the REST API.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- TENANTS — one row per customer / organisation
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenants (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) UNIQUE NOT NULL,
    plan_tier       VARCHAR(20) NOT NULL DEFAULT 'free'
                        CHECK (plan_tier IN ('free', 'pro', 'enterprise')),
    monthly_limit   INTEGER NOT NULL DEFAULT 100,   -- API calls per month
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER set_updated_at_tenants
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Seed a default dev tenant
INSERT INTO tenants (name, plan_tier, monthly_limit)
VALUES ('dev', 'enterprise', 999999)
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- API KEYS — hashed keys bound to a tenant
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_keys (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_prefix      VARCHAR(8) NOT NULL,            -- first 8 chars for display (e.g. nxa_xxxx)
    key_hash        VARCHAR(64) NOT NULL UNIQUE,    -- SHA-256 of the full key
    name            VARCHAR(100),                   -- human label, e.g. "production"
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,                    -- NULL = never expires
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash      ON api_keys (key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant    ON api_keys (tenant_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_prefix    ON api_keys (key_prefix);

-- ---------------------------------------------------------------------------
-- API USAGE — per-call tracking for billing / rate limiting
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_usage (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    api_key_id      UUID REFERENCES api_keys(id) ON DELETE SET NULL,
    endpoint        VARCHAR(100) NOT NULL,
    method          VARCHAR(10) NOT NULL DEFAULT 'POST',
    status_code     INTEGER,
    response_ms     INTEGER,
    called_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_usage_tenant   ON api_usage (tenant_id, called_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_usage_endpoint ON api_usage (endpoint, called_at DESC);

COMMIT;
