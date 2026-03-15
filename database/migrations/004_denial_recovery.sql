-- ============================================================
-- Denial Recovery & Revenue Tracking Schema
-- Migration: 004_denial_recovery.sql
-- CO-16 / CO-50 / CO-97 recovery business layer
-- ============================================================

CREATE TYPE denial_code_enum AS ENUM ('CO-16', 'CO-50', 'CO-97');

CREATE TYPE denial_status_enum AS ENUM (
    'detected',     -- pattern found, not yet worked
    'fixing',       -- auto-fix in progress
    'fixed',        -- fix applied, ready to resubmit
    'submitted',    -- resubmitted to payer
    'paid',         -- payer paid — revenue realized
    'rejected',     -- fix attempt failed
    'skipped'       -- below value threshold or unsupported
);

-- ============================================================
-- RECOVERABLE DENIALS — one row per actionable denial
-- ============================================================

CREATE TABLE recoverable_denials (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id       UUID NOT NULL REFERENCES webpt_connections(id) ON DELETE CASCADE,
    claim_id            UUID REFERENCES webpt_claims(id) ON DELETE SET NULL,
    webpt_claim_id      VARCHAR(255) NOT NULL,
    clinic_id           VARCHAR(255) NOT NULL,

    -- Denial info
    denial_code         denial_code_enum NOT NULL,
    billed_amount       DECIMAL(12, 2) NOT NULL DEFAULT 0,
    estimated_recovery  DECIMAL(12, 2),
    success_probability FLOAT CHECK (success_probability BETWEEN 0 AND 1),

    -- Fix applied
    fixes_applied       JSONB DEFAULT '[]',
    fix_notes           TEXT,

    -- Status lifecycle
    status              denial_status_enum DEFAULT 'detected',
    detected_at         TIMESTAMPTZ DEFAULT NOW(),
    fixed_at            TIMESTAMPTZ,
    submitted_at        TIMESTAMPTZ,
    paid_at             TIMESTAMPTZ,

    -- Revenue share (populated when paid)
    paid_amount         DECIMAL(12, 2),
    your_fee            DECIMAL(12, 2),   -- 20 % of paid_amount
    clinic_net          DECIMAL(12, 2),   -- 80 % of paid_amount

    UNIQUE (connection_id, webpt_claim_id, denial_code)
);

CREATE INDEX idx_rec_denials_clinic  ON recoverable_denials (clinic_id, status);
CREATE INDEX idx_rec_denials_code    ON recoverable_denials (denial_code);
CREATE INDEX idx_rec_denials_status  ON recoverable_denials (status);
CREATE INDEX idx_rec_denials_conn    ON recoverable_denials (connection_id);

-- ============================================================
-- REVENUE SHARES — monthly invoicing per clinic
-- ============================================================

CREATE TABLE revenue_shares (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clinic_id       VARCHAR(255) NOT NULL,
    period_month    DATE NOT NULL,          -- first day of the month
    total_recovered DECIMAL(12, 2) DEFAULT 0,
    your_fee        DECIMAL(12, 2) DEFAULT 0,
    clinic_payout   DECIMAL(12, 2) DEFAULT 0,
    denial_count    INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending',  -- pending | invoiced | paid
    invoiced_at     TIMESTAMPTZ,
    paid_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (clinic_id, period_month)
);

CREATE INDEX idx_revenue_shares_clinic ON revenue_shares (clinic_id);
CREATE INDEX idx_revenue_shares_status ON revenue_shares (status);

CREATE TRIGGER set_updated_at_revenue_shares
    BEFORE UPDATE ON revenue_shares
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
