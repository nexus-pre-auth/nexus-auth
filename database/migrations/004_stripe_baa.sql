-- =============================================================================
-- CodeMed AI — Migration 004: Stripe + BAA Trust Infrastructure
-- =============================================================================
-- Adds columns and tables for:
--   - Stripe customer / payment method tracking on clinics
--   - BAA signature records
--   - Payment history ledger
--   - Revenue share tracking (contingency fee model)
--   - Denial recovery records (referenced by invoices)
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Extend clinics table with Stripe + BAA fields
-- (Safe to run multiple times — uses IF NOT EXISTS / idempotent ALTER)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'clinics' AND column_name = 'stripe_customer_id'
    ) THEN
        ALTER TABLE clinics ADD COLUMN stripe_customer_id        VARCHAR(100);
        ALTER TABLE clinics ADD COLUMN default_payment_method    VARCHAR(100);
        ALTER TABLE clinics ADD COLUMN baa_signed                BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE clinics ADD COLUMN baa_signed_at             TIMESTAMPTZ;
        ALTER TABLE clinics ADD COLUMN baa_document_url          TEXT;
        ALTER TABLE clinics ADD COLUMN baa_signer_name           TEXT;
        ALTER TABLE clinics ADD COLUMN baa_signer_email          TEXT;
        ALTER TABLE clinics ADD COLUMN integration_disabled      BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_clinics_stripe_customer
    ON clinics (stripe_customer_id)
    WHERE stripe_customer_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- BAA signature audit log
-- Keeps full history: one row per version / renewal.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS baa_signatures (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clinic_id           UUID NOT NULL,
    -- Signer
    signer_name         TEXT NOT NULL,
    signer_email        TEXT NOT NULL,
    signer_title        TEXT,
    -- Document
    document_version    VARCHAR(20) NOT NULL DEFAULT '1.0',
    document_url        TEXT,                              -- S3 / DocuSign URL
    hellosign_request_id TEXT,
    -- Status
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'signed', 'declined', 'expired')),
    signed_at           TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ,
    -- Metadata
    ip_address          INET,
    user_agent          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_baa_clinic_id    ON baa_signatures (clinic_id);
CREATE INDEX IF NOT EXISTS idx_baa_status       ON baa_signatures (status);
CREATE INDEX IF NOT EXISTS idx_baa_signed_at    ON baa_signatures (signed_at DESC);

-- ---------------------------------------------------------------------------
-- Denial recovery records
-- Populated by the claims processing pipeline.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS denials (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clinic_id           UUID NOT NULL,
    claim_id            VARCHAR(100) NOT NULL,
    denial_code         VARCHAR(20)  NOT NULL,   -- e.g. CO-16, CO-50, CO-97
    denied_amount       NUMERIC(12, 2) NOT NULL,
    recovered_amount    NUMERIC(12, 2),
    status              VARCHAR(30)  NOT NULL DEFAULT 'denied'
                            CHECK (status IN ('denied', 'appealed', 'recovered', 'written_off')),
    appeal_letter_id    UUID,
    denied_at           TIMESTAMPTZ,
    appealed_at         TIMESTAMPTZ,
    recovered_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_denials_clinic_id    ON denials (clinic_id);
CREATE INDEX IF NOT EXISTS idx_denials_status       ON denials (status);
CREATE INDEX IF NOT EXISTS idx_denials_recovered_at ON denials (recovered_at DESC)
    WHERE status = 'recovered';
CREATE INDEX IF NOT EXISTS idx_denials_denial_code  ON denials (denial_code);

-- ---------------------------------------------------------------------------
-- Revenue shares (contingency fee ledger)
-- One row per clinic per billing period.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS revenue_shares (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clinic_id           UUID NOT NULL,
    period              VARCHAR(7) NOT NULL,   -- 'YYYY-MM'
    total_recovered     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    fee_rate            NUMERIC(5, 4) NOT NULL DEFAULT 0.1500,  -- 15 %
    fee_amount          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'invoiced', 'paid', 'disputed', 'written_off')),
    stripe_invoice_id   VARCHAR(100),
    paid_amount         NUMERIC(12, 2),
    paid_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (clinic_id, period)
);

CREATE INDEX IF NOT EXISTS idx_revenue_shares_clinic_period
    ON revenue_shares (clinic_id, period);
CREATE INDEX IF NOT EXISTS idx_revenue_shares_status
    ON revenue_shares (status);

-- ---------------------------------------------------------------------------
-- Payment history ledger (Stripe event mirror)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payment_history (
    id                  BIGSERIAL PRIMARY KEY,
    clinic_id           UUID,
    stripe_invoice_id   VARCHAR(100) NOT NULL,
    stripe_customer_id  VARCHAR(100),
    amount              NUMERIC(12, 2) NOT NULL,
    currency            CHAR(3) NOT NULL DEFAULT 'USD',
    status              VARCHAR(50) NOT NULL,   -- Stripe status verbatim
    stripe_event_type   VARCHAR(80),
    description         TEXT,
    invoice_url         TEXT,
    pdf_url             TEXT,
    period              VARCHAR(7),
    raw_event           JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_history_clinic
    ON payment_history (clinic_id);
CREATE INDEX IF NOT EXISTS idx_payment_history_invoice
    ON payment_history (stripe_invoice_id);
CREATE INDEX IF NOT EXISTS idx_payment_history_created
    ON payment_history (created_at DESC);

-- ---------------------------------------------------------------------------
-- Convenience view: monthly billing summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW billing_summary AS
SELECT
    c.id                                AS clinic_id,
    c.name                              AS clinic_name,
    rs.period,
    rs.total_recovered,
    rs.fee_rate,
    rs.fee_amount,
    rs.status                           AS billing_status,
    rs.stripe_invoice_id,
    rs.paid_at,
    COUNT(d.id)                         AS denial_count,
    SUM(d.denied_amount)                AS total_denied,
    c.baa_signed,
    c.stripe_customer_id IS NOT NULL    AS has_stripe
FROM clinics c
LEFT JOIN revenue_shares rs ON rs.clinic_id = c.id
LEFT JOIN denials d ON d.clinic_id = c.id
    AND TO_CHAR(d.recovered_at, 'YYYY-MM') = rs.period
GROUP BY c.id, c.name, rs.period, rs.total_recovered,
         rs.fee_rate, rs.fee_amount, rs.status,
         rs.stripe_invoice_id, rs.paid_at,
         c.baa_signed, c.stripe_customer_id;

COMMENT ON VIEW billing_summary IS
    'Monthly revenue, denial, and billing status roll-up per clinic.';

COMMIT;
