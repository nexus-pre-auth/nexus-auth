-- ============================================================
-- Approval Gate & Resubmission Audit Trail
-- Migration: 007_approval_gate.sql
--
-- Adds:
--   1. New denial status values: pending_approval, approved
--   2. approval_tokens — one-time email-link tokens for clinic approval
--   3. denial_resubmissions — audit trail of every payer submission attempt
-- ============================================================

-- Extend the status enum with approval workflow states.
-- PostgreSQL requires ADD VALUE outside a transaction block, so these
-- are safe to run independently (they are idempotent via IF NOT EXISTS).
ALTER TYPE denial_status_enum ADD VALUE IF NOT EXISTS 'pending_approval' AFTER 'fixed';
ALTER TYPE denial_status_enum ADD VALUE IF NOT EXISTS 'approved'         AFTER 'pending_approval';

-- ============================================================
-- APPROVAL TOKENS
-- One token per denial; valid for both approve and reject actions.
-- Token is a 32-byte URL-safe secret (43 chars of base64url).
-- ============================================================

CREATE TABLE IF NOT EXISTS approval_tokens (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    denial_id     UUID NOT NULL REFERENCES recoverable_denials(id) ON DELETE CASCADE,
    token         VARCHAR(64) UNIQUE NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    used_at       TIMESTAMPTZ,           -- set when token is consumed
    used_action   VARCHAR(10),           -- 'approve' | 'reject'
    rejection_reason TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_approval_tokens_denial ON approval_tokens (denial_id);
CREATE INDEX IF NOT EXISTS idx_approval_tokens_token  ON approval_tokens (token);
-- Fast lookup for unexpired, unused tokens
CREATE INDEX IF NOT EXISTS idx_approval_tokens_active
    ON approval_tokens (token) WHERE used_at IS NULL;

-- ============================================================
-- DENIAL RESUBMISSIONS
-- One row per payer submission attempt so we can track round-trips.
-- ============================================================

CREATE TABLE IF NOT EXISTS denial_resubmissions (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    denial_id         UUID NOT NULL REFERENCES recoverable_denials(id) ON DELETE CASCADE,
    attempt_number    INT NOT NULL DEFAULT 1,
    fixes_snapshot    JSONB,             -- copy of fixes_applied at time of submission
    submitted_at      TIMESTAMPTZ DEFAULT NOW(),
    payer_response    TEXT,
    payer_response_at TIMESTAMPTZ,
    status            VARCHAR(20) NOT NULL DEFAULT 'pending',
                                        -- pending | accepted | rejected_payer
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resubmissions_denial ON denial_resubmissions (denial_id);
CREATE INDEX IF NOT EXISTS idx_resubmissions_status ON denial_resubmissions (status);

-- ============================================================
-- Helper: add submitted_at column if not already present
-- (it was defined in 004 but may not have been backfilled)
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE  table_name = 'recoverable_denials'
          AND  column_name = 'approved_at'
    ) THEN
        ALTER TABLE recoverable_denials ADD COLUMN approved_at TIMESTAMPTZ;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE  table_name = 'recoverable_denials'
          AND  column_name = 'rejection_reason'
    ) THEN
        ALTER TABLE recoverable_denials ADD COLUMN rejection_reason TEXT;
    END IF;
END $$;
