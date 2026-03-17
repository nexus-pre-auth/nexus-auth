-- ============================================================
-- Add metadata column to webpt_connections
-- Migration: 006_webpt_connections_metadata.sql
--
-- Stores clinic contact info (name, email) used by the
-- scheduler for weekly digest targeting, and simulation
-- flags for test data cleanup.
-- ============================================================

ALTER TABLE webpt_connections
    ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::JSONB;

CREATE INDEX IF NOT EXISTS idx_webpt_connections_metadata
    ON webpt_connections USING GIN (metadata);
