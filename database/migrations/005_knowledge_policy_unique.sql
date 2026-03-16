-- ============================================================
-- Add unique constraint on knowledge_documents.policy_number
-- Migration: 005_knowledge_policy_unique.sql
--
-- Required by the knowledge-base seeder, which upserts mandate
-- records using ON CONFLICT (policy_number).
-- ============================================================

-- Deduplicate any existing rows before adding the constraint
-- (keeps the most recently updated row per policy_number).
DELETE FROM knowledge_documents a
USING knowledge_documents b
WHERE a.policy_number = b.policy_number
  AND a.policy_number IS NOT NULL
  AND a.updated_at < b.updated_at;

ALTER TABLE knowledge_documents
    ADD CONSTRAINT uq_knowledge_documents_policy_number
    UNIQUE (policy_number);
