-- Migration 008: Blacklist hardening indexes and updated_at column
-- Adds updated_at to blacklisted_addresses, creates proper partial indexes,
-- and adds missing freeze_events index.

-- Add updated_at column for audit trail on deactivation
ALTER TABLE blacklisted_addresses
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Partial index for active blacklisted addresses — used by IsBlacklisted hot path.
-- The existing idx_blacklist_chain_address covers (chain, lower(address)) but not
-- the is_active filter, causing extra row filtering.
CREATE INDEX IF NOT EXISTS idx_blacklist_active_chain_address
    ON blacklisted_addresses (chain, address)
    WHERE is_active = TRUE;

-- Index for freeze event lookups by chain+address (currently missing)
CREATE INDEX IF NOT EXISTS idx_freeze_events_chain_address
    ON freeze_events (chain, address);
