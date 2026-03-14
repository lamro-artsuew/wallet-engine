-- 006_ledger_hardening.sql
-- Fixes critical concurrency and scalability issues in the ledger system:
-- 1. ledger_chain_head: serializes concurrent hash chain writes (prevents fork)
-- 2. ledger_account_balances: materialized balances for O(1) lookups

-- Chain head singleton row — locked via SELECT FOR UPDATE during PostEntry
-- to prevent two concurrent transactions from reading the same previous_hash
CREATE TABLE IF NOT EXISTS ledger_chain_head (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_hash   TEXT NOT NULL DEFAULT 'GENESIS',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the initial chain head
INSERT INTO ledger_chain_head (id, last_hash, updated_at)
VALUES (1, 'GENESIS', NOW())
ON CONFLICT (id) DO NOTHING;

-- Materialized account balances — updated atomically during PostEntry
-- Provides O(1) balance lookups instead of scanning all ledger_lines
CREATE TABLE IF NOT EXISTS ledger_account_balances (
    account_id  UUID NOT NULL REFERENCES ledger_accounts(id),
    currency    TEXT NOT NULL,
    balance     NUMERIC(78, 0) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, currency)
);
