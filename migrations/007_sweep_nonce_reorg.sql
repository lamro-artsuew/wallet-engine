-- 007_sweep_nonce_reorg.sql
-- Adds nonce management, reorg tracking, and reconciliation tables

-- Nonce reservations: atomic nonce tracking per chain/address
CREATE TABLE IF NOT EXISTS nonce_reservations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain       TEXT NOT NULL,
    address     TEXT NOT NULL,
    nonce       BIGINT NOT NULL,
    purpose     TEXT NOT NULL CHECK (purpose IN ('SWEEP', 'WITHDRAWAL', 'GAS_FUND')),
    reference_id UUID,
    tx_hash     TEXT,
    state       TEXT NOT NULL DEFAULT 'RESERVED'
                    CHECK (state IN ('RESERVED', 'BROADCAST', 'CONFIRMED', 'FAILED', 'EXPIRED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain, address, nonce)
);
CREATE INDEX IF NOT EXISTS idx_nonce_chain_addr_state ON nonce_reservations(chain, address, state);

-- Reorg events: track detected reorganizations
CREATE TABLE IF NOT EXISTS reorg_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain           TEXT NOT NULL,
    detected_at_block BIGINT NOT NULL,
    fork_block      BIGINT NOT NULL,
    depth           INT NOT NULL,
    old_hash        TEXT NOT NULL,
    new_hash        TEXT NOT NULL,
    affected_deposits INT NOT NULL DEFAULT 0,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_reorg_chain_resolved ON reorg_events(chain, resolved);

-- Reconciliation runs: audit trail for balance checks
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain           TEXT NOT NULL,
    run_type        TEXT NOT NULL CHECK (run_type IN ('HOT_WALLET', 'LIABILITY', 'TRIAL_BALANCE')),
    on_chain_balance NUMERIC(78, 0),
    ledger_balance  NUMERIC(78, 0),
    drift           NUMERIC(78, 0),
    drift_pct       NUMERIC(10, 6),
    status          TEXT NOT NULL DEFAULT 'PASS'
                        CHECK (status IN ('PASS', 'DRIFT', 'FAIL', 'ERROR')),
    details         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_recon_chain_status ON reconciliation_runs(chain, status);
CREATE INDEX IF NOT EXISTS idx_recon_created ON reconciliation_runs(created_at DESC);

-- Block hash history for reorg detection (supplement chain_index_state)
CREATE TABLE IF NOT EXISTS block_hash_history (
    chain       TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain, block_number)
);

-- Gas price history for oracle
CREATE TABLE IF NOT EXISTS gas_price_history (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain       TEXT NOT NULL,
    gas_price   NUMERIC(78, 0) NOT NULL,
    base_fee    NUMERIC(78, 0),
    priority_fee NUMERIC(78, 0),
    sampled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gas_chain_time ON gas_price_history(chain, sampled_at DESC);
