-- Wallet Engine Schema — Phase 1: Foundation
-- All tables use UUIDs, immutable audit columns, and idempotency keys.

-- Managed wallets (hot, warm, cold)
CREATE TABLE IF NOT EXISTS wallets (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain           TEXT NOT NULL,
    address         TEXT NOT NULL,
    tier            TEXT NOT NULL CHECK (tier IN ('HOT', 'WARM', 'COLD')),
    label           TEXT NOT NULL DEFAULT '',
    encrypted_key   BYTEA,                          -- encrypted private key (hot only)
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, address)
);

-- HD derivation state per chain
CREATE TABLE IF NOT EXISTS hd_derivation_state (
    chain           TEXT PRIMARY KEY,
    master_seed_id  TEXT NOT NULL,                   -- Vault path or key ID, never raw seed
    next_index      BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-user per-chain deposit addresses
CREATE TABLE IF NOT EXISTS deposit_addresses (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID NOT NULL,
    user_id         UUID NOT NULL,
    chain           TEXT NOT NULL,
    address         TEXT NOT NULL,
    derivation_path TEXT NOT NULL,
    derivation_index BIGINT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, address),
    UNIQUE(chain, derivation_index)
);
CREATE INDEX idx_deposit_addresses_user ON deposit_addresses(user_id, chain);
CREATE INDEX idx_deposit_addresses_workspace ON deposit_addresses(workspace_id, chain);

-- Deposits detected on-chain
CREATE TABLE IF NOT EXISTS deposits (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key     TEXT NOT NULL UNIQUE,        -- chain:txhash:logindex
    chain               TEXT NOT NULL,
    tx_hash             TEXT NOT NULL,
    log_index           INT NOT NULL DEFAULT 0,
    block_number        BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    from_address        TEXT NOT NULL,
    to_address          TEXT NOT NULL,
    token_address       TEXT NOT NULL DEFAULT '',     -- empty = native
    token_symbol        TEXT NOT NULL,
    amount              NUMERIC(78, 0) NOT NULL,
    decimals            INT NOT NULL DEFAULT 18,
    state               TEXT NOT NULL DEFAULT 'DETECTED'
                        CHECK (state IN ('DETECTED','PENDING','CONFIRMING','CONFIRMED','SWEPT','FAILED','REORGED')),
    confirmations       INT NOT NULL DEFAULT 0,
    required_confirmations INT NOT NULL DEFAULT 12,
    deposit_address_id  UUID NOT NULL REFERENCES deposit_addresses(id),
    workspace_id        UUID NOT NULL,
    user_id             UUID NOT NULL,
    sweep_tx_hash       TEXT,
    ledger_entry_id     UUID,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at        TIMESTAMPTZ,
    swept_at            TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_deposits_chain_state ON deposits(chain, state);
CREATE INDEX idx_deposits_user ON deposits(user_id, chain);
CREATE INDEX idx_deposits_workspace ON deposits(workspace_id);
CREATE INDEX idx_deposits_block ON deposits(chain, block_number);
CREATE INDEX idx_deposits_tx ON deposits(tx_hash);

-- Withdrawals (outbound transfers)
CREATE TABLE IF NOT EXISTS withdrawals (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key     TEXT NOT NULL UNIQUE,
    workspace_id        UUID NOT NULL,
    user_id             UUID NOT NULL,
    chain               TEXT NOT NULL,
    to_address          TEXT NOT NULL,
    token_address       TEXT NOT NULL DEFAULT '',
    token_symbol        TEXT NOT NULL,
    amount              NUMERIC(78, 0) NOT NULL,
    decimals            INT NOT NULL DEFAULT 18,
    state               TEXT NOT NULL DEFAULT 'INITIATED'
                        CHECK (state IN (
                            'INITIATED','RISK_CHECK','RISK_APPROVED','RISK_REJECTED',
                            'SIGNING','SIGNED','BROADCAST','PENDING_CONFIRMATION',
                            'CONFIRMED','FAILED','EXPIRED','MANUAL_REVIEW'
                        )),
    tx_hash             TEXT,
    nonce               BIGINT,
    gas_price           NUMERIC(78, 0),
    gas_used            BIGINT,
    fee                 NUMERIC(78, 0),
    risk_score          INT,
    risk_level          TEXT,
    source_wallet_id    UUID REFERENCES wallets(id),
    ledger_entry_id     UUID,
    error_message       TEXT,
    confirmations       INT NOT NULL DEFAULT 0,
    required_confirmations INT NOT NULL DEFAULT 12,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    broadcast_at        TIMESTAMPTZ,
    confirmed_at        TIMESTAMPTZ
);
CREATE INDEX idx_withdrawals_chain_state ON withdrawals(chain, state);
CREATE INDEX idx_withdrawals_user ON withdrawals(user_id);
CREATE INDEX idx_withdrawals_workspace ON withdrawals(workspace_id);
CREATE INDEX idx_withdrawals_tx ON withdrawals(tx_hash);

-- Double-entry ledger accounts
CREATE TABLE IF NOT EXISTS ledger_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code            TEXT NOT NULL UNIQUE,            -- e.g. "ASSET:HOT:ETH:USDT"
    name            TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('ASSET','LIABILITY','REVENUE','EXPENSE')),
    chain           TEXT,
    currency        TEXT NOT NULL,
    parent_id       UUID REFERENCES ledger_accounts(id),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Journal entries (immutable, hash-chained)
CREATE TABLE IF NOT EXISTS ledger_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    entry_type      TEXT NOT NULL,                   -- DEPOSIT, WITHDRAWAL, SWEEP, FEE, REBALANCE
    description     TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','POSTED','REVERSED')),
    reference_type  TEXT,                            -- 'deposit', 'withdrawal', 'sweep'
    reference_id    UUID,
    previous_hash   TEXT NOT NULL DEFAULT '',
    entry_hash      TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_ledger_entries_type ON ledger_entries(entry_type);
CREATE INDEX idx_ledger_entries_ref ON ledger_entries(reference_type, reference_id);

-- Journal lines (debits & credits within an entry)
CREATE TABLE IF NOT EXISTS ledger_lines (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id        UUID NOT NULL REFERENCES ledger_entries(id),
    account_id      UUID NOT NULL REFERENCES ledger_accounts(id),
    amount          NUMERIC(78, 0) NOT NULL,
    currency        TEXT NOT NULL,
    is_debit        BOOLEAN NOT NULL,
    sequence        INT NOT NULL DEFAULT 0,
    UNIQUE(entry_id, sequence)
);
CREATE INDEX idx_ledger_lines_account ON ledger_lines(account_id);

-- WORM enforcement: prevent UPDATE/DELETE on ledger_entries and ledger_lines
CREATE OR REPLACE FUNCTION prevent_ledger_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Ledger entries are immutable (WORM). Use compensating entries for corrections.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_entries_worm
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER trg_ledger_lines_worm
    BEFORE UPDATE OR DELETE ON ledger_lines
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

-- Per-chain block indexing cursor
CREATE TABLE IF NOT EXISTS chain_index_state (
    chain               TEXT PRIMARY KEY,
    last_block_number   BIGINT NOT NULL DEFAULT 0,
    last_block_hash     TEXT NOT NULL DEFAULT '',
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sweep records (deposit address → hot wallet)
CREATE TABLE IF NOT EXISTS sweeps (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain           TEXT NOT NULL,
    from_address    TEXT NOT NULL,
    to_address      TEXT NOT NULL,
    token_address   TEXT NOT NULL DEFAULT '',
    amount          NUMERIC(78, 0) NOT NULL,
    tx_hash         TEXT,
    gas_cost        NUMERIC(78, 0),
    state           TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','BROADCAST','CONFIRMED','FAILED')),
    deposit_id      UUID NOT NULL REFERENCES deposits(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at    TIMESTAMPTZ
);
CREATE INDEX idx_sweeps_chain_state ON sweeps(chain, state);

-- Seed default ledger accounts for each supported chain
INSERT INTO ledger_accounts (code, name, type, chain, currency) VALUES
    -- Hot wallet asset accounts
    ('ASSET:HOT:ETH:USDT', 'ETH Hot Wallet USDT', 'ASSET', 'ethereum', 'USDT'),
    ('ASSET:HOT:BSC:USDT', 'BSC Hot Wallet USDT', 'ASSET', 'bsc', 'USDT'),
    ('ASSET:HOT:POLYGON:USDT', 'Polygon Hot Wallet USDT', 'ASSET', 'polygon', 'USDT'),
    ('ASSET:HOT:ARBITRUM:USDT', 'Arbitrum Hot Wallet USDT', 'ASSET', 'arbitrum', 'USDT'),
    ('ASSET:HOT:OPTIMISM:USDT', 'Optimism Hot Wallet USDT', 'ASSET', 'optimism', 'USDT'),
    ('ASSET:HOT:AVALANCHE:USDT', 'Avalanche Hot Wallet USDT', 'ASSET', 'avalanche', 'USDT'),
    -- User liability accounts (aggregate — per-user tracking via reference)
    ('LIABILITY:USER:USDT', 'User USDT Balances', 'LIABILITY', NULL, 'USDT'),
    ('LIABILITY:USER:USDC', 'User USDC Balances', 'LIABILITY', NULL, 'USDC'),
    -- Fee revenue accounts
    ('REVENUE:WITHDRAWAL_FEE:USDT', 'Withdrawal Fee Revenue USDT', 'REVENUE', NULL, 'USDT'),
    -- Gas expense accounts
    ('EXPENSE:GAS:ETH', 'ETH Gas Expenses', 'EXPENSE', 'ethereum', 'ETH'),
    ('EXPENSE:GAS:BSC', 'BSC Gas Expenses', 'EXPENSE', 'bsc', 'BNB'),
    ('EXPENSE:GAS:POLYGON', 'Polygon Gas Expenses', 'EXPENSE', 'polygon', 'MATIC')
ON CONFLICT (code) DO NOTHING;
