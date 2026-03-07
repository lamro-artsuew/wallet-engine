-- Rebalance policies per chain
CREATE TABLE IF NOT EXISTS rebalance_policies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain TEXT NOT NULL,
    token_symbol TEXT NOT NULL DEFAULT 'NATIVE',
    hot_target_pct NUMERIC(5,2) NOT NULL DEFAULT 3.00,
    hot_max_pct NUMERIC(5,2) NOT NULL DEFAULT 5.00,
    hot_min_pct NUMERIC(5,2) NOT NULL DEFAULT 1.00,
    warm_target_pct NUMERIC(5,2) NOT NULL DEFAULT 10.00,
    warm_max_pct NUMERIC(5,2) NOT NULL DEFAULT 15.00,
    min_rebalance_amount NUMERIC(78, 0) NOT NULL DEFAULT 0,
    auto_rebalance BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, token_symbol)
);

-- Rebalance operations log
CREATE TABLE IF NOT EXISTS rebalance_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    from_tier TEXT NOT NULL CHECK (from_tier IN ('HOT', 'WARM', 'COLD')),
    to_tier TEXT NOT NULL CHECK (to_tier IN ('HOT', 'WARM', 'COLD')),
    from_wallet_id UUID REFERENCES wallets(id),
    to_wallet_id UUID REFERENCES wallets(id),
    amount NUMERIC(78, 0) NOT NULL,
    tx_hash TEXT,
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'APPROVED', 'SIGNING', 'BROADCAST', 'CONFIRMED', 'FAILED', 'REJECTED')),
    requires_approval BOOLEAN NOT NULL DEFAULT TRUE,
    approved_by TEXT,
    ledger_entry_id UUID,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rebalance_ops_state ON rebalance_operations(state);
CREATE INDEX IF NOT EXISTS idx_rebalance_ops_chain ON rebalance_operations(chain, token_symbol);

-- Seed default rebalance policies for supported chains
INSERT INTO rebalance_policies (chain, token_symbol, hot_target_pct, hot_max_pct, hot_min_pct, warm_target_pct, warm_max_pct, auto_rebalance) VALUES
('ethereum', 'NATIVE', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('ethereum', 'USDT', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('ethereum', 'USDC', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('bsc', 'NATIVE', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('bsc', 'USDT', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('polygon', 'NATIVE', 3.00, 5.00, 1.00, 10.00, 15.00, false),
('polygon', 'USDT', 3.00, 5.00, 1.00, 10.00, 15.00, false)
ON CONFLICT (chain, token_symbol) DO NOTHING;
