-- Velocity limits for per-transaction rate limiting and geo checks
CREATE TABLE IF NOT EXISTS velocity_limits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope TEXT NOT NULL CHECK (scope IN ('GLOBAL', 'WORKSPACE', 'USER', 'CHAIN')),
    scope_id TEXT, -- NULL for GLOBAL, workspace_id/user_id/chain for others
    chain TEXT, -- NULL = all chains
    token_symbol TEXT, -- NULL = all tokens
    max_amount_per_tx NUMERIC(78, 0), -- Max single withdrawal amount (smallest unit)
    max_amount_per_hour NUMERIC(78, 0),
    max_amount_per_day NUMERIC(78, 0),
    max_count_per_hour INT,
    max_count_per_day INT,
    cooldown_seconds INT DEFAULT 0, -- Min time between withdrawals
    geo_allowed_countries TEXT[], -- NULL = all allowed
    geo_blocked_countries TEXT[], -- NULL = none blocked
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_velocity_limits_scope ON velocity_limits(scope, scope_id);

-- Withdrawal attempts log for velocity tracking
CREATE TABLE IF NOT EXISTS withdrawal_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    withdrawal_id UUID REFERENCES withdrawals(id),
    workspace_id UUID NOT NULL,
    user_id UUID NOT NULL,
    chain TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    source_ip TEXT,
    country_code TEXT,
    velocity_check_passed BOOLEAN NOT NULL,
    rejection_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_withdrawal_attempts_user_time ON withdrawal_attempts(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_withdrawal_attempts_workspace_time ON withdrawal_attempts(workspace_id, created_at);
