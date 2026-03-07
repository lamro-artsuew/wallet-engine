-- Fiat accounts
CREATE TABLE IF NOT EXISTS fiat_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    currency TEXT NOT NULL, -- USD, EUR, GBP
    provider TEXT NOT NULL, -- BANK, EMI, STRIPE, WISE
    provider_account_id TEXT, -- External account reference
    account_type TEXT NOT NULL CHECK (account_type IN ('OPERATING', 'CLIENT_FUNDS', 'SETTLEMENT', 'FEE_COLLECTION')),
    balance NUMERIC(20, 2) NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workspace_id, currency, provider, account_type)
);

-- Fiat transactions
CREATE TABLE IF NOT EXISTS fiat_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    fiat_account_id UUID REFERENCES fiat_accounts(id),
    type TEXT NOT NULL CHECK (type IN ('DEPOSIT', 'WITHDRAWAL', 'CONVERSION', 'FEE', 'ADJUSTMENT')),
    currency TEXT NOT NULL,
    amount NUMERIC(20, 2) NOT NULL,
    reference TEXT, -- Bank reference / wire ID
    counterparty TEXT, -- User/entity
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'REVERSED')),
    ledger_entry_id UUID, -- Link to ledger
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fiat_tx_account ON fiat_transactions(fiat_account_id);
CREATE INDEX IF NOT EXISTS idx_fiat_tx_workspace ON fiat_transactions(workspace_id, created_at);
CREATE INDEX IF NOT EXISTS idx_fiat_tx_state ON fiat_transactions(state);

-- Conversion rates cache
CREATE TABLE IF NOT EXISTS conversion_rates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_currency TEXT NOT NULL, -- USD, BTC, ETH, USDT
    to_currency TEXT NOT NULL,
    rate NUMERIC(24, 12) NOT NULL, -- High precision rate
    source TEXT NOT NULL DEFAULT 'MANUAL', -- MANUAL, COINMARKETCAP, COINGECKO, INTERNAL
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(from_currency, to_currency, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_conversion_rates_pair ON conversion_rates(from_currency, to_currency, valid_from DESC);
