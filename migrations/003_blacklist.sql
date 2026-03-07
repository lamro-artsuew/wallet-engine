-- Blacklisted/frozen address monitoring for stablecoin compliance
-- Migration 003: Blacklist tables, stablecoin contracts, freeze events

-- Add blacklist flag to deposits
ALTER TABLE deposits ADD COLUMN IF NOT EXISTS is_from_blacklisted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS blacklisted_addresses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('OFAC', 'USDT_BLACKLIST', 'USDC_BLACKLIST', 'MANUAL', 'CHAINALYSIS', 'RISK_ENGINE')),
    reason TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, address, source)
);

CREATE INDEX IF NOT EXISTS idx_blacklist_chain_address ON blacklisted_addresses(chain, lower(address));
CREATE INDEX IF NOT EXISTS idx_blacklist_source ON blacklisted_addresses(source);

-- Stablecoin contract registry
CREATE TABLE IF NOT EXISTS stablecoin_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    symbol TEXT NOT NULL,
    issuer TEXT NOT NULL,
    has_blacklist BOOLEAN NOT NULL DEFAULT FALSE,
    has_freeze BOOLEAN NOT NULL DEFAULT FALSE,
    blacklist_method TEXT,
    freeze_method TEXT,
    decimals INT NOT NULL DEFAULT 18,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, contract_address)
);

-- Seed known stablecoin contracts
INSERT INTO stablecoin_contracts (chain, contract_address, symbol, issuer, has_blacklist, has_freeze, blacklist_method, decimals) VALUES
('ethereum', '0xdAC17F958D2ee523a2206206994597C13D831ec7', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 6),
('ethereum', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 6),
('bsc', '0x55d398326f99059fF775485246999027B3197955', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 18),
('bsc', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 18),
('polygon', '0xc2132D05D31c914a87C6611C10748AEb04B58e8F', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 6),
('polygon', '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 6),
('arbitrum', '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 6),
('arbitrum', '0xaf88d065e77c8cC2239327C5EDb3A432268e5831', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 6),
('optimism', '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 6),
('optimism', '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 6),
('avalanche', '0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7', 'USDT', 'TETHER', true, false, 'isBlackListed(address)', 6),
('avalanche', '0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E', 'USDC', 'CIRCLE', true, false, 'isBlacklisted(address)', 6)
ON CONFLICT (chain, contract_address) DO NOTHING;

-- Freeze events log
CREATE TABLE IF NOT EXISTS freeze_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('FROZEN', 'UNFROZEN', 'BLACKLISTED', 'UNBLACKLISTED')),
    tx_hash TEXT,
    block_number BIGINT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_freeze_events_address ON freeze_events(chain, lower(address));
