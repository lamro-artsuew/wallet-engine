-- Composite partial indexes for velocity window queries.
-- Partial index on velocity_check_passed = TRUE eliminates rejected attempts,
-- which is the dominant optimization for high-rejection-rate scenarios.

CREATE INDEX IF NOT EXISTS idx_wa_velocity_window
    ON withdrawal_attempts (user_id, chain, token_symbol, created_at)
    WHERE velocity_check_passed = TRUE;

CREATE INDEX IF NOT EXISTS idx_wa_last_withdrawal
    ON withdrawal_attempts (user_id, chain, created_at DESC)
    WHERE velocity_check_passed = TRUE;
