package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// WithdrawalRepo handles withdrawal persistence
type WithdrawalRepo struct {
	pool *pgxpool.Pool
}

func NewWithdrawalRepo(pool *pgxpool.Pool) *WithdrawalRepo {
	return &WithdrawalRepo{pool: pool}
}

// Create inserts a new withdrawal. If the idempotency_key already exists (unique
// constraint violation), the raw postgres error is returned — callers should use
// FindByIdempotencyKey first or handle the unique violation explicitly.
func (r *WithdrawalRepo) Create(ctx context.Context, w *domain.Withdrawal) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO withdrawals (
			id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state,
			source_wallet_id, required_confirmations
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`,
		w.ID, w.IdempotencyKey, w.WorkspaceID, w.UserID, w.Chain, w.ToAddress,
		w.TokenAddress, w.TokenSymbol, w.Amount.String(), w.Decimals, string(w.State),
		w.SourceWalletID, w.RequiredConfs,
	)
	return err
}

// FindByID returns a withdrawal by ID
func (r *WithdrawalRepo) FindByID(ctx context.Context, id uuid.UUID) (*domain.Withdrawal, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE id = $1
	`, id)
	return scanWithdrawal(row)
}

// FindByIdempotencyKey returns a withdrawal by its idempotency key
func (r *WithdrawalRepo) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Withdrawal, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE idempotency_key = $1
	`, key)
	return scanWithdrawal(row)
}

// UpdateState atomically transitions a withdrawal to a new state.
// fromState is the expected current state — if the row's actual state differs,
// the update is rejected and ErrStateConflict is returned (prevents TOCTOU).
func (r *WithdrawalRepo) UpdateState(ctx context.Context, id uuid.UUID, fromState, toState domain.WithdrawalState, errorMsg *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET
			state = $1,
			error_message = COALESCE($2, error_message),
			updated_at = NOW(),
			broadcast_at = CASE WHEN $1 = 'BROADCAST' THEN NOW() ELSE broadcast_at END,
			confirmed_at = CASE WHEN $1 = 'CONFIRMED' THEN NOW() ELSE confirmed_at END
		WHERE id = $3 AND state = $4
	`, string(toState), errorMsg, id, string(fromState))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal %s: %w (expected %s)", id, ErrStateConflict, fromState)
	}
	return nil
}

// SetTxHash atomically records the broadcast transaction hash and nonce.
// Only allowed from SIGNING or SIGNED states (prevents re-broadcast of
// confirmed/failed withdrawals). Returns ErrStateConflict if state doesn't match.
func (r *WithdrawalRepo) SetTxHash(ctx context.Context, id uuid.UUID, txHash string, nonce int64) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET
			tx_hash = $1, nonce = $2, state = 'BROADCAST',
			broadcast_at = NOW(), updated_at = NOW()
		WHERE id = $3 AND state IN ('SIGNING', 'SIGNED')
	`, txHash, nonce, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal %s: %w (expected SIGNING or SIGNED for broadcast)", id, ErrStateConflict)
	}
	return nil
}

// SetRiskScore updates the risk assessment for a withdrawal
func (r *WithdrawalRepo) SetRiskScore(ctx context.Context, id uuid.UUID, score int, level string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET risk_score = $1, risk_level = $2, updated_at = NOW()
		WHERE id = $3
	`, score, level, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal %s not found", id)
	}
	return nil
}

// UpdateConfirmations monotonically updates the confirmation count.
// Uses GREATEST to prevent concurrent pollers from regressing the count.
func (r *WithdrawalRepo) UpdateConfirmations(ctx context.Context, id uuid.UUID, confs int) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET confirmations = GREATEST(confirmations, $1), updated_at = NOW()
		WHERE id = $2
	`, confs, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal %s not found", id)
	}
	return nil
}

// FindByChainAndState returns withdrawals for a chain in a given state.
// Limited to prevent unbounded memory usage on executor hot paths.
func (r *WithdrawalRepo) FindByChainAndState(ctx context.Context, chain string, state domain.WithdrawalState, limit int) ([]*domain.Withdrawal, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE chain = $1 AND state = $2
		ORDER BY created_at ASC
		LIMIT $3
	`, chain, string(state), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWithdrawals(rows)
}

// FindByUser returns withdrawals for a user
func (r *WithdrawalRepo) FindByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*domain.Withdrawal, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE user_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWithdrawals(rows)
}

// FindByWorkspace returns withdrawals for a workspace
func (r *WithdrawalRepo) FindByWorkspace(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*domain.Withdrawal, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE workspace_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, workspaceID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWithdrawals(rows)
}

// FindAll returns all withdrawals with pagination (admin view)
func (r *WithdrawalRepo) FindAll(ctx context.Context, limit, offset int) ([]*domain.Withdrawal, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals
		ORDER BY created_at DESC LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWithdrawals(rows)
}

// scanWithdrawalRow scans a single withdrawal row. Used by both scanWithdrawal
// (QueryRow) and scanWithdrawals (Rows) to eliminate duplication.
func scanWithdrawalRow(scanner interface {
	Scan(dest ...interface{}) error
}) (*domain.Withdrawal, error) {
	w := &domain.Withdrawal{}
	var amountStr string
	var gasPriceStr, feeStr *string
	err := scanner.Scan(
		&w.ID, &w.IdempotencyKey, &w.WorkspaceID, &w.UserID, &w.Chain, &w.ToAddress,
		&w.TokenAddress, &w.TokenSymbol, &amountStr, &w.Decimals, &w.State, &w.TxHash, &w.Nonce,
		&gasPriceStr, &w.GasUsed, &feeStr, &w.RiskScore, &w.RiskLevel, &w.SourceWalletID,
		&w.LedgerEntryID, &w.ErrorMessage, &w.Confirmations, &w.RequiredConfs,
		&w.CreatedAt, &w.UpdatedAt, &w.BroadcastAt, &w.ConfirmedAt,
	)
	if err != nil {
		return nil, err
	}
	var parseErr error
	w.Amount, parseErr = parseBigInt(amountStr)
	if parseErr != nil {
		return nil, fmt.Errorf("parse amount for withdrawal %s: %w", w.ID, parseErr)
	}
	if err := parseOptionalBigInt(gasPriceStr, &w.GasPrice); err != nil {
		return nil, fmt.Errorf("parse gas_price for withdrawal %s: %w", w.ID, err)
	}
	if err := parseOptionalBigInt(feeStr, &w.Fee); err != nil {
		return nil, fmt.Errorf("parse fee for withdrawal %s: %w", w.ID, err)
	}
	return w, nil
}

func scanWithdrawal(row pgx.Row) (*domain.Withdrawal, error) {
	return scanWithdrawalRow(row)
}

func scanWithdrawals(rows pgx.Rows) ([]*domain.Withdrawal, error) {
	var withdrawals []*domain.Withdrawal
	for rows.Next() {
		w, err := scanWithdrawalRow(rows)
		if err != nil {
			return nil, err
		}
		withdrawals = append(withdrawals, w)
	}
	return withdrawals, rows.Err()
}
