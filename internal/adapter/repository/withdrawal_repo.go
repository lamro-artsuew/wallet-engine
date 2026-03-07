package repository

import (
	"context"
	"fmt"
	"math/big"

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

// Create inserts a new withdrawal
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

// UpdateState transitions a withdrawal to a new state
func (r *WithdrawalRepo) UpdateState(ctx context.Context, id uuid.UUID, state domain.WithdrawalState, errorMsg *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET
			state = $1,
			error_message = COALESCE($2, error_message),
			updated_at = NOW(),
			broadcast_at = CASE WHEN $1 = 'BROADCAST' THEN NOW() ELSE broadcast_at END,
			confirmed_at = CASE WHEN $1 = 'CONFIRMED' THEN NOW() ELSE confirmed_at END
		WHERE id = $3
	`, string(state), errorMsg, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("withdrawal %s not found", id)
	}
	return nil
}

// SetTxHash records the broadcast transaction hash and nonce
func (r *WithdrawalRepo) SetTxHash(ctx context.Context, id uuid.UUID, txHash string, nonce int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET
			tx_hash = $1, nonce = $2, state = 'BROADCAST',
			broadcast_at = NOW(), updated_at = NOW()
		WHERE id = $3
	`, txHash, nonce, id)
	return err
}

// SetRiskScore updates the risk assessment for a withdrawal
func (r *WithdrawalRepo) SetRiskScore(ctx context.Context, id uuid.UUID, score int, level string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET risk_score = $1, risk_level = $2, updated_at = NOW()
		WHERE id = $3
	`, score, level, id)
	return err
}

// UpdateConfirmations updates the confirmation count
func (r *WithdrawalRepo) UpdateConfirmations(ctx context.Context, id uuid.UUID, confs int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE withdrawals SET confirmations = $1, updated_at = NOW() WHERE id = $2
	`, confs, id)
	return err
}

// FindByChainAndState returns withdrawals for a chain in a given state
func (r *WithdrawalRepo) FindByChainAndState(ctx context.Context, chain string, state domain.WithdrawalState) ([]*domain.Withdrawal, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, workspace_id, user_id, chain, to_address,
			token_address, token_symbol, amount, decimals, state, tx_hash, nonce,
			gas_price, gas_used, fee, risk_score, risk_level, source_wallet_id,
			ledger_entry_id, error_message, confirmations, required_confirmations,
			created_at, updated_at, broadcast_at, confirmed_at
		FROM withdrawals WHERE chain = $1 AND state = $2
		ORDER BY created_at ASC
	`, chain, string(state))
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

func scanWithdrawal(row pgx.Row) (*domain.Withdrawal, error) {
	w := &domain.Withdrawal{}
	var amountStr string
	var gasPriceStr, feeStr *string
	err := row.Scan(
		&w.ID, &w.IdempotencyKey, &w.WorkspaceID, &w.UserID, &w.Chain, &w.ToAddress,
		&w.TokenAddress, &w.TokenSymbol, &amountStr, &w.Decimals, &w.State, &w.TxHash, &w.Nonce,
		&gasPriceStr, &w.GasUsed, &feeStr, &w.RiskScore, &w.RiskLevel, &w.SourceWalletID,
		&w.LedgerEntryID, &w.ErrorMessage, &w.Confirmations, &w.RequiredConfs,
		&w.CreatedAt, &w.UpdatedAt, &w.BroadcastAt, &w.ConfirmedAt,
	)
	if err != nil {
		return nil, err
	}
	w.Amount = new(big.Int)
	w.Amount.SetString(amountStr, 10)
	if gasPriceStr != nil {
		w.GasPrice = new(big.Int)
		w.GasPrice.SetString(*gasPriceStr, 10)
	}
	if feeStr != nil {
		w.Fee = new(big.Int)
		w.Fee.SetString(*feeStr, 10)
	}
	return w, nil
}

func scanWithdrawals(rows pgx.Rows) ([]*domain.Withdrawal, error) {
	var withdrawals []*domain.Withdrawal
	for rows.Next() {
		w := &domain.Withdrawal{}
		var amountStr string
		var gasPriceStr, feeStr *string
		err := rows.Scan(
			&w.ID, &w.IdempotencyKey, &w.WorkspaceID, &w.UserID, &w.Chain, &w.ToAddress,
			&w.TokenAddress, &w.TokenSymbol, &amountStr, &w.Decimals, &w.State, &w.TxHash, &w.Nonce,
			&gasPriceStr, &w.GasUsed, &feeStr, &w.RiskScore, &w.RiskLevel, &w.SourceWalletID,
			&w.LedgerEntryID, &w.ErrorMessage, &w.Confirmations, &w.RequiredConfs,
			&w.CreatedAt, &w.UpdatedAt, &w.BroadcastAt, &w.ConfirmedAt,
		)
		if err != nil {
			return nil, err
		}
		w.Amount = new(big.Int)
		w.Amount.SetString(amountStr, 10)
		if gasPriceStr != nil {
			w.GasPrice = new(big.Int)
			w.GasPrice.SetString(*gasPriceStr, 10)
		}
		if feeStr != nil {
			w.Fee = new(big.Int)
			w.Fee.SetString(*feeStr, 10)
		}
		withdrawals = append(withdrawals, w)
	}
	return withdrawals, rows.Err()
}
