package repository

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// RebalanceRepo handles rebalance policy and operation persistence
type RebalanceRepo struct {
	pool *pgxpool.Pool
}

func NewRebalanceRepo(pool *pgxpool.Pool) *RebalanceRepo {
	return &RebalanceRepo{pool: pool}
}

// GetPolicy returns the rebalance policy for a chain/token pair
func (r *RebalanceRepo) GetPolicy(ctx context.Context, chain, token string) (*domain.RebalancePolicy, error) {
	p := &domain.RebalancePolicy{}
	var minAmountStr string
	err := r.pool.QueryRow(ctx, `
		SELECT id, chain, token_symbol, hot_target_pct, hot_max_pct, hot_min_pct,
			warm_target_pct, warm_max_pct, min_rebalance_amount, auto_rebalance,
			is_active, created_at, updated_at
		FROM rebalance_policies WHERE chain = $1 AND token_symbol = $2
	`, chain, token).Scan(
		&p.ID, &p.Chain, &p.TokenSymbol, &p.HotTargetPct, &p.HotMaxPct, &p.HotMinPct,
		&p.WarmTargetPct, &p.WarmMaxPct, &minAmountStr, &p.AutoRebalance,
		&p.IsActive, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	p.MinRebalanceAmount = new(big.Int)
	p.MinRebalanceAmount.SetString(minAmountStr, 10)
	return p, nil
}

// ListPolicies returns all rebalance policies
func (r *RebalanceRepo) ListPolicies(ctx context.Context) ([]*domain.RebalancePolicy, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, token_symbol, hot_target_pct, hot_max_pct, hot_min_pct,
			warm_target_pct, warm_max_pct, min_rebalance_amount, auto_rebalance,
			is_active, created_at, updated_at
		FROM rebalance_policies ORDER BY chain, token_symbol
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolicies(rows)
}

// UpdatePolicy updates an existing rebalance policy
func (r *RebalanceRepo) UpdatePolicy(ctx context.Context, p *domain.RebalancePolicy) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_policies SET
			hot_target_pct = $1, hot_max_pct = $2, hot_min_pct = $3,
			warm_target_pct = $4, warm_max_pct = $5,
			min_rebalance_amount = $6, auto_rebalance = $7,
			is_active = $8, updated_at = NOW()
		WHERE chain = $9 AND token_symbol = $10
	`,
		p.HotTargetPct, p.HotMaxPct, p.HotMinPct,
		p.WarmTargetPct, p.WarmMaxPct,
		p.MinRebalanceAmount.String(), p.AutoRebalance,
		p.IsActive, p.Chain, p.TokenSymbol,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("policy for %s/%s not found", p.Chain, p.TokenSymbol)
	}
	return nil
}

// CreateOperation inserts a new rebalance operation
func (r *RebalanceRepo) CreateOperation(ctx context.Context, op *domain.RebalanceOperation) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO rebalance_operations (
			id, chain, token_symbol, from_tier, to_tier, from_wallet_id, to_wallet_id,
			amount, state, requires_approval, approved_by
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		op.ID, op.Chain, op.TokenSymbol, string(op.FromTier), string(op.ToTier),
		op.FromWalletID, op.ToWalletID, op.Amount.String(),
		string(op.State), op.RequiresApproval, op.ApprovedBy,
	)
	return err
}

// FindActiveOps returns PENDING or APPROVED operations for a given chain/token/direction
func (r *RebalanceRepo) FindActiveOps(ctx context.Context, chain, token string, fromTier, toTier domain.WalletTier) ([]*domain.RebalanceOperation, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, token_symbol, from_tier, to_tier, from_wallet_id, to_wallet_id,
			amount, tx_hash, state, requires_approval, approved_by, ledger_entry_id,
			error_message, created_at, updated_at
		FROM rebalance_operations
		WHERE chain = $1 AND token_symbol = $2
			AND from_tier = $3 AND to_tier = $4
			AND state IN ('PENDING', 'APPROVED', 'SIGNING', 'BROADCAST')
		ORDER BY created_at DESC
	`, chain, token, string(fromTier), string(toTier))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOperations(rows)
}

// PendingSourceWalletIDs returns a set of wallet IDs that are currently the source
// of PENDING/APPROVED/SIGNING/BROADCAST operations for a given chain.
func (r *RebalanceRepo) PendingSourceWalletIDs(ctx context.Context, chain string) (map[uuid.UUID]bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT from_wallet_id
		FROM rebalance_operations
		WHERE chain = $1 AND state IN ('PENDING', 'APPROVED', 'SIGNING', 'BROADCAST')
	`, chain)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[uuid.UUID]bool)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result[id] = true
	}
	return result, rows.Err()
}

// SetApprovedBy records who approved an operation
func (r *RebalanceRepo) SetApprovedBy(ctx context.Context, id uuid.UUID, approvedBy string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations SET approved_by = $1, updated_at = NOW() WHERE id = $2
	`, approvedBy, id)
	return err
}

// ExpireOperations transitions PENDING ops created before cutoff to REJECTED state.
// Returns the number of expired operations.
func (r *RebalanceRepo) ExpireOperations(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations
		SET state = 'REJECTED', error_message = 'expired: not acted on within TTL', updated_at = NOW()
		WHERE state = 'PENDING' AND created_at < $1
	`, cutoff)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// UpdateOperationState transitions a rebalance operation to a new state
func (r *RebalanceRepo) UpdateOperationState(ctx context.Context, id uuid.UUID, state domain.RebalanceState, errMsg *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations SET
			state = $1,
			error_message = COALESCE($2, error_message),
			updated_at = NOW()
		WHERE id = $3
	`, string(state), errMsg, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("rebalance operation %s not found", id)
	}
	return nil
}

// ListOperations returns rebalance operations with optional filters
func (r *RebalanceRepo) ListOperations(ctx context.Context, chain string, state string, limit int) ([]*domain.RebalanceOperation, error) {
	query := `
		SELECT id, chain, token_symbol, from_tier, to_tier, from_wallet_id, to_wallet_id,
			amount, tx_hash, state, requires_approval, approved_by, ledger_entry_id,
			error_message, created_at, updated_at
		FROM rebalance_operations WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if chain != "" {
		query += fmt.Sprintf(" AND chain = $%d", argIdx)
		args = append(args, chain)
		argIdx++
	}
	if state != "" {
		query += fmt.Sprintf(" AND state = $%d", argIdx)
		args = append(args, state)
		argIdx++
	}

	query += " ORDER BY created_at DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, limit)
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOperations(rows)
}

// GetOperation returns a single rebalance operation by ID
func (r *RebalanceRepo) GetOperation(ctx context.Context, id uuid.UUID) (*domain.RebalanceOperation, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, chain, token_symbol, from_tier, to_tier, from_wallet_id, to_wallet_id,
			amount, tx_hash, state, requires_approval, approved_by, ledger_entry_id,
			error_message, created_at, updated_at
		FROM rebalance_operations WHERE id = $1
	`, id)
	return scanOperation(row)
}

func scanPolicies(rows pgx.Rows) ([]*domain.RebalancePolicy, error) {
	var policies []*domain.RebalancePolicy
	for rows.Next() {
		p := &domain.RebalancePolicy{}
		var minAmountStr string
		err := rows.Scan(
			&p.ID, &p.Chain, &p.TokenSymbol, &p.HotTargetPct, &p.HotMaxPct, &p.HotMinPct,
			&p.WarmTargetPct, &p.WarmMaxPct, &minAmountStr, &p.AutoRebalance,
			&p.IsActive, &p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		p.MinRebalanceAmount = new(big.Int)
		p.MinRebalanceAmount.SetString(minAmountStr, 10)
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

func scanOperation(row pgx.Row) (*domain.RebalanceOperation, error) {
	op := &domain.RebalanceOperation{}
	var amountStr string
	err := row.Scan(
		&op.ID, &op.Chain, &op.TokenSymbol, &op.FromTier, &op.ToTier,
		&op.FromWalletID, &op.ToWalletID, &amountStr, &op.TxHash,
		&op.State, &op.RequiresApproval, &op.ApprovedBy, &op.LedgerEntryID,
		&op.ErrorMessage, &op.CreatedAt, &op.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	op.Amount = new(big.Int)
	op.Amount.SetString(amountStr, 10)
	return op, nil
}

func scanOperations(rows pgx.Rows) ([]*domain.RebalanceOperation, error) {
	var ops []*domain.RebalanceOperation
	for rows.Next() {
		op := &domain.RebalanceOperation{}
		var amountStr string
		err := rows.Scan(
			&op.ID, &op.Chain, &op.TokenSymbol, &op.FromTier, &op.ToTier,
			&op.FromWalletID, &op.ToWalletID, &amountStr, &op.TxHash,
			&op.State, &op.RequiresApproval, &op.ApprovedBy, &op.LedgerEntryID,
			&op.ErrorMessage, &op.CreatedAt, &op.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		op.Amount = new(big.Int)
		op.Amount.SetString(amountStr, 10)
		ops = append(ops, op)
	}
	return ops, rows.Err()
}
