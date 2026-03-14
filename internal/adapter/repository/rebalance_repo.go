package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// ErrStateConflict indicates a state transition failed because the current state
// did not match the expected from-state (TOCTOU guard).
var ErrStateConflict = fmt.Errorf("state conflict: current state does not match expected")

// Known rebalance states for input validation
var knownRebalanceStates = map[string]bool{
	"PENDING": true, "APPROVED": true, "SIGNING": true, "BROADCAST": true,
	"CONFIRMED": true, "FAILED": true, "REJECTED": true,
}

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
	var parseErr error
	p.MinRebalanceAmount, parseErr = parseBigInt(minAmountStr)
	if parseErr != nil {
		return nil, fmt.Errorf("parse min_rebalance_amount for %s/%s: %w", chain, token, parseErr)
	}
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

// FindActiveOps returns in-flight operations for a given chain/token/direction.
// Used for dedup — prevents creating duplicate ops for the same rebalance direction.
// NOTE: FAILED ops are excluded. If a FAILED op was actually broadcast (e.g., crash
// after broadcast but before recording tx hash), the caller must verify on-chain
// state before creating a replacement op.
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

// PendingSourceWalletIDs returns wallet IDs that are currently source of active operations.
// Used to exclude busy wallets from new rebalance ops.
// This is chain-scoped (not token-scoped) because two concurrent transactions from
// the same wallet would cause nonce conflicts regardless of token.
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

// ApproveOperation atomically transitions a PENDING operation to APPROVED with
// the approving principal recorded. Returns ErrStateConflict if the operation
// is no longer PENDING (TOCTOU guard).
func (r *RebalanceRepo) ApproveOperation(ctx context.Context, id uuid.UUID, approvedBy string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations
		SET state = 'APPROVED', approved_by = $1, updated_at = NOW()
		WHERE id = $2 AND state = 'PENDING'
	`, approvedBy, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("operation %s: %w (expected PENDING)", id, ErrStateConflict)
	}
	return nil
}

// ExpireOperations transitions stale operations to terminal states:
// - PENDING ops older than pendingCutoff → REJECTED
// - APPROVED ops older than approvedCutoff → FAILED (requires human review)
// Returns the total number of expired operations.
func (r *RebalanceRepo) ExpireOperations(ctx context.Context, pendingCutoff, approvedCutoff time.Time) (int, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations
		SET state = CASE
				WHEN state = 'PENDING' THEN 'REJECTED'
				WHEN state = 'APPROVED' THEN 'FAILED'
			END,
			error_message = CASE
				WHEN state = 'PENDING' THEN 'expired: not acted on within TTL'
				WHEN state = 'APPROVED' THEN 'expired: approved but never executed — requires review'
			END,
			updated_at = NOW()
		WHERE (state = 'PENDING' AND created_at < $1)
		   OR (state = 'APPROVED' AND created_at < $2)
	`, pendingCutoff, approvedCutoff)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// UpdateOperationState atomically transitions a rebalance operation to a new state.
// fromState is the expected current state — if the row's actual state differs, the
// update is rejected and ErrStateConflict is returned (prevents TOCTOU races).
func (r *RebalanceRepo) UpdateOperationState(ctx context.Context, id uuid.UUID, fromState, toState domain.RebalanceState, errMsg *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE rebalance_operations SET
			state = $1,
			error_message = COALESCE($2, error_message),
			updated_at = NOW()
		WHERE id = $3 AND state = $4
	`, string(toState), errMsg, id, string(fromState))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("operation %s: %w (expected %s)", id, ErrStateConflict, fromState)
	}
	return nil
}

// ListOperations returns rebalance operations with optional filters
func (r *RebalanceRepo) ListOperations(ctx context.Context, chain string, state string, limit, offset int) ([]*domain.RebalanceOperation, error) {
	if state != "" && !knownRebalanceStates[state] {
		return nil, fmt.Errorf("unknown rebalance state: %q", state)
	}

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
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)
	argIdx++
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
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
		var parseErr error
		p.MinRebalanceAmount, parseErr = parseBigInt(minAmountStr)
		if parseErr != nil {
			return nil, fmt.Errorf("parse min_rebalance_amount for %s/%s: %w", p.Chain, p.TokenSymbol, parseErr)
		}
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

// scanRebalanceOp scans a single rebalance operation row.
// Used by both scanOperation (QueryRow) and scanOperations (Rows).
func scanRebalanceOp(scanner interface {
	Scan(dest ...interface{}) error
}) (*domain.RebalanceOperation, error) {
	op := &domain.RebalanceOperation{}
	var amountStr string
	err := scanner.Scan(
		&op.ID, &op.Chain, &op.TokenSymbol, &op.FromTier, &op.ToTier,
		&op.FromWalletID, &op.ToWalletID, &amountStr, &op.TxHash,
		&op.State, &op.RequiresApproval, &op.ApprovedBy, &op.LedgerEntryID,
		&op.ErrorMessage, &op.CreatedAt, &op.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	var parseErr error
	op.Amount, parseErr = parseBigInt(amountStr)
	if parseErr != nil {
		return nil, fmt.Errorf("parse amount for rebalance op %s: %w", op.ID, parseErr)
	}
	return op, nil
}

func scanOperation(row pgx.Row) (*domain.RebalanceOperation, error) {
	return scanRebalanceOp(row)
}

func scanOperations(rows pgx.Rows) ([]*domain.RebalanceOperation, error) {
	var ops []*domain.RebalanceOperation
	for rows.Next() {
		op, err := scanRebalanceOp(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}
