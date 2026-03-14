package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReorgEvent represents a detected chain reorganization
type ReorgEvent struct {
	ID                uuid.UUID  `json:"id" db:"id"`
	Chain             string     `json:"chain" db:"chain"`
	DetectedAtBlock   int64      `json:"detected_at_block" db:"detected_at_block"`
	ForkBlock         int64      `json:"fork_block" db:"fork_block"`
	Depth             int        `json:"depth" db:"depth"`
	OldHash           string     `json:"old_hash" db:"old_hash"`
	NewHash           string     `json:"new_hash" db:"new_hash"`
	AffectedDeposits  int        `json:"affected_deposits" db:"affected_deposits"`
	Resolved          bool       `json:"resolved" db:"resolved"`
	CreatedAt         time.Time  `json:"created_at" db:"created_at"`
	ResolvedAt        *time.Time `json:"resolved_at,omitempty" db:"resolved_at"`
}

// ReorgRepo manages reorg event tracking and block hash history
type ReorgRepo struct {
	pool *pgxpool.Pool
}

func NewReorgRepo(pool *pgxpool.Pool) *ReorgRepo {
	return &ReorgRepo{pool: pool}
}

// InsertReorgEvent records a detected reorg
func (r *ReorgRepo) InsertReorgEvent(ctx context.Context, e *ReorgEvent) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO reorg_events (id, chain, detected_at_block, fork_block, depth, old_hash, new_hash, affected_deposits)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, e.ID, e.Chain, e.DetectedAtBlock, e.ForkBlock, e.Depth, e.OldHash, e.NewHash, e.AffectedDeposits)
	return err
}

// ResolveReorgEvent marks a reorg event as resolved
func (r *ReorgRepo) ResolveReorgEvent(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE reorg_events SET resolved = TRUE, resolved_at = NOW() WHERE id = $1
	`, id)
	return err
}

// FindUnresolved returns all unresolved reorg events for a chain
func (r *ReorgRepo) FindUnresolved(ctx context.Context, chain string) ([]*ReorgEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, detected_at_block, fork_block, depth, old_hash, new_hash, affected_deposits, resolved, created_at, resolved_at
		FROM reorg_events WHERE chain = $1 AND resolved = FALSE
		ORDER BY created_at DESC
	`, chain)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*ReorgEvent
	for rows.Next() {
		e := &ReorgEvent{}
		if err := rows.Scan(&e.ID, &e.Chain, &e.DetectedAtBlock, &e.ForkBlock, &e.Depth, &e.OldHash, &e.NewHash,
			&e.AffectedDeposits, &e.Resolved, &e.CreatedAt, &e.ResolvedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// SaveBlockHash stores a block hash for reorg detection
func (r *ReorgRepo) SaveBlockHash(ctx context.Context, chain string, blockNum int64, blockHash string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO block_hash_history (chain, block_number, block_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (chain, block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash
	`, chain, blockNum, blockHash)
	return err
}

// GetBlockHash retrieves a stored block hash
func (r *ReorgRepo) GetBlockHash(ctx context.Context, chain string, blockNum int64) (string, error) {
	var hash string
	err := r.pool.QueryRow(ctx, `
		SELECT block_hash FROM block_hash_history WHERE chain = $1 AND block_number = $2
	`, chain, blockNum).Scan(&hash)
	if err != nil {
		return "", err
	}
	return hash, nil
}

// PruneOldHashes removes block hashes older than maxBlocks behind current
func (r *ReorgRepo) PruneOldHashes(ctx context.Context, chain string, keepLatest int64) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM block_hash_history
		WHERE chain = $1 AND block_number < (
			SELECT COALESCE(MAX(block_number), 0) - $2 FROM block_hash_history WHERE chain = $1
		)
	`, chain, keepLatest)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ReconciliationRun records a reconciliation check
type ReconciliationRun struct {
	ID             uuid.UUID `json:"id" db:"id"`
	Chain          string    `json:"chain" db:"chain"`
	RunType        string    `json:"run_type" db:"run_type"`
	OnChainBalance string    `json:"on_chain_balance" db:"on_chain_balance"`
	LedgerBalance  string    `json:"ledger_balance" db:"ledger_balance"`
	Drift          string    `json:"drift" db:"drift"`
	DriftPct       float64   `json:"drift_pct" db:"drift_pct"`
	Status         string    `json:"status" db:"status"`
	Details        string    `json:"details" db:"details"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
}

// ReconciliationRepo manages reconciliation audit trail
type ReconciliationRepo struct {
	pool *pgxpool.Pool
}

func NewReconciliationRepo(pool *pgxpool.Pool) *ReconciliationRepo {
	return &ReconciliationRepo{pool: pool}
}

// Insert stores a reconciliation run result
func (r *ReconciliationRepo) Insert(ctx context.Context, run *ReconciliationRun) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO reconciliation_runs (id, chain, run_type, on_chain_balance, ledger_balance, drift, drift_pct, status, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, run.ID, run.Chain, run.RunType, run.OnChainBalance, run.LedgerBalance,
		run.Drift, run.DriftPct, run.Status, run.Details)
	return err
}

// FindLatest returns the most recent reconciliation runs
func (r *ReconciliationRepo) FindLatest(ctx context.Context, chain string, limit int) ([]*ReconciliationRun, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, run_type, on_chain_balance, ledger_balance, drift, drift_pct, status, details, created_at
		FROM reconciliation_runs WHERE chain = $1
		ORDER BY created_at DESC LIMIT $2
	`, chain, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*ReconciliationRun
	for rows.Next() {
		run := &ReconciliationRun{}
		if err := rows.Scan(&run.ID, &run.Chain, &run.RunType, &run.OnChainBalance, &run.LedgerBalance,
			&run.Drift, &run.DriftPct, &run.Status, &run.Details, &run.CreatedAt); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// CountDriftEvents returns count of DRIFT/FAIL events in a time window
func (r *ReconciliationRepo) CountDriftEvents(ctx context.Context, chain string, since time.Duration) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM reconciliation_runs
		WHERE chain = $1 AND status IN ('DRIFT', 'FAIL')
		AND created_at > NOW() - $2::interval
	`, chain, fmt.Sprintf("%d seconds", int(since.Seconds()))).Scan(&count)
	return count, err
}
