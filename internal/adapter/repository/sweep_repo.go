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
	"github.com/lamro-artsuew/wallet-engine/internal/numeric"
)

// SweepRepo handles sweep record persistence
type SweepRepo struct {
	pool *pgxpool.Pool
}

func NewSweepRepo(pool *pgxpool.Pool) *SweepRepo {
	return &SweepRepo{pool: pool}
}

// Insert creates a new sweep record
func (r *SweepRepo) Insert(ctx context.Context, s *domain.SweepRecord) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO sweeps (id, chain, from_address, to_address, token_address, amount, tx_hash, gas_cost, state, deposit_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, s.ID, s.Chain, s.FromAddress, s.ToAddress, s.TokenAddress,
		s.Amount.String(), s.TxHash, bigIntStr(s.GasCost), s.State, s.DepositID)
	return err
}

// UpdateState transitions a sweep to a new state
func (r *SweepRepo) UpdateState(ctx context.Context, id uuid.UUID, state string, txHash string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE sweeps SET state = $1, tx_hash = COALESCE(NULLIF($2, ''), tx_hash),
			confirmed_at = CASE WHEN $1 = 'CONFIRMED' THEN NOW() ELSE confirmed_at END,
			updated_at = NOW()
		WHERE id = $3
	`, state, txHash, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("sweep %s not found", id)
	}
	return nil
}

// UpdateGasCost sets the actual gas cost after confirmation
func (r *SweepRepo) UpdateGasCost(ctx context.Context, id uuid.UUID, gasCost string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE sweeps SET gas_cost = $1, updated_at = NOW() WHERE id = $2
	`, gasCost, id)
	return err
}

// FindByState returns sweeps in a given state for a chain
func (r *SweepRepo) FindByState(ctx context.Context, chain, state string) ([]*domain.SweepRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, from_address, to_address, token_address, amount, tx_hash, gas_cost, state, deposit_id, created_at, confirmed_at
		FROM sweeps WHERE chain = $1 AND state = $2
		ORDER BY created_at ASC
	`, chain, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSweeps(rows)
}

// FindByDepositID returns the sweep for a deposit
func (r *SweepRepo) FindByDepositID(ctx context.Context, depositID uuid.UUID) (*domain.SweepRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, from_address, to_address, token_address, amount, tx_hash, gas_cost, state, deposit_id, created_at, confirmed_at
		FROM sweeps WHERE deposit_id = $1
		ORDER BY created_at DESC LIMIT 1
	`, depositID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sweeps, err := scanSweeps(rows)
	if err != nil {
		return nil, err
	}
	if len(sweeps) == 0 {
		return nil, fmt.Errorf("no sweep found for deposit %s", depositID)
	}
	return sweeps[0], nil
}

// FindPendingAll returns all pending sweeps across all chains
func (r *SweepRepo) FindPendingAll(ctx context.Context) ([]*domain.SweepRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, from_address, to_address, token_address, amount, tx_hash, gas_cost, state, deposit_id, created_at, confirmed_at
		FROM sweeps WHERE state IN ('PENDING', 'BROADCAST')
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSweeps(rows)
}

// FindBroadcastStale returns broadcast sweeps older than the given duration (stuck TXs)
func (r *SweepRepo) FindBroadcastStale(ctx context.Context, chain string, olderThan time.Duration) ([]*domain.SweepRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, from_address, to_address, token_address, amount, tx_hash, gas_cost, state, deposit_id, created_at, confirmed_at
		FROM sweeps WHERE chain = $1 AND state = 'BROADCAST' AND created_at < NOW() - $2::interval
		ORDER BY created_at ASC
	`, chain, fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSweeps(rows)
}

func scanSweeps(rows pgx.Rows) ([]*domain.SweepRecord, error) {
	var sweeps []*domain.SweepRecord
	for rows.Next() {
		s := &domain.SweepRecord{}
		var amountStr, gasCostStr string
		err := rows.Scan(
			&s.ID, &s.Chain, &s.FromAddress, &s.ToAddress, &s.TokenAddress,
			&amountStr, &s.TxHash, &gasCostStr, &s.State, &s.DepositID,
			&s.CreatedAt, &s.ConfirmedAt,
		)
		if err != nil {
			return nil, err
		}
		s.Amount, err = numeric.ParseBigInt(amountStr)
		if err != nil {
			return nil, fmt.Errorf("parse sweep amount: %w", err)
		}
		if gasCostStr != "" {
			s.GasCost, _ = numeric.ParseBigInt(gasCostStr)
		}
		sweeps = append(sweeps, s)
	}
	return sweeps, rows.Err()
}

func bigIntStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}
