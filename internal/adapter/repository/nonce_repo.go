package repository

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NonceReservation represents a reserved nonce for a chain/address pair
type NonceReservation struct {
	ID          uuid.UUID `json:"id" db:"id"`
	Chain       string    `json:"chain" db:"chain"`
	Address     string    `json:"address" db:"address"`
	Nonce       int64     `json:"nonce" db:"nonce"`
	Purpose     string    `json:"purpose" db:"purpose"`
	ReferenceID *uuid.UUID `json:"reference_id,omitempty" db:"reference_id"`
	TxHash      *string   `json:"tx_hash,omitempty" db:"tx_hash"`
	State       string    `json:"state" db:"state"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// NonceRepo manages nonce reservations
type NonceRepo struct {
	pool *pgxpool.Pool
}

func NewNonceRepo(pool *pgxpool.Pool) *NonceRepo {
	return &NonceRepo{pool: pool}
}

// ReserveNext atomically reserves the next available nonce for a chain/address.
// It takes the max of (on-chain pending nonce, max reserved nonce + 1).
func (r *NonceRepo) ReserveNext(ctx context.Context, chain, address string, onChainNonce int64, purpose string, refID *uuid.UUID) (*NonceReservation, error) {
	id := uuid.New()
	var nonce int64

	err := r.pool.QueryRow(ctx, `
		WITH max_reserved AS (
			SELECT COALESCE(MAX(nonce), $3 - 1) AS max_nonce
			FROM nonce_reservations
			WHERE chain = $1 AND address = $2 AND state IN ('RESERVED', 'BROADCAST')
		)
		INSERT INTO nonce_reservations (id, chain, address, nonce, purpose, reference_id, state)
		SELECT $4, $1, $2, GREATEST(mr.max_nonce + 1, $3), $5, $6, 'RESERVED'
		FROM max_reserved mr
		RETURNING nonce
	`, chain, address, onChainNonce, id, purpose, refID).Scan(&nonce)
	if err != nil {
		return nil, fmt.Errorf("reserve nonce for %s on %s: %w", address, chain, err)
	}

	return &NonceReservation{
		ID:          id,
		Chain:       chain,
		Address:     address,
		Nonce:       nonce,
		Purpose:     purpose,
		ReferenceID: refID,
		State:       "RESERVED",
	}, nil
}

// UpdateState transitions a nonce reservation state
func (r *NonceRepo) UpdateState(ctx context.Context, id uuid.UUID, state string, txHash string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE nonce_reservations SET state = $1, tx_hash = NULLIF($2, ''), updated_at = NOW()
		WHERE id = $3
	`, state, txHash, id)
	return err
}

// ExpireStale marks old RESERVED nonces as EXPIRED (cleanup for crashed processes)
func (r *NonceRepo) ExpireStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE nonce_reservations SET state = 'EXPIRED', updated_at = NOW()
		WHERE state = 'RESERVED' AND created_at < NOW() - $1::interval
	`, fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// GetMaxReservedNonce returns the highest reserved/broadcast nonce for a chain/address
func (r *NonceRepo) GetMaxReservedNonce(ctx context.Context, chain, address string) (int64, error) {
	var nonce int64
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(nonce), -1)
		FROM nonce_reservations
		WHERE chain = $1 AND address = $2 AND state IN ('RESERVED', 'BROADCAST')
	`, chain, address).Scan(&nonce)
	return nonce, err
}

// Stub for big.Int import
var _ = new(big.Int)
