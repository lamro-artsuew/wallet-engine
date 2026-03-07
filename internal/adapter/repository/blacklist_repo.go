package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// BlacklistRepo handles blacklist and stablecoin contract persistence
type BlacklistRepo struct {
	pool *pgxpool.Pool
}

func NewBlacklistRepo(pool *pgxpool.Pool) *BlacklistRepo {
	return &BlacklistRepo{pool: pool}
}

// IsBlacklisted returns active blacklist entries for a chain+address (case-insensitive)
func (r *BlacklistRepo) IsBlacklisted(ctx context.Context, chain string, address string) ([]*domain.BlacklistedAddress, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, source, reason, detected_at, is_active, created_at
		FROM blacklisted_addresses
		WHERE chain = $1 AND lower(address) = lower($2) AND is_active = TRUE
	`, chain, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.BlacklistedAddress
	for rows.Next() {
		e := &domain.BlacklistedAddress{}
		if err := rows.Scan(&e.ID, &e.Chain, &e.Address, &e.Source, &e.Reason, &e.DetectedAt, &e.IsActive, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// AddToBlacklist inserts a blacklist entry (idempotent via ON CONFLICT)
func (r *BlacklistRepo) AddToBlacklist(ctx context.Context, entry *domain.BlacklistedAddress) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.DetectedAt.IsZero() {
		entry.DetectedAt = time.Now()
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO blacklisted_addresses (id, chain, address, source, reason, detected_at, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (chain, address, source) DO NOTHING
	`, entry.ID, entry.Chain, entry.Address, entry.Source, entry.Reason, entry.DetectedAt, entry.IsActive)
	return err
}

// RemoveFromBlacklist soft-deletes a blacklist entry by ID
func (r *BlacklistRepo) RemoveFromBlacklist(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE blacklisted_addresses SET is_active = FALSE WHERE id = $1
	`, id)
	return err
}

// ListBlacklisted returns paginated active blacklist entries, optionally filtered by chain
func (r *BlacklistRepo) ListBlacklisted(ctx context.Context, chain string, limit, offset int) ([]*domain.BlacklistedAddress, error) {
	var query string
	var args []interface{}
	if chain != "" {
		query = `
			SELECT id, chain, address, source, reason, detected_at, is_active, created_at
			FROM blacklisted_addresses WHERE chain = $1 AND is_active = TRUE
			ORDER BY created_at DESC LIMIT $2 OFFSET $3`
		args = []interface{}{chain, limit, offset}
	} else {
		query = `
			SELECT id, chain, address, source, reason, detected_at, is_active, created_at
			FROM blacklisted_addresses WHERE is_active = TRUE
			ORDER BY created_at DESC LIMIT $1 OFFSET $2`
		args = []interface{}{limit, offset}
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.BlacklistedAddress
	for rows.Next() {
		e := &domain.BlacklistedAddress{}
		if err := rows.Scan(&e.ID, &e.Chain, &e.Address, &e.Source, &e.Reason, &e.DetectedAt, &e.IsActive, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetStablecoinContracts returns active stablecoin contracts for a chain
func (r *BlacklistRepo) GetStablecoinContracts(ctx context.Context, chain string) ([]*domain.StablecoinContract, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, contract_address, symbol, issuer, has_blacklist, has_freeze,
			blacklist_method, freeze_method, decimals, is_active, created_at
		FROM stablecoin_contracts WHERE chain = $1 AND is_active = TRUE
	`, chain)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStablecoinContracts(rows)
}

// GetAllStablecoinContracts returns all active stablecoin contracts
func (r *BlacklistRepo) GetAllStablecoinContracts(ctx context.Context) ([]*domain.StablecoinContract, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, contract_address, symbol, issuer, has_blacklist, has_freeze,
			blacklist_method, freeze_method, decimals, is_active, created_at
		FROM stablecoin_contracts WHERE is_active = TRUE
		ORDER BY chain, symbol
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStablecoinContracts(rows)
}

func scanStablecoinContracts(rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}) ([]*domain.StablecoinContract, error) {
	var contracts []*domain.StablecoinContract
	for rows.Next() {
		c := &domain.StablecoinContract{}
		if err := rows.Scan(
			&c.ID, &c.Chain, &c.ContractAddress, &c.Symbol, &c.Issuer,
			&c.HasBlacklist, &c.HasFreeze, &c.BlacklistMethod, &c.FreezeMethod,
			&c.Decimals, &c.IsActive, &c.CreatedAt,
		); err != nil {
			return nil, err
		}
		contracts = append(contracts, c)
	}
	return contracts, rows.Err()
}

// LogFreezeEvent records a freeze/blacklist event
func (r *BlacklistRepo) LogFreezeEvent(ctx context.Context, event *domain.FreezeEvent) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO freeze_events (id, chain, address, contract_address, event_type, tx_hash, block_number, detected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
	`, uuid.New(), event.Chain, event.Address, event.ContractAddress, event.EventType, event.TxHash, event.BlockNumber)
	return err
}

// SetDepositBlacklistFlag updates the blacklist flag for a deposit
func (r *BlacklistRepo) SetDepositBlacklistFlag(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, isBlacklisted bool) error {
	_, err := pool.Exec(ctx, `
		UPDATE deposits SET is_from_blacklisted = $1, updated_at = NOW() WHERE id = $2
	`, isBlacklisted, id)
	return err
}
