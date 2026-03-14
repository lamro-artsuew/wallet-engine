package repository

import (
	"context"
	"fmt"
	"strings"
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

// IsBlacklisted checks if an address is actively blacklisted and returns matching entries.
// Addresses are stored in normalized lowercase form; query also lowercases for safety.
func (r *BlacklistRepo) IsBlacklisted(ctx context.Context, chain string, address string) ([]*domain.BlacklistedAddress, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, source, reason, detected_at, is_active, created_at, updated_at
		FROM blacklisted_addresses
		WHERE chain = $1 AND address = lower($2) AND is_active = TRUE
	`, chain, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.BlacklistedAddress
	for rows.Next() {
		e, err := scanBlacklistEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// CountActiveBlacklisted returns the count of active blacklisted entries.
// Used by the Prometheus gauge to reflect actual DB state on each scrape.
func (r *BlacklistRepo) CountActiveBlacklisted(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM blacklisted_addresses WHERE is_active = TRUE`).Scan(&count)
	return count, err
}

// AddToBlacklist inserts a blacklist entry. If the address already exists for this
// chain+source, reactivates it and updates the reason (handles re-blacklisting after removal).
// Address is normalized to lowercase at insert time.
func (r *BlacklistRepo) AddToBlacklist(ctx context.Context, entry *domain.BlacklistedAddress) error {
	entry.Address = strings.ToLower(entry.Address)
	err := r.pool.QueryRow(ctx, `
		INSERT INTO blacklisted_addresses (id, chain, address, source, reason, detected_at, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (chain, address, source) DO UPDATE SET
			is_active = TRUE,
			reason = EXCLUDED.reason,
			detected_at = EXCLUDED.detected_at,
			updated_at = NOW()
		RETURNING id
	`, entry.ID, entry.Chain, entry.Address, entry.Source, entry.Reason, entry.DetectedAt, entry.IsActive).Scan(&entry.ID)
	return err
}

// RemoveFromBlacklist soft-deletes a blacklist entry by ID.
// Returns error if the entry does not exist (audit requirement).
func (r *BlacklistRepo) RemoveFromBlacklist(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE blacklisted_addresses SET is_active = FALSE, updated_at = NOW() WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("blacklisted address %s not found", id)
	}
	return nil
}

// scanBlacklistEntry scans a single blacklisted address row (DRY helper)
func scanBlacklistEntry(scanner interface{ Scan(dest ...interface{}) error }) (*domain.BlacklistedAddress, error) {
	e := &domain.BlacklistedAddress{}
	if err := scanner.Scan(&e.ID, &e.Chain, &e.Address, &e.Source, &e.Reason,
		&e.DetectedAt, &e.IsActive, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return nil, err
	}
	return e, nil
}

// ListBlacklisted returns paginated active blacklist entries, optionally filtered by chain.
// Uses unified query with optional chain filter to avoid DRY violation.
func (r *BlacklistRepo) ListBlacklisted(ctx context.Context, chain string, limit, offset int) ([]*domain.BlacklistedAddress, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, source, reason, detected_at, is_active, created_at, updated_at
		FROM blacklisted_addresses
		WHERE is_active = TRUE AND ($1::text = '' OR chain = $1)
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, chain, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.BlacklistedAddress
	for rows.Next() {
		e, err := scanBlacklistEntry(rows)
		if err != nil {
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
		ORDER BY symbol
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

// LogFreezeEvent records a freeze/blacklist event for audit trail.
// Uses the event's ID and DetectedAt if set; falls back to generated values.
func (r *BlacklistRepo) LogFreezeEvent(ctx context.Context, event *domain.FreezeEvent) error {
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.DetectedAt.IsZero() {
		event.DetectedAt = time.Now()
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO freeze_events (id, chain, address, contract_address, event_type, tx_hash, block_number, detected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, event.ID, event.Chain, strings.ToLower(event.Address), event.ContractAddress,
		event.EventType, event.TxHash, event.BlockNumber, event.DetectedAt)
	return err
}
