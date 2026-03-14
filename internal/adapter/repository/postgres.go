package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/lamro-artsuew/wallet-engine/internal/numeric"
)

// DepositRepo handles deposit persistence
type DepositRepo struct {
	pool *pgxpool.Pool
}

func NewDepositRepo(pool *pgxpool.Pool) *DepositRepo {
	return &DepositRepo{pool: pool}
}

// Upsert inserts or updates a deposit (idempotent on idempotency_key)
func (r *DepositRepo) Upsert(ctx context.Context, d *domain.Deposit) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO deposits (
			id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, is_from_blacklisted
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		ON CONFLICT (idempotency_key) DO UPDATE SET
			confirmations = EXCLUDED.confirmations,
			state = EXCLUDED.state,
			block_hash = EXCLUDED.block_hash,
			is_from_blacklisted = deposits.is_from_blacklisted OR EXCLUDED.is_from_blacklisted,
			updated_at = NOW()
	`,
		d.ID, d.IdempotencyKey, d.Chain, d.TxHash, d.LogIndex, d.BlockNumber, d.BlockHash,
		d.FromAddress, d.ToAddress, d.TokenAddress, d.TokenSymbol, d.Amount.String(), d.Decimals,
		string(d.State), d.Confirmations, d.RequiredConfs, d.DepositAddressID,
		d.WorkspaceID, d.UserID, d.DetectedAt, d.IsFromBlacklisted,
	)
	return err
}

// UpdateState transitions a deposit to a new state
func (r *DepositRepo) UpdateState(ctx context.Context, id uuid.UUID, state domain.DepositState, confirmations int) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE deposits SET state = $1, confirmations = $2, updated_at = NOW(),
			confirmed_at = CASE WHEN $1 = 'CONFIRMED' THEN NOW() ELSE confirmed_at END,
			swept_at = CASE WHEN $1 = 'SWEPT' THEN NOW() ELSE swept_at END
		WHERE id = $3
	`, string(state), confirmations, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("deposit %s not found", id)
	}
	return nil
}

// FindByChainAndState returns deposits matching chain and state
func (r *DepositRepo) FindByChainAndState(ctx context.Context, chain string, state domain.DepositState) ([]*domain.Deposit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, confirmed_at, swept_at, created_at, updated_at,
			is_from_blacklisted
		FROM deposits WHERE chain = $1 AND state = $2
		ORDER BY block_number ASC
	`, chain, string(state))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeposits(rows)
}

// FindByUser returns deposits for a user
func (r *DepositRepo) FindByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*domain.Deposit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, confirmed_at, swept_at, created_at, updated_at,
			is_from_blacklisted
		FROM deposits WHERE user_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeposits(rows)
}

// FindByWorkspace returns deposits for a workspace
func (r *DepositRepo) FindByWorkspace(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*domain.Deposit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, confirmed_at, swept_at, created_at, updated_at,
			is_from_blacklisted
		FROM deposits WHERE workspace_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, workspaceID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeposits(rows)
}

// FindAll returns all deposits with pagination (admin view)
func (r *DepositRepo) FindAll(ctx context.Context, limit, offset int) ([]*domain.Deposit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, confirmed_at, swept_at, created_at, updated_at,
			is_from_blacklisted
		FROM deposits
		ORDER BY created_at DESC LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeposits(rows)
}

// FindByID returns a single deposit by ID
func (r *DepositRepo) FindByID(ctx context.Context, id uuid.UUID) (*domain.Deposit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, chain, tx_hash, log_index, block_number, block_hash,
			from_address, to_address, token_address, token_symbol, amount, decimals,
			state, confirmations, required_confirmations, deposit_address_id,
			workspace_id, user_id, detected_at, confirmed_at, swept_at, created_at, updated_at,
			is_from_blacklisted
		FROM deposits WHERE id = $1
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deposits, err := scanDeposits(rows)
	if err != nil {
		return nil, err
	}
	if len(deposits) == 0 {
		return nil, fmt.Errorf("deposit %s not found", id)
	}
	return deposits[0], nil
}

func scanDeposits(rows pgx.Rows) ([]*domain.Deposit, error) {
	var deposits []*domain.Deposit
	for rows.Next() {
		d := &domain.Deposit{}
		var amountStr string
		var confirmedAt, sweptAt *time.Time
		err := rows.Scan(
			&d.ID, &d.IdempotencyKey, &d.Chain, &d.TxHash, &d.LogIndex, &d.BlockNumber, &d.BlockHash,
			&d.FromAddress, &d.ToAddress, &d.TokenAddress, &d.TokenSymbol, &amountStr, &d.Decimals,
			&d.State, &d.Confirmations, &d.RequiredConfs, &d.DepositAddressID,
			&d.WorkspaceID, &d.UserID, &d.DetectedAt, &confirmedAt, &sweptAt, &d.CreatedAt, &d.UpdatedAt,
			&d.IsFromBlacklisted,
		)
		if err != nil {
			return nil, err
		}
		d.Amount, err = numeric.ParseBigInt(amountStr)
		if err != nil {
			return nil, fmt.Errorf("parse amount for deposit %s: %w", d.ID, err)
		}
		d.ConfirmedAt = confirmedAt
		d.SweptAt = sweptAt
		deposits = append(deposits, d)
	}
	return deposits, rows.Err()
}

// SetBlacklistFlag updates the blacklist flag for a deposit.
// This belongs in DepositRepo (not BlacklistRepo) because it writes to the deposits table.
func (r *DepositRepo) SetBlacklistFlag(ctx context.Context, id uuid.UUID, isBlacklisted bool) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE deposits SET is_from_blacklisted = $1, updated_at = NOW() WHERE id = $2
	`, isBlacklisted, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("deposit %s not found", id)
	}
	return nil
}

// DepositAddressRepo handles deposit address persistence
type DepositAddressRepo struct {
	pool *pgxpool.Pool
}

func NewDepositAddressRepo(pool *pgxpool.Pool) *DepositAddressRepo {
	return &DepositAddressRepo{pool: pool}
}

// Create inserts a new deposit address
func (r *DepositAddressRepo) Create(ctx context.Context, da *domain.DepositAddress) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO deposit_addresses (id, workspace_id, user_id, chain, address, derivation_path, derivation_index)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, da.ID, da.WorkspaceID, da.UserID, da.Chain, da.Address, da.DerivationPath, da.DerivationIdx)
	return err
}

// FindByChain returns all active deposit addresses for a chain
func (r *DepositAddressRepo) FindByChain(ctx context.Context, chain string) ([]*domain.DepositAddress, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, user_id, chain, address, derivation_path, derivation_index, is_active, created_at
		FROM deposit_addresses WHERE chain = $1 AND is_active = TRUE
	`, chain)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addrs []*domain.DepositAddress
	for rows.Next() {
		a := &domain.DepositAddress{}
		if err := rows.Scan(&a.ID, &a.WorkspaceID, &a.UserID, &a.Chain, &a.Address, &a.DerivationPath, &a.DerivationIdx, &a.IsActive, &a.CreatedAt); err != nil {
			return nil, err
		}
		addrs = append(addrs, a)
	}
	return addrs, rows.Err()
}

// FindByUserAndChain returns a user's deposit address for a specific chain
func (r *DepositAddressRepo) FindByUserAndChain(ctx context.Context, userID uuid.UUID, chain string) (*domain.DepositAddress, error) {
	a := &domain.DepositAddress{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, user_id, chain, address, derivation_path, derivation_index, is_active, created_at
		FROM deposit_addresses WHERE user_id = $1 AND chain = $2 AND is_active = TRUE
		LIMIT 1
	`, userID, chain).Scan(&a.ID, &a.WorkspaceID, &a.UserID, &a.Chain, &a.Address, &a.DerivationPath, &a.DerivationIdx, &a.IsActive, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// ChainIndexRepo manages per-chain block cursor
type ChainIndexRepo struct {
	pool *pgxpool.Pool
}

func NewChainIndexRepo(pool *pgxpool.Pool) *ChainIndexRepo {
	return &ChainIndexRepo{pool: pool}
}

// GetState returns the current index state for a chain
func (r *ChainIndexRepo) GetState(ctx context.Context, chain string) (*domain.ChainIndexState, error) {
	s := &domain.ChainIndexState{}
	err := r.pool.QueryRow(ctx, `
		SELECT chain, last_block_number, last_block_hash, updated_at
		FROM chain_index_state WHERE chain = $1
	`, chain).Scan(&s.Chain, &s.LastBlockNumber, &s.LastBlockHash, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// UpsertState updates or creates the chain index cursor
func (r *ChainIndexRepo) UpsertState(ctx context.Context, chain string, blockNum int64, blockHash string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO chain_index_state (chain, last_block_number, last_block_hash, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (chain) DO UPDATE SET
			last_block_number = EXCLUDED.last_block_number,
			last_block_hash = EXCLUDED.last_block_hash,
			updated_at = NOW()
	`, chain, blockNum, blockHash)
	return err
}

// WalletRepo handles managed wallet persistence
type WalletRepo struct {
	pool *pgxpool.Pool
}

func NewWalletRepo(pool *pgxpool.Pool) *WalletRepo {
	return &WalletRepo{pool: pool}
}

// FindByChainAndTier returns wallets by chain and tier
func (r *WalletRepo) FindByChainAndTier(ctx context.Context, chain string, tier domain.WalletTier) ([]*domain.Wallet, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, tier, label, is_active, created_at, updated_at
		FROM wallets WHERE chain = $1 AND tier = $2 AND is_active = TRUE
	`, chain, string(tier))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wallets []*domain.Wallet
	for rows.Next() {
		w := &domain.Wallet{}
		if err := rows.Scan(&w.ID, &w.Chain, &w.Address, &w.Tier, &w.Label, &w.IsActive, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, err
		}
		wallets = append(wallets, w)
	}
	return wallets, rows.Err()
}

// FindAll returns all active wallets
func (r *WalletRepo) FindAll(ctx context.Context) ([]*domain.Wallet, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, tier, label, is_active, created_at, updated_at
		FROM wallets WHERE is_active = TRUE ORDER BY chain, tier
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wallets []*domain.Wallet
	for rows.Next() {
		w := &domain.Wallet{}
		if err := rows.Scan(&w.ID, &w.Chain, &w.Address, &w.Tier, &w.Label, &w.IsActive, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, err
		}
		wallets = append(wallets, w)
	}
	return wallets, rows.Err()
}

// FindByID returns a single wallet by ID
func (r *WalletRepo) FindByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w := &domain.Wallet{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, chain, address, tier, label, encrypted_key, is_active, created_at, updated_at
		FROM wallets WHERE id = $1
	`, id).Scan(&w.ID, &w.Chain, &w.Address, &w.Tier, &w.Label, &w.EncryptedKey, &w.IsActive, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("wallet %s not found", id)
	}
	return w, nil
}

// Create inserts a new wallet
func (r *WalletRepo) Create(ctx context.Context, w *domain.Wallet) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO wallets (id, chain, address, tier, label, is_active)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, w.ID, w.Chain, w.Address, string(w.Tier), w.Label, w.IsActive)
	return err
}

// Update modifies a wallet's label and tier
func (r *WalletRepo) Update(ctx context.Context, id uuid.UUID, label string, tier domain.WalletTier) error {
	res, err := r.pool.Exec(ctx, `
		UPDATE wallets SET label = $2, tier = $3, updated_at = NOW()
		WHERE id = $1 AND is_active = TRUE
	`, id, label, string(tier))
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("wallet %s not found", id)
	}
	return nil
}

// Deactivate soft-deletes a wallet
func (r *WalletRepo) Deactivate(ctx context.Context, id uuid.UUID) error {
	res, err := r.pool.Exec(ctx, `
		UPDATE wallets SET is_active = FALSE, updated_at = NOW()
		WHERE id = $1 AND is_active = TRUE
	`, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("wallet %s not found", id)
	}
	return nil
}

// FindByChain returns all active wallets for a chain
func (r *WalletRepo) FindByChain(ctx context.Context, chain string) ([]*domain.Wallet, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chain, address, tier, label, is_active, created_at, updated_at
		FROM wallets WHERE chain = $1 AND is_active = TRUE ORDER BY tier
	`, chain)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wallets []*domain.Wallet
	for rows.Next() {
		w := &domain.Wallet{}
		if err := rows.Scan(&w.ID, &w.Chain, &w.Address, &w.Tier, &w.Label, &w.IsActive, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, err
		}
		wallets = append(wallets, w)
	}
	return wallets, rows.Err()
}

// FindAllAddresses returns all active deposit addresses with optional filters
func (r *DepositAddressRepo) FindAll(ctx context.Context, chain, workspaceID string, limit, offset int) ([]*domain.DepositAddress, error) {
	query := `SELECT id, workspace_id, user_id, chain, address, derivation_path, derivation_index, is_active, created_at
		FROM deposit_addresses WHERE is_active = TRUE`
	args := []interface{}{}
	argIdx := 1

	if chain != "" {
		query += fmt.Sprintf(" AND chain = $%d", argIdx)
		args = append(args, chain)
		argIdx++
	}
	if workspaceID != "" {
		wsID, err := uuid.Parse(workspaceID)
		if err == nil {
			query += fmt.Sprintf(" AND workspace_id = $%d", argIdx)
			args = append(args, wsID)
			argIdx++
		}
	}
	query += " ORDER BY created_at DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, limit)
		argIdx++
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
		argIdx++
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addrs []*domain.DepositAddress
	for rows.Next() {
		a := &domain.DepositAddress{}
		if err := rows.Scan(&a.ID, &a.WorkspaceID, &a.UserID, &a.Chain, &a.Address, &a.DerivationPath, &a.DerivationIdx, &a.IsActive, &a.CreatedAt); err != nil {
			return nil, err
		}
		addrs = append(addrs, a)
	}
	return addrs, rows.Err()
}

// HDDerivationRepo manages HD derivation state
type HDDerivationRepo struct {
	pool *pgxpool.Pool
}

func NewHDDerivationRepo(pool *pgxpool.Pool) *HDDerivationRepo {
	return &HDDerivationRepo{pool: pool}
}

// GetAndIncrementIndex atomically gets the next index and increments it
func (r *HDDerivationRepo) GetAndIncrementIndex(ctx context.Context, chain string) (int64, error) {
	var idx int64
	err := r.pool.QueryRow(ctx, `
		UPDATE hd_derivation_state 
		SET next_index = next_index + 1, updated_at = NOW()
		WHERE chain = $1
		RETURNING next_index - 1
	`, chain).Scan(&idx)
	if err != nil {
		// Initialize if not exists
		err = r.pool.QueryRow(ctx, `
			INSERT INTO hd_derivation_state (chain, master_seed_id, next_index)
			VALUES ($1, 'vault:wallet-engine/master-seed', 1)
			ON CONFLICT (chain) DO UPDATE SET next_index = hd_derivation_state.next_index + 1, updated_at = NOW()
			RETURNING next_index - 1
		`, chain).Scan(&idx)
	}
	return idx, err
}
