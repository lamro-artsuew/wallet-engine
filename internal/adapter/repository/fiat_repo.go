package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// FiatRepo handles fiat account and transaction persistence
type FiatRepo struct {
	pool *pgxpool.Pool
}

func NewFiatRepo(pool *pgxpool.Pool) *FiatRepo {
	return &FiatRepo{pool: pool}
}

// CreateFiatAccount inserts a new fiat account
func (r *FiatRepo) CreateFiatAccount(ctx context.Context, account *domain.FiatAccount) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO fiat_accounts (id, workspace_id, currency, provider, provider_account_id, account_type, balance)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (workspace_id, currency, provider, account_type) DO NOTHING
	`, account.ID, account.WorkspaceID, account.Currency, account.Provider,
		account.ProviderAccountID, account.AccountType, account.Balance)
	return err
}

// GetFiatAccount returns a single fiat account by ID
func (r *FiatRepo) GetFiatAccount(ctx context.Context, id uuid.UUID) (*domain.FiatAccount, error) {
	a := &domain.FiatAccount{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance, is_active, created_at, updated_at
		FROM fiat_accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
		&a.AccountType, &a.Balance, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("fiat account %s not found: %w", id, err)
	}
	return a, nil
}

// ListFiatAccounts returns all fiat accounts for a workspace
func (r *FiatRepo) ListFiatAccounts(ctx context.Context, workspaceID uuid.UUID) ([]*domain.FiatAccount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance, is_active, created_at, updated_at
		FROM fiat_accounts WHERE workspace_id = $1 AND is_active = TRUE
		ORDER BY currency, account_type
	`, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.FiatAccount
	for rows.Next() {
		a := &domain.FiatAccount{}
		if err := rows.Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
			&a.AccountType, &a.Balance, &a.IsActive, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// UpdateBalance updates the balance of a fiat account
func (r *FiatRepo) UpdateBalance(ctx context.Context, id uuid.UUID, newBalance float64) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE fiat_accounts SET balance = $1, updated_at = NOW() WHERE id = $2
	`, newBalance, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("fiat account %s not found", id)
	}
	return nil
}

// CreateTransaction inserts a new fiat transaction
func (r *FiatRepo) CreateTransaction(ctx context.Context, tx *domain.FiatTransaction) error {
	metadataJSON, err := json.Marshal(tx.Metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO fiat_transactions (id, workspace_id, fiat_account_id, type, currency, amount,
			reference, counterparty, state, ledger_entry_id, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, tx.ID, tx.WorkspaceID, tx.FiatAccountID, string(tx.Type), tx.Currency, tx.Amount,
		tx.Reference, tx.Counterparty, string(tx.State), tx.LedgerEntryID, metadataJSON)
	return err
}

// UpdateTransactionState updates the state of a fiat transaction
func (r *FiatRepo) UpdateTransactionState(ctx context.Context, id uuid.UUID, state domain.FiatTransactionState) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE fiat_transactions SET state = $1, updated_at = NOW() WHERE id = $2
	`, string(state), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("fiat transaction %s not found", id)
	}
	return nil
}

// ListTransactions returns paginated fiat transactions for a workspace
func (r *FiatRepo) ListTransactions(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*domain.FiatTransaction, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, fiat_account_id, type, currency, amount,
			reference, counterparty, state, ledger_entry_id, metadata, created_at, updated_at
		FROM fiat_transactions WHERE workspace_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, workspaceID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []*domain.FiatTransaction
	for rows.Next() {
		t := &domain.FiatTransaction{}
		var metadataJSON []byte
		if err := rows.Scan(&t.ID, &t.WorkspaceID, &t.FiatAccountID, &t.Type, &t.Currency, &t.Amount,
			&t.Reference, &t.Counterparty, &t.State, &t.LedgerEntryID, &metadataJSON,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.Metadata = make(map[string]interface{})
		if len(metadataJSON) > 0 {
			json.Unmarshal(metadataJSON, &t.Metadata)
		}
		txs = append(txs, t)
	}
	return txs, rows.Err()
}

// GetLatestRate returns the most recent conversion rate for a currency pair
func (r *FiatRepo) GetLatestRate(ctx context.Context, from, to string) (*domain.ConversionRate, error) {
	rate := &domain.ConversionRate{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, from_currency, to_currency, rate, source, valid_from, valid_until, created_at
		FROM conversion_rates
		WHERE from_currency = $1 AND to_currency = $2
			AND valid_from <= NOW()
			AND (valid_until IS NULL OR valid_until > NOW())
		ORDER BY valid_from DESC LIMIT 1
	`, from, to).Scan(&rate.ID, &rate.FromCurrency, &rate.ToCurrency, &rate.Rate,
		&rate.Source, &rate.ValidFrom, &rate.ValidUntil, &rate.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("no rate found for %s/%s: %w", from, to, err)
	}
	return rate, nil
}

// SaveRate inserts a new conversion rate
func (r *FiatRepo) SaveRate(ctx context.Context, rate *domain.ConversionRate) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO conversion_rates (id, from_currency, to_currency, rate, source, valid_from, valid_until)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (from_currency, to_currency, valid_from) DO UPDATE SET
			rate = EXCLUDED.rate, source = EXCLUDED.source, valid_until = EXCLUDED.valid_until
	`, rate.ID, rate.FromCurrency, rate.ToCurrency, rate.Rate, rate.Source, rate.ValidFrom, rate.ValidUntil)
	return err
}

// FindOperatingAccount returns the first active fiat account for a workspace/currency,
// preferring OPERATING accounts
func (r *FiatRepo) FindOperatingAccount(ctx context.Context, workspaceID uuid.UUID, currency string) (*domain.FiatAccount, error) {
	a := &domain.FiatAccount{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance, is_active, created_at, updated_at
		FROM fiat_accounts
		WHERE workspace_id = $1 AND currency = $2 AND is_active = TRUE
		ORDER BY CASE account_type WHEN 'OPERATING' THEN 0 ELSE 1 END
		LIMIT 1
	`, workspaceID, currency).Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
		&a.AccountType, &a.Balance, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("no active fiat account for workspace %s currency %s: %w", workspaceID, currency, err)
	}
	return a, nil
}
