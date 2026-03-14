package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/shopspring/decimal"
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
		account.ProviderAccountID, account.AccountType, account.Balance.String())
	return err
}

// GetFiatAccount returns a single fiat account by ID
func (r *FiatRepo) GetFiatAccount(ctx context.Context, id uuid.UUID) (*domain.FiatAccount, error) {
	a := &domain.FiatAccount{}
	var balStr string
	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance::TEXT, is_active, created_at, updated_at
		FROM fiat_accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
		&a.AccountType, &balStr, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("fiat account %s not found: %w", id, err)
	}
	a.Balance, err = decimal.NewFromString(balStr)
	if err != nil {
		return nil, fmt.Errorf("parse balance for fiat account %s: %w", id, err)
	}
	return a, nil
}

// scanFiatAccount scans a fiat account row (shared by list queries)
func scanFiatAccount(scanner interface{ Scan(dest ...interface{}) error }) (*domain.FiatAccount, error) {
	a := &domain.FiatAccount{}
	var balStr string
	if err := scanner.Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
		&a.AccountType, &balStr, &a.IsActive, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	var err error
	a.Balance, err = decimal.NewFromString(balStr)
	if err != nil {
		return nil, fmt.Errorf("parse balance for fiat account %s: %w", a.ID, err)
	}
	return a, nil
}

// ListFiatAccounts returns all fiat accounts for a workspace
func (r *FiatRepo) ListFiatAccounts(ctx context.Context, workspaceID uuid.UUID) ([]*domain.FiatAccount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance::TEXT, is_active, created_at, updated_at
		FROM fiat_accounts WHERE workspace_id = $1 AND is_active = TRUE
		ORDER BY currency, account_type
	`, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.FiatAccount
	for rows.Next() {
		a, err := scanFiatAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// ListAllFiatAccounts returns all active fiat accounts across workspaces (admin view)
func (r *FiatRepo) ListAllFiatAccounts(ctx context.Context) ([]*domain.FiatAccount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance::TEXT, is_active, created_at, updated_at
		FROM fiat_accounts WHERE is_active = TRUE
		ORDER BY workspace_id, currency, account_type
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.FiatAccount
	for rows.Next() {
		a, err := scanFiatAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// UpdateBalance updates the balance of a fiat account using decimal precision.
// NOTE: This is a denormalized cache of the authoritative ledger balance. In a future
// refactor, this field should be removed and balance derived exclusively from ledger
// entries via SUM. For now, this must succeed — failures indicate data inconsistency.
func (r *FiatRepo) UpdateBalance(ctx context.Context, id uuid.UUID, newBalance decimal.Decimal) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE fiat_accounts SET balance = $1, updated_at = NOW() WHERE id = $2
	`, newBalance.String(), id)
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
	`, tx.ID, tx.WorkspaceID, tx.FiatAccountID, string(tx.Type), tx.Currency, tx.Amount.String(),
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

// scanFiatTransaction scans a fiat transaction row
func scanFiatTransaction(scanner interface{ Scan(dest ...interface{}) error }) (*domain.FiatTransaction, error) {
	t := &domain.FiatTransaction{}
	var amountStr string
	var metadataJSON []byte
	if err := scanner.Scan(&t.ID, &t.WorkspaceID, &t.FiatAccountID, &t.Type, &t.Currency, &amountStr,
		&t.Reference, &t.Counterparty, &t.State, &t.LedgerEntryID, &metadataJSON,
		&t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	var err error
	t.Amount, err = decimal.NewFromString(amountStr)
	if err != nil {
		return nil, fmt.Errorf("parse amount for fiat tx %s: %w", t.ID, err)
	}
	t.Metadata = make(map[string]interface{})
	if len(metadataJSON) > 0 {
		json.Unmarshal(metadataJSON, &t.Metadata)
	}
	return t, nil
}

// ListTransactions returns paginated fiat transactions for a workspace
func (r *FiatRepo) ListTransactions(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*domain.FiatTransaction, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, fiat_account_id, type, currency, amount::TEXT,
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
		t, err := scanFiatTransaction(rows)
		if err != nil {
			return nil, err
		}
		txs = append(txs, t)
	}
	return txs, rows.Err()
}

// ListAllTransactions returns paginated fiat transactions across all workspaces (admin view)
func (r *FiatRepo) ListAllTransactions(ctx context.Context, limit, offset int) ([]*domain.FiatTransaction, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, fiat_account_id, type, currency, amount::TEXT,
			reference, counterparty, state, ledger_entry_id, metadata, created_at, updated_at
		FROM fiat_transactions
		ORDER BY created_at DESC LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []*domain.FiatTransaction
	for rows.Next() {
		t, err := scanFiatTransaction(rows)
		if err != nil {
			return nil, err
		}
		txs = append(txs, t)
	}
	return txs, rows.Err()
}

// GetLatestRate returns the most recent conversion rate for a currency pair
func (r *FiatRepo) GetLatestRate(ctx context.Context, from, to string) (*domain.ConversionRate, error) {
	rate := &domain.ConversionRate{}
	var rateStr string
	err := r.pool.QueryRow(ctx, `
		SELECT id, from_currency, to_currency, rate::TEXT, source, valid_from, valid_until, created_at
		FROM conversion_rates
		WHERE from_currency = $1 AND to_currency = $2
			AND valid_from <= NOW()
			AND (valid_until IS NULL OR valid_until > NOW())
		ORDER BY valid_from DESC LIMIT 1
	`, from, to).Scan(&rate.ID, &rate.FromCurrency, &rate.ToCurrency, &rateStr,
		&rate.Source, &rate.ValidFrom, &rate.ValidUntil, &rate.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("no rate found for %s/%s: %w", from, to, err)
	}
	rate.Rate, err = decimal.NewFromString(rateStr)
	if err != nil {
		return nil, fmt.Errorf("parse rate for %s/%s: %w", from, to, err)
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
	`, rate.ID, rate.FromCurrency, rate.ToCurrency, rate.Rate.String(), rate.Source, rate.ValidFrom, rate.ValidUntil)
	return err
}

// FindOperatingAccount returns the first active fiat account for a workspace/currency,
// preferring OPERATING accounts
func (r *FiatRepo) FindOperatingAccount(ctx context.Context, workspaceID uuid.UUID, currency string) (*domain.FiatAccount, error) {
	a := &domain.FiatAccount{}
	var balStr string
	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, currency, provider, provider_account_id, account_type,
			balance::TEXT, is_active, created_at, updated_at
		FROM fiat_accounts
		WHERE workspace_id = $1 AND currency = $2 AND is_active = TRUE
		ORDER BY CASE account_type WHEN 'OPERATING' THEN 0 ELSE 1 END
		LIMIT 1
	`, workspaceID, currency).Scan(&a.ID, &a.WorkspaceID, &a.Currency, &a.Provider, &a.ProviderAccountID,
		&a.AccountType, &balStr, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("no active fiat account for workspace %s currency %s: %w", workspaceID, currency, err)
	}
	a.Balance, err = decimal.NewFromString(balStr)
	if err != nil {
		return nil, fmt.Errorf("parse balance for fiat account %s: %w", a.ID, err)
	}
	return a, nil
}
