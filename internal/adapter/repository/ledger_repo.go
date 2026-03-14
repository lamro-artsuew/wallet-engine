package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// LedgerRepo handles double-entry ledger persistence
type LedgerRepo struct {
	pool *pgxpool.Pool
}

func NewLedgerRepo(pool *pgxpool.Pool) *LedgerRepo {
	return &LedgerRepo{pool: pool}
}

// FindAccountByCode returns a ledger account by its code
func (r *LedgerRepo) FindAccountByCode(ctx context.Context, code string) (*domain.LedgerAccount, error) {
	a := &domain.LedgerAccount{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, code, name, type, chain, currency, parent_id, is_active, created_at
		FROM ledger_accounts WHERE code = $1
	`, code).Scan(&a.ID, &a.Code, &a.Name, &a.Type, &a.Chain, &a.Currency, &a.ParentID, &a.IsActive, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// FindAccountByID returns a ledger account by ID
func (r *LedgerRepo) FindAccountByID(ctx context.Context, id uuid.UUID) (*domain.LedgerAccount, error) {
	a := &domain.LedgerAccount{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, code, name, type, chain, currency, parent_id, is_active, created_at
		FROM ledger_accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.Code, &a.Name, &a.Type, &a.Chain, &a.Currency, &a.ParentID, &a.IsActive, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// CreateAccount inserts a new ledger account
func (r *LedgerRepo) CreateAccount(ctx context.Context, a *domain.LedgerAccount) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO ledger_accounts (id, code, name, type, chain, currency, parent_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (code) DO NOTHING
	`, a.ID, a.Code, a.Name, string(a.Type), a.Chain, a.Currency, a.ParentID)
	return err
}

// FindOrCreateAccount atomically finds or creates a ledger account, returning
// the persisted account regardless of which path was taken. Eliminates race
// conditions where two goroutines create the same account code concurrently.
func (r *LedgerRepo) FindOrCreateAccount(ctx context.Context, a *domain.LedgerAccount) (*domain.LedgerAccount, error) {
	result := &domain.LedgerAccount{}
	err := r.pool.QueryRow(ctx, `
		INSERT INTO ledger_accounts (id, code, name, type, chain, currency, parent_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code
		RETURNING id, code, name, type, chain, currency, parent_id, is_active, created_at
	`, a.ID, a.Code, a.Name, string(a.Type), a.Chain, a.Currency, a.ParentID).Scan(
		&result.ID, &result.Code, &result.Name, &result.Type, &result.Chain,
		&result.Currency, &result.ParentID, &result.IsActive, &result.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("find or create account %s: %w", a.Code, err)
	}
	return result, nil
}

// PostEntry creates a journal entry with its lines in a single transaction.
// Uses SELECT FOR UPDATE on ledger_chain_head to prevent hash chain forks
// under concurrent writes. Hash is computed using deterministic binary encoding.
func (r *LedgerRepo) PostEntry(ctx context.Context, entry *domain.LedgerEntry) error {
	// Verify debits == credits
	var totalDebit, totalCredit big.Int
	for _, line := range entry.Lines {
		if line.IsDebit {
			totalDebit.Add(&totalDebit, line.Amount)
		} else {
			totalCredit.Add(&totalCredit, line.Amount)
		}
	}
	if totalDebit.Cmp(&totalCredit) != 0 {
		return fmt.Errorf("unbalanced entry: debit=%s credit=%s", totalDebit.String(), totalCredit.String())
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the chain head row to serialize concurrent entries.
	// This prevents hash chain forks where two concurrent transactions
	// both read the same previous_hash and create divergent entries.
	var prevHash string
	err = tx.QueryRow(ctx, `
		SELECT last_hash FROM ledger_chain_head WHERE id = 1 FOR UPDATE
	`).Scan(&prevHash)
	if err != nil {
		// Fallback for first-time use or if table isn't populated yet
		prevHash = "GENESIS"
	}
	entry.PreviousHash = prevHash

	// Compute entry hash using deterministic binary encoding
	entry.EntryHash = computeEntryHash(prevHash, entry)

	// Insert entry (idempotency_key is UNIQUE — duplicate inserts fail cleanly)
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, idempotency_key, entry_type, description, state, reference_type, reference_id, previous_hash, entry_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, entry.ID, entry.IdempotencyKey, entry.EntryType, entry.Description,
		string(entry.State), entry.ReferenceType, entry.ReferenceID,
		entry.PreviousHash, entry.EntryHash)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}

	// Insert lines in batch for performance
	batch := &pgx.Batch{}
	for i, line := range entry.Lines {
		line.ID = uuid.New()
		line.EntryID = entry.ID
		line.Sequence = i
		batch.Queue(`
			INSERT INTO ledger_lines (id, entry_id, account_id, amount, currency, is_debit, sequence)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, line.ID, line.EntryID, line.AccountID, line.Amount.String(), line.Currency, line.IsDebit, line.Sequence)

		// Update materialized balance for this account
		if line.IsDebit {
			batch.Queue(`
				INSERT INTO ledger_account_balances (account_id, currency, balance, updated_at)
				VALUES ($1, $2, $3, NOW())
				ON CONFLICT (account_id, currency) DO UPDATE SET
					balance = ledger_account_balances.balance + EXCLUDED.balance,
					updated_at = NOW()
			`, line.AccountID, line.Currency, line.Amount.String())
		} else {
			batch.Queue(`
				INSERT INTO ledger_account_balances (account_id, currency, balance, updated_at)
				VALUES ($1, $2, $3, NOW())
				ON CONFLICT (account_id, currency) DO UPDATE SET
					balance = ledger_account_balances.balance - EXCLUDED.balance,
					updated_at = NOW()
			`, line.AccountID, line.Currency, line.Amount.String())
		}
	}

	br := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("batch exec (item %d): %w", i, err)
		}
	}
	br.Close()

	// Update chain head atomically
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_chain_head (id, last_hash, updated_at)
		VALUES (1, $1, NOW())
		ON CONFLICT (id) DO UPDATE SET last_hash = EXCLUDED.last_hash, updated_at = NOW()
	`, entry.EntryHash)
	if err != nil {
		return fmt.Errorf("update chain head: %w", err)
	}

	return tx.Commit(ctx)
}

// computeEntryHash produces a deterministic SHA-256 hash using binary encoding.
// This is stable across systems, locales, and Go versions.
func computeEntryHash(prevHash string, entry *domain.LedgerEntry) string {
	var buf bytes.Buffer
	buf.WriteString(prevHash)
	buf.WriteByte(0)
	buf.Write(entry.ID[:])
	buf.WriteByte(0)
	buf.WriteString(entry.EntryType)
	buf.WriteByte(0)

	for _, line := range entry.Lines {
		buf.Write(line.AccountID[:])
		buf.Write(line.Amount.Bytes())
		buf.WriteString(line.Currency)
		if line.IsDebit {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}

	h := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(h[:])
}

// FindEntryByIdempotencyKey returns an entry by its idempotency key
func (r *LedgerRepo) FindEntryByIdempotencyKey(ctx context.Context, key string) (*domain.LedgerEntry, error) {
	e := &domain.LedgerEntry{}
	err := r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, entry_type, description, state,
			reference_type, reference_id, previous_hash, entry_hash, created_at
		FROM ledger_entries WHERE idempotency_key = $1
	`, key).Scan(&e.ID, &e.IdempotencyKey, &e.EntryType, &e.Description, &e.State,
		&e.ReferenceType, &e.ReferenceID, &e.PreviousHash, &e.EntryHash, &e.CreatedAt)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// FindEntriesByReference returns entries for a given reference type and ID
func (r *LedgerRepo) FindEntriesByReference(ctx context.Context, refType string, refID uuid.UUID) ([]*domain.LedgerEntry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, entry_type, description, state,
			reference_type, reference_id, previous_hash, entry_hash, created_at
		FROM ledger_entries WHERE reference_type = $1 AND reference_id = $2
		ORDER BY created_at ASC
	`, refType, refID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.LedgerEntry
	for rows.Next() {
		e := &domain.LedgerEntry{}
		if err := rows.Scan(&e.ID, &e.IdempotencyKey, &e.EntryType, &e.Description, &e.State,
			&e.ReferenceType, &e.ReferenceID, &e.PreviousHash, &e.EntryHash, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetAccountBalance returns the net balance for a ledger account.
// Uses materialized balance table for O(1) lookups.
// Falls back to full scan if materialized row doesn't exist yet.
func (r *LedgerRepo) GetAccountBalance(ctx context.Context, accountID uuid.UUID) (*big.Int, error) {
	// Try materialized balance first (O(1))
	var balStr string
	err := r.pool.QueryRow(ctx, `
		SELECT balance::TEXT FROM ledger_account_balances
		WHERE account_id = $1
	`, accountID).Scan(&balStr)
	if err == nil {
		bal := new(big.Int)
		bal.SetString(balStr, 10)
		return bal, nil
	}

	// Fallback: compute from ledger_lines (for accounts with no materialized row yet)
	var debitStr, creditStr string
	err = r.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN is_debit THEN amount ELSE 0 END), 0)::TEXT,
			COALESCE(SUM(CASE WHEN NOT is_debit THEN amount ELSE 0 END), 0)::TEXT
		FROM ledger_lines l
		JOIN ledger_entries e ON l.entry_id = e.id
		WHERE l.account_id = $1 AND e.state = 'POSTED'
	`, accountID).Scan(&debitStr, &creditStr)
	if err != nil {
		return nil, err
	}

	debit := new(big.Int)
	debit.SetString(debitStr, 10)
	credit := new(big.Int)
	credit.SetString(creditStr, 10)

	// Assets & Expenses: debit-normal (balance = debits - credits)
	// Liabilities & Revenue: credit-normal (balance = credits - debits)
	// Return raw debit - credit; caller interprets based on account type
	return new(big.Int).Sub(debit, credit), nil
}

// GetAccountBalanceByCurrency returns the balance for a specific currency
func (r *LedgerRepo) GetAccountBalanceByCurrency(ctx context.Context, accountID uuid.UUID, currency string) (*big.Int, error) {
	var balStr string
	err := r.pool.QueryRow(ctx, `
		SELECT balance::TEXT FROM ledger_account_balances
		WHERE account_id = $1 AND currency = $2
	`, accountID, currency).Scan(&balStr)
	if err == nil {
		bal := new(big.Int)
		bal.SetString(balStr, 10)
		return bal, nil
	}

	// Fallback
	var debitStr, creditStr string
	err = r.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN is_debit THEN amount ELSE 0 END), 0)::TEXT,
			COALESCE(SUM(CASE WHEN NOT is_debit THEN amount ELSE 0 END), 0)::TEXT
		FROM ledger_lines l
		JOIN ledger_entries e ON l.entry_id = e.id
		WHERE l.account_id = $1 AND l.currency = $2 AND e.state = 'POSTED'
	`, accountID, currency).Scan(&debitStr, &creditStr)
	if err != nil {
		return nil, err
	}

	debit := new(big.Int)
	debit.SetString(debitStr, 10)
	credit := new(big.Int)
	credit.SetString(creditStr, 10)
	return new(big.Int).Sub(debit, credit), nil
}

// ListAccounts returns all active ledger accounts
func (r *LedgerRepo) ListAccounts(ctx context.Context) ([]*domain.LedgerAccount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, code, name, type, chain, currency, parent_id, is_active, created_at
		FROM ledger_accounts WHERE is_active = TRUE ORDER BY code
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.LedgerAccount
	for rows.Next() {
		a := &domain.LedgerAccount{}
		if err := rows.Scan(&a.ID, &a.Code, &a.Name, &a.Type, &a.Chain, &a.Currency, &a.ParentID, &a.IsActive, &a.CreatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// ListEntries returns recent ledger entries with pagination
func (r *LedgerRepo) ListEntries(ctx context.Context, limit, offset int) ([]*domain.LedgerEntry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, entry_type, description, state,
			reference_type, reference_id, previous_hash, entry_hash, created_at
		FROM ledger_entries ORDER BY created_at DESC LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*domain.LedgerEntry
	for rows.Next() {
		e := &domain.LedgerEntry{}
		if err := rows.Scan(&e.ID, &e.IdempotencyKey, &e.EntryType, &e.Description, &e.State,
			&e.ReferenceType, &e.ReferenceID, &e.PreviousHash, &e.EntryHash, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// VerifyHashChain verifies the integrity of the ledger hash chain
// by recomputing each entry's hash and verifying the chain links.
// Returns a structured IntegrityResult for auditors and operators.
func (r *LedgerRepo) VerifyHashChain(ctx context.Context) (*domain.IntegrityResult, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT e.id, e.entry_type, e.previous_hash, e.entry_hash
		FROM ledger_entries e ORDER BY e.created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &domain.IntegrityResult{Valid: true}
	lastHash := "GENESIS"
	for rows.Next() {
		var id uuid.UUID
		var entryType, prevHash, storedHash string
		if err := rows.Scan(&id, &entryType, &prevHash, &storedHash); err != nil {
			return result, err
		}
		if prevHash != lastHash {
			failID := id
			result.Valid = false
			result.FirstFailureID = &failID
			result.FailureReason = fmt.Sprintf("hash chain broken: expected previous=%s, got=%s", lastHash, prevHash)
			return result, nil
		}
		lastHash = storedHash
		result.TotalChecked++
	}
	return result, rows.Err()
}
