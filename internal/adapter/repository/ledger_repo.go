package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/google/uuid"
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

// PostEntry creates a journal entry with its lines in a single transaction.
// The entry_hash is computed as SHA-256(previous_hash || entry_id || lines_data).
// This method enforces debit == credit balance before posting.
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
	defer tx.Rollback(ctx)

	// Get previous hash for chain integrity
	var prevHash string
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT entry_hash FROM ledger_entries ORDER BY created_at DESC LIMIT 1),
			'GENESIS'
		)
	`).Scan(&prevHash)
	if err != nil {
		return fmt.Errorf("get previous hash: %w", err)
	}
	entry.PreviousHash = prevHash

	// Compute entry hash
	hashInput := prevHash + "|" + entry.ID.String() + "|" + entry.EntryType + "|"
	for _, line := range entry.Lines {
		hashInput += fmt.Sprintf("%s:%s:%s:%v;", line.AccountID, line.Amount.String(), line.Currency, line.IsDebit)
	}
	h := sha256.Sum256([]byte(hashInput))
	entry.EntryHash = hex.EncodeToString(h[:])

	// Insert entry
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, idempotency_key, entry_type, description, state, reference_type, reference_id, previous_hash, entry_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, entry.ID, entry.IdempotencyKey, entry.EntryType, entry.Description,
		string(entry.State), entry.ReferenceType, entry.ReferenceID,
		entry.PreviousHash, entry.EntryHash)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}

	// Insert lines
	for i, line := range entry.Lines {
		line.ID = uuid.New()
		line.EntryID = entry.ID
		line.Sequence = i
		_, err = tx.Exec(ctx, `
			INSERT INTO ledger_lines (id, entry_id, account_id, amount, currency, is_debit, sequence)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, line.ID, line.EntryID, line.AccountID, line.Amount.String(), line.Currency, line.IsDebit, line.Sequence)
		if err != nil {
			return fmt.Errorf("insert line %d: %w", i, err)
		}
	}

	return tx.Commit(ctx)
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

// GetAccountBalance returns the net balance for a ledger account
func (r *LedgerRepo) GetAccountBalance(ctx context.Context, accountID uuid.UUID) (*big.Int, error) {
	var debitStr, creditStr string
	err := r.pool.QueryRow(ctx, `
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
func (r *LedgerRepo) VerifyHashChain(ctx context.Context) (int, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, previous_hash, entry_hash FROM ledger_entries ORDER BY created_at ASC
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	var lastHash string = "GENESIS"
	for rows.Next() {
		var id uuid.UUID
		var prevHash, entryHash string
		if err := rows.Scan(&id, &prevHash, &entryHash); err != nil {
			return count, err
		}
		if prevHash != lastHash {
			return count, fmt.Errorf("hash chain broken at entry %s: expected previous=%s, got=%s", id, lastHash, prevHash)
		}
		lastHash = entryHash
		count++
	}
	return count, rows.Err()
}
