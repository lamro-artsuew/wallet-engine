package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	ledgerEntriesPosted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_ledger_entries_posted_total",
		Help: "Total ledger entries posted by type",
	}, []string{"entry_type"})

	ledgerPostingErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_ledger_posting_errors_total",
		Help: "Ledger posting errors by type",
	}, []string{"entry_type", "error_type"})
)

// LedgerService handles double-entry ledger operations
type LedgerService struct {
	ledgerRepo *repository.LedgerRepo
}

// NewLedgerService creates a new ledger service
func NewLedgerService(ledgerRepo *repository.LedgerRepo) *LedgerService {
	return &LedgerService{ledgerRepo: ledgerRepo}
}

// PostDepositEntry creates a double-entry journal for a confirmed deposit.
// Debit:  ASSET:HOT:{CHAIN}:{TOKEN}  (hot wallet received tokens)
// Credit: LIABILITY:USER:{TOKEN}     (owed to user)
func (s *LedgerService) PostDepositEntry(ctx context.Context, d *domain.Deposit) error {
	if err := validateAmount(d.Amount); err != nil {
		return fmt.Errorf("deposit %s: %w", d.ID, err)
	}

	idempotencyKey := fmt.Sprintf("deposit:%s", d.ID.String())
	if err := s.checkIdempotency(ctx, idempotencyKey); err != nil {
		if errors.Is(err, errAlreadyPosted) {
			return nil
		}
		return err
	}

	chainUpper := strings.ToUpper(d.Chain)
	assetCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, d.TokenSymbol)
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", d.TokenSymbol)

	assetAccount, err := s.findOrCreateAccount(ctx, assetCode, d.TokenSymbol, domain.AccountAsset,
		fmt.Sprintf("%s Hot Wallet %s", d.Chain, d.TokenSymbol), &d.Chain)
	if err != nil {
		ledgerPostingErrors.WithLabelValues("DEPOSIT", "account_create").Inc()
		return fmt.Errorf("resolve asset account %s: %w", assetCode, err)
	}

	liabilityAccount, err := s.findOrCreateAccount(ctx, liabilityCode, d.TokenSymbol, domain.AccountLiability,
		fmt.Sprintf("User %s Balances", d.TokenSymbol), nil)
	if err != nil {
		ledgerPostingErrors.WithLabelValues("DEPOSIT", "account_create").Inc()
		return fmt.Errorf("resolve liability account %s: %w", liabilityCode, err)
	}

	refType := "deposit"
	entry := &domain.LedgerEntry{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		EntryType:      "DEPOSIT",
		Description:    fmt.Sprintf("Deposit %s %s on %s tx:%s", d.Amount.String(), d.TokenSymbol, d.Chain, d.TxHash),
		State:          domain.EntryPosted,
		ReferenceType:  &refType,
		ReferenceID:    &d.ID,
		Lines: []domain.LedgerLine{
			{
				AccountID: assetAccount.ID,
				Amount:    new(big.Int).Set(d.Amount),
				Currency:  d.TokenSymbol,
				IsDebit:   true,
			},
			{
				AccountID: liabilityAccount.ID,
				Amount:    new(big.Int).Set(d.Amount),
				Currency:  d.TokenSymbol,
				IsDebit:   false,
			},
		},
	}

	if err := s.ledgerRepo.PostEntry(ctx, entry); err != nil {
		ledgerPostingErrors.WithLabelValues("DEPOSIT", "post").Inc()
		return fmt.Errorf("post deposit entry: %w", err)
	}

	ledgerEntriesPosted.WithLabelValues("DEPOSIT").Inc()
	log.Info().
		Str("entry_id", entry.ID.String()).
		Str("deposit_id", d.ID.String()).
		Str("hash", entry.EntryHash).
		Msg("deposit ledger entry posted")

	return nil
}

// PostWithdrawalEntry creates a double-entry journal for a confirmed withdrawal.
// Base entry (always):
//   Debit:  LIABILITY:USER:{TOKEN}          amount
//   Credit: ASSET:HOT:{CHAIN}:{TOKEN}       amount
// Fee entry (when fee > 0):
//   Debit:  LIABILITY:USER:{TOKEN}          fee
//   Credit: REVENUE:WITHDRAWAL_FEE:{TOKEN}  fee
func (s *LedgerService) PostWithdrawalEntry(ctx context.Context, w *domain.Withdrawal) error {
	if err := validateAmount(w.Amount); err != nil {
		return fmt.Errorf("withdrawal %s: %w", w.ID, err)
	}

	idempotencyKey := fmt.Sprintf("withdrawal:%s", w.ID.String())
	if err := s.checkIdempotency(ctx, idempotencyKey); err != nil {
		if errors.Is(err, errAlreadyPosted) {
			return nil
		}
		return err
	}

	chainUpper := strings.ToUpper(w.Chain)
	assetCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, w.TokenSymbol)
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", w.TokenSymbol)

	assetAccount, err := s.ledgerRepo.FindAccountByCode(ctx, assetCode)
	if err != nil {
		ledgerPostingErrors.WithLabelValues("WITHDRAWAL", "account_missing").Inc()
		return fmt.Errorf("asset account %s not found: %w", assetCode, err)
	}

	liabilityAccount, err := s.ledgerRepo.FindAccountByCode(ctx, liabilityCode)
	if err != nil {
		ledgerPostingErrors.WithLabelValues("WITHDRAWAL", "account_missing").Inc()
		return fmt.Errorf("liability account %s not found: %w", liabilityCode, err)
	}

	refType := "withdrawal"
	entry := &domain.LedgerEntry{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		EntryType:      "WITHDRAWAL",
		Description:    fmt.Sprintf("Withdrawal %s %s on %s to:%s", w.Amount.String(), w.TokenSymbol, w.Chain, w.ToAddress),
		State:          domain.EntryPosted,
		ReferenceType:  &refType,
		ReferenceID:    &w.ID,
		Lines: []domain.LedgerLine{
			{
				AccountID: liabilityAccount.ID,
				Amount:    new(big.Int).Set(w.Amount),
				Currency:  w.TokenSymbol,
				IsDebit:   true,
			},
			{
				AccountID: assetAccount.ID,
				Amount:    new(big.Int).Set(w.Amount),
				Currency:  w.TokenSymbol,
				IsDebit:   false,
			},
		},
	}

	// Fee must be recorded — auto-create fee account if missing
	if w.Fee != nil && w.Fee.Sign() > 0 {
		feeCode := fmt.Sprintf("REVENUE:WITHDRAWAL_FEE:%s", w.TokenSymbol)
		feeAccount, err := s.findOrCreateAccount(ctx, feeCode, w.TokenSymbol, domain.AccountRevenue,
			fmt.Sprintf("Withdrawal Fee Revenue %s", w.TokenSymbol), nil)
		if err != nil {
			ledgerPostingErrors.WithLabelValues("WITHDRAWAL", "fee_account").Inc()
			return fmt.Errorf("resolve fee account %s: %w", feeCode, err)
		}
		entry.Lines = append(entry.Lines,
			domain.LedgerLine{
				AccountID: liabilityAccount.ID,
				Amount:    new(big.Int).Set(w.Fee),
				Currency:  w.TokenSymbol,
				IsDebit:   true,
			},
			domain.LedgerLine{
				AccountID: feeAccount.ID,
				Amount:    new(big.Int).Set(w.Fee),
				Currency:  w.TokenSymbol,
				IsDebit:   false,
			},
		)
	}

	if err := s.ledgerRepo.PostEntry(ctx, entry); err != nil {
		ledgerPostingErrors.WithLabelValues("WITHDRAWAL", "post").Inc()
		return fmt.Errorf("post withdrawal entry: %w", err)
	}

	ledgerEntriesPosted.WithLabelValues("WITHDRAWAL").Inc()
	log.Info().
		Str("entry_id", entry.ID.String()).
		Str("withdrawal_id", w.ID.String()).
		Str("hash", entry.EntryHash).
		Msg("withdrawal ledger entry posted")

	return nil
}

// PostSweepEntry creates a journal for sweeping funds from deposit address to hot wallet.
// This is an internal transfer — no user liability change.
// Main entry:
//   Debit:  ASSET:HOT:{CHAIN}:{TOKEN}      swept_amount
//   Credit: ASSET:DEPOSIT:{CHAIN}:{TOKEN}   swept_amount
// Gas expense (when gas > 0):
//   Debit:  EXPENSE:GAS:{CHAIN}:NATIVE      gas_cost
//   Credit: ASSET:HOT:{CHAIN}:NATIVE        gas_cost
func (s *LedgerService) PostSweepEntry(ctx context.Context, sweep *domain.SweepRecord, tokenSymbol string) error {
	if err := validateAmount(sweep.Amount); err != nil {
		return fmt.Errorf("sweep %s: %w", sweep.ID, err)
	}

	idempotencyKey := fmt.Sprintf("sweep:%s", sweep.ID.String())
	if err := s.checkIdempotency(ctx, idempotencyKey); err != nil {
		if errors.Is(err, errAlreadyPosted) {
			return nil
		}
		return err
	}

	chainUpper := strings.ToUpper(sweep.Chain)
	hotCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, tokenSymbol)
	depositCode := fmt.Sprintf("ASSET:DEPOSIT:%s:%s", chainUpper, tokenSymbol)

	hotAccount, err := s.ledgerRepo.FindAccountByCode(ctx, hotCode)
	if err != nil {
		return fmt.Errorf("hot account %s not found: %w", hotCode, err)
	}

	depositAccount, err := s.findOrCreateAccount(ctx, depositCode, tokenSymbol, domain.AccountAsset,
		fmt.Sprintf("%s Deposit Addresses %s", sweep.Chain, tokenSymbol), &sweep.Chain)
	if err != nil {
		return fmt.Errorf("resolve deposit account %s: %w", depositCode, err)
	}

	refType := "sweep"
	entry := &domain.LedgerEntry{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		EntryType:      "SWEEP",
		Description:    fmt.Sprintf("Sweep %s from %s to hot wallet on %s", tokenSymbol, sweep.FromAddress, sweep.Chain),
		State:          domain.EntryPosted,
		ReferenceType:  &refType,
		ReferenceID:    &sweep.ID,
		Lines: []domain.LedgerLine{
			{
				AccountID: hotAccount.ID,
				Amount:    new(big.Int).Set(sweep.Amount),
				Currency:  tokenSymbol,
				IsDebit:   true,
			},
			{
				AccountID: depositAccount.ID,
				Amount:    new(big.Int).Set(sweep.Amount),
				Currency:  tokenSymbol,
				IsDebit:   false,
			},
		},
	}

	// Record gas expense if gas was consumed
	if sweep.GasCost != nil && sweep.GasCost.Sign() > 0 {
		nativeSymbol := "NATIVE"
		gasExpenseCode := fmt.Sprintf("EXPENSE:GAS:%s:%s", chainUpper, nativeSymbol)
		nativeHotCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, nativeSymbol)

		gasExpenseAcct, err := s.findOrCreateAccount(ctx, gasExpenseCode, nativeSymbol, domain.AccountExpense,
			fmt.Sprintf("Gas Expense %s", sweep.Chain), &sweep.Chain)
		if err != nil {
			log.Warn().Err(err).Str("chain", sweep.Chain).Msg("failed to create gas expense account, sweep entry posted without gas")
		} else {
			nativeHotAcct, err := s.findOrCreateAccount(ctx, nativeHotCode, nativeSymbol, domain.AccountAsset,
				fmt.Sprintf("%s Hot Wallet Native", sweep.Chain), &sweep.Chain)
			if err != nil {
				log.Warn().Err(err).Str("chain", sweep.Chain).Msg("failed to resolve native hot account for gas")
			} else {
				entry.Lines = append(entry.Lines,
					domain.LedgerLine{
						AccountID: gasExpenseAcct.ID,
						Amount:    new(big.Int).Set(sweep.GasCost),
						Currency:  nativeSymbol,
						IsDebit:   true,
					},
					domain.LedgerLine{
						AccountID: nativeHotAcct.ID,
						Amount:    new(big.Int).Set(sweep.GasCost),
						Currency:  nativeSymbol,
						IsDebit:   false,
					},
				)
			}
		}
	}

	if err := s.ledgerRepo.PostEntry(ctx, entry); err != nil {
		ledgerPostingErrors.WithLabelValues("SWEEP", "post").Inc()
		return fmt.Errorf("post sweep entry: %w", err)
	}

	ledgerEntriesPosted.WithLabelValues("SWEEP").Inc()
	log.Info().
		Str("entry_id", entry.ID.String()).
		Str("sweep_id", sweep.ID.String()).
		Str("chain", sweep.Chain).
		Str("token", tokenSymbol).
		Str("hash", entry.EntryHash).
		Msg("sweep ledger entry posted")

	return nil
}

// PostFiatEntry creates a balanced double-entry journal for fiat/bridge operations.
// Returns the entry ID for linking to fiat transactions.
func (s *LedgerService) PostFiatEntry(ctx context.Context, idempotencyKey, entryType, description string,
	debitAccountCode, creditAccountCode, currency string, amount *big.Int,
	refType string, refID *uuid.UUID) (*uuid.UUID, error) {

	if err := validateAmount(amount); err != nil {
		return nil, fmt.Errorf("%s: %w", entryType, err)
	}

	// Idempotency check
	existing, err := s.ledgerRepo.FindEntryByIdempotencyKey(ctx, idempotencyKey)
	if err == nil {
		return &existing.ID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		ledgerPostingErrors.WithLabelValues(entryType, "idempotency_check").Inc()
		return nil, fmt.Errorf("idempotency check for %s failed: %w", idempotencyKey, err)
	}

	debitAccount, err := s.getOrCreateAccount(ctx, debitAccountCode, currency)
	if err != nil {
		ledgerPostingErrors.WithLabelValues(entryType, "account_create").Inc()
		return nil, fmt.Errorf("resolve debit account %s: %w", debitAccountCode, err)
	}

	creditAccount, err := s.getOrCreateAccount(ctx, creditAccountCode, currency)
	if err != nil {
		ledgerPostingErrors.WithLabelValues(entryType, "account_create").Inc()
		return nil, fmt.Errorf("resolve credit account %s: %w", creditAccountCode, err)
	}

	entry := &domain.LedgerEntry{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		EntryType:      entryType,
		Description:    description,
		State:          domain.EntryPosted,
		ReferenceType:  &refType,
		ReferenceID:    refID,
		Lines: []domain.LedgerLine{
			{
				AccountID: debitAccount.ID,
				Amount:    new(big.Int).Set(amount),
				Currency:  currency,
				IsDebit:   true,
			},
			{
				AccountID: creditAccount.ID,
				Amount:    new(big.Int).Set(amount),
				Currency:  currency,
				IsDebit:   false,
			},
		},
	}

	if err := s.ledgerRepo.PostEntry(ctx, entry); err != nil {
		ledgerPostingErrors.WithLabelValues(entryType, "post").Inc()
		return nil, fmt.Errorf("post entry: %w", err)
	}

	ledgerEntriesPosted.WithLabelValues(entryType).Inc()
	log.Info().
		Str("entry_id", entry.ID.String()).
		Str("entry_type", entryType).
		Str("hash", entry.EntryHash).
		Msg("fiat ledger entry posted")

	return &entry.ID, nil
}

// getOrCreateAccount resolves a ledger account by code, creating it if missing.
// Uses the consolidated FindOrCreateAccount repo method for atomicity.
func (s *LedgerService) getOrCreateAccount(ctx context.Context, code, currency string) (*domain.LedgerAccount, error) {
	account, err := s.ledgerRepo.FindAccountByCode(ctx, code)
	if err == nil {
		return account, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("lookup account %s: %w", code, err)
	}

	var accountType domain.LedgerAccountType
	var name string
	switch {
	case strings.HasPrefix(code, "ASSET:"):
		accountType = domain.AccountAsset
		name = fmt.Sprintf("Asset %s", code)
	case strings.HasPrefix(code, "LIABILITY:"):
		accountType = domain.AccountLiability
		name = fmt.Sprintf("Liability %s", code)
	case strings.HasPrefix(code, "REVENUE:"):
		accountType = domain.AccountRevenue
		name = fmt.Sprintf("Revenue %s", code)
	case strings.HasPrefix(code, "EXPENSE:"):
		accountType = domain.AccountExpense
		name = fmt.Sprintf("Expense %s", code)
	default:
		return nil, fmt.Errorf("unknown account type for code: %s", code)
	}

	return s.ledgerRepo.FindOrCreateAccount(ctx, &domain.LedgerAccount{
		ID:       uuid.New(),
		Code:     code,
		Name:     name,
		Type:     accountType,
		Currency: currency,
		IsActive: true,
	})
}

// findOrCreateAccount resolves or creates a ledger account atomically.
func (s *LedgerService) findOrCreateAccount(ctx context.Context, code, currency string,
	accountType domain.LedgerAccountType, name string, chain *string) (*domain.LedgerAccount, error) {

	return s.ledgerRepo.FindOrCreateAccount(ctx, &domain.LedgerAccount{
		ID:       uuid.New(),
		Code:     code,
		Name:     name,
		Type:     accountType,
		Chain:    chain,
		Currency: currency,
		IsActive: true,
	})
}

// checkIdempotency checks if an entry with the given key already exists.
// Returns nil (no error) if already posted — caller should return nil.
// Returns ErrNotFound-based sentinel if entry doesn't exist — caller should proceed.
// Returns wrapped error on DB failure — caller must propagate.
func (s *LedgerService) checkIdempotency(ctx context.Context, key string) error {
	_, err := s.ledgerRepo.FindEntryByIdempotencyKey(ctx, key)
	if err == nil {
		log.Debug().Str("key", key).Msg("ledger entry already posted, skipping")
		return errAlreadyPosted
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // not found = proceed
	}
	// Transient DB error — must not proceed with posting
	return fmt.Errorf("idempotency check for %s failed: %w", key, err)
}

// errAlreadyPosted is a sentinel used internally by checkIdempotency
var errAlreadyPosted = errors.New("already posted")

// GetAccountBalance returns the balance for an account by code
func (s *LedgerService) GetAccountBalance(ctx context.Context, code string) (*big.Int, error) {
	account, err := s.ledgerRepo.FindAccountByCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("account %s not found: %w", code, err)
	}
	return s.ledgerRepo.GetAccountBalance(ctx, account.ID)
}

// ListAccounts returns all ledger accounts
func (s *LedgerService) ListAccounts(ctx context.Context) ([]*domain.LedgerAccount, error) {
	return s.ledgerRepo.ListAccounts(ctx)
}

// ListEntries returns recent ledger entries
func (s *LedgerService) ListEntries(ctx context.Context, limit, offset int) ([]*domain.LedgerEntry, error) {
	return s.ledgerRepo.ListEntries(ctx, limit, offset)
}

// VerifyIntegrity checks the hash chain integrity of the ledger.
// Returns a structured IntegrityResult with total checked, validity, and failure details.
func (s *LedgerService) VerifyIntegrity(ctx context.Context) (*domain.IntegrityResult, error) {
	return s.ledgerRepo.VerifyHashChain(ctx)
}

// validateAmount ensures the posting amount is positive
func validateAmount(amount *big.Int) error {
	if amount == nil {
		return fmt.Errorf("amount is nil")
	}
	if amount.Sign() <= 0 {
		return fmt.Errorf("amount must be positive, got %s", amount.String())
	}
	return nil
}
