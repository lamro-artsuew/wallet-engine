package service

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"
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
	idempotencyKey := fmt.Sprintf("deposit:%s", d.ID.String())

	// Idempotency: skip if already posted
	if _, err := s.ledgerRepo.FindEntryByIdempotencyKey(ctx, idempotencyKey); err == nil {
		log.Debug().Str("deposit", d.ID.String()).Msg("deposit ledger entry already posted")
		return nil
	}

	// Resolve accounts
	chainUpper := strings.ToUpper(d.Chain)
	assetCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, d.TokenSymbol)
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", d.TokenSymbol)

	assetAccount, err := s.ledgerRepo.FindAccountByCode(ctx, assetCode)
	if err != nil {
		// Auto-create if missing
		assetAccount = &domain.LedgerAccount{
			ID:       uuid.New(),
			Code:     assetCode,
			Name:     fmt.Sprintf("%s Hot Wallet %s", d.Chain, d.TokenSymbol),
			Type:     domain.AccountAsset,
			Chain:    &d.Chain,
			Currency: d.TokenSymbol,
			IsActive: true,
		}
		if err := s.ledgerRepo.CreateAccount(ctx, assetAccount); err != nil {
			ledgerPostingErrors.WithLabelValues("DEPOSIT", "account_create").Inc()
			return fmt.Errorf("create asset account %s: %w", assetCode, err)
		}
		// Re-fetch to get the ID (in case ON CONFLICT hit)
		assetAccount, _ = s.ledgerRepo.FindAccountByCode(ctx, assetCode)
	}

	liabilityAccount, err := s.ledgerRepo.FindAccountByCode(ctx, liabilityCode)
	if err != nil {
		liabilityAccount = &domain.LedgerAccount{
			ID:       uuid.New(),
			Code:     liabilityCode,
			Name:     fmt.Sprintf("User %s Balances", d.TokenSymbol),
			Type:     domain.AccountLiability,
			Currency: d.TokenSymbol,
			IsActive: true,
		}
		if err := s.ledgerRepo.CreateAccount(ctx, liabilityAccount); err != nil {
			ledgerPostingErrors.WithLabelValues("DEPOSIT", "account_create").Inc()
			return fmt.Errorf("create liability account %s: %w", liabilityCode, err)
		}
		liabilityAccount, _ = s.ledgerRepo.FindAccountByCode(ctx, liabilityCode)
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
// Debit:  LIABILITY:USER:{TOKEN}     (reduce user balance)
// Credit: ASSET:HOT:{CHAIN}:{TOKEN}  (tokens left hot wallet)
func (s *LedgerService) PostWithdrawalEntry(ctx context.Context, w *domain.Withdrawal) error {
	idempotencyKey := fmt.Sprintf("withdrawal:%s", w.ID.String())

	if _, err := s.ledgerRepo.FindEntryByIdempotencyKey(ctx, idempotencyKey); err == nil {
		log.Debug().Str("withdrawal", w.ID.String()).Msg("withdrawal ledger entry already posted")
		return nil
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

	// Add fee line if applicable
	if w.Fee != nil && w.Fee.Sign() > 0 {
		feeCode := fmt.Sprintf("REVENUE:WITHDRAWAL_FEE:%s", w.TokenSymbol)
		feeAccount, err := s.ledgerRepo.FindAccountByCode(ctx, feeCode)
		if err == nil {
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
// Debit:  ASSET:HOT:{CHAIN}:{TOKEN}      (hot wallet receives)
// Credit: ASSET:DEPOSIT:{CHAIN}:{TOKEN}   (deposit address releases)
func (s *LedgerService) PostSweepEntry(ctx context.Context, sweep *domain.SweepRecord, tokenSymbol string) error {
	idempotencyKey := fmt.Sprintf("sweep:%s", sweep.ID.String())

	if _, err := s.ledgerRepo.FindEntryByIdempotencyKey(ctx, idempotencyKey); err == nil {
		return nil
	}

	chainUpper := strings.ToUpper(sweep.Chain)
	hotCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, tokenSymbol)
	depositCode := fmt.Sprintf("ASSET:DEPOSIT:%s:%s", chainUpper, tokenSymbol)

	hotAccount, err := s.ledgerRepo.FindAccountByCode(ctx, hotCode)
	if err != nil {
		return fmt.Errorf("hot account %s not found: %w", hotCode, err)
	}

	depositAccount, err := s.ledgerRepo.FindAccountByCode(ctx, depositCode)
	if err != nil {
		// Auto-create deposit asset account
		depositAccount = &domain.LedgerAccount{
			ID:       uuid.New(),
			Code:     depositCode,
			Name:     fmt.Sprintf("%s Deposit Addresses %s", sweep.Chain, tokenSymbol),
			Type:     domain.AccountAsset,
			Chain:    &sweep.Chain,
			Currency: tokenSymbol,
			IsActive: true,
		}
		if err := s.ledgerRepo.CreateAccount(ctx, depositAccount); err != nil {
			return fmt.Errorf("create deposit account: %w", err)
		}
		depositAccount, _ = s.ledgerRepo.FindAccountByCode(ctx, depositCode)
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

	if err := s.ledgerRepo.PostEntry(ctx, entry); err != nil {
		ledgerPostingErrors.WithLabelValues("SWEEP", "post").Inc()
		return fmt.Errorf("post sweep entry: %w", err)
	}

	ledgerEntriesPosted.WithLabelValues("SWEEP").Inc()
	return nil
}

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

// VerifyIntegrity checks the hash chain integrity of the ledger
func (s *LedgerService) VerifyIntegrity(ctx context.Context) (int, error) {
	return s.ledgerRepo.VerifyHashChain(ctx)
}
