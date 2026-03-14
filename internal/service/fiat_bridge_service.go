package service

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

var (
	fiatOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_fiat_operations_total",
		Help: "Total fiat operations by type, currency, and result",
	}, []string{"type", "currency", "result"})

	conversionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_conversions_total",
		Help: "Total currency conversions",
	}, []string{"from_currency", "to_currency"})
)

const (
	// maxCryptoRateAge is the maximum age of a conversion rate for crypto pairs.
	// Rates older than this are rejected to prevent stale-rate exploitation.
	maxCryptoRateAge = 5 * time.Minute

	// maxRateDeltaPercent is the maximum allowed rate change vs the previous rate.
	// Prevents misconfigured rates from producing absurd ledger entries.
	maxRateDeltaPercent = 50

	// cryptoSubunitExponent is the exponent for crypto → subunit conversion (10^8 = satoshi-scale).
	cryptoSubunitExponent = 8
)

// FiatBridgeService bridges fiat operations with the double-entry ledger.
// All financial amounts use decimal.Decimal — never float64 — to prevent
// IEEE 754 representation errors in regulated financial operations.
type FiatBridgeService struct {
	fiatRepo  *repository.FiatRepo
	ledgerSvc *LedgerService
}

// NewFiatBridgeService creates a new fiat bridge service
func NewFiatBridgeService(fiatRepo *repository.FiatRepo, ledgerSvc *LedgerService) *FiatBridgeService {
	return &FiatBridgeService{
		fiatRepo:  fiatRepo,
		ledgerSvc: ledgerSvc,
	}
}

// RecordFiatDeposit records a fiat deposit and creates ledger entries.
// Debit: ASSET:FIAT:{CURRENCY} (we received fiat)
// Credit: LIABILITY:USER:{CURRENCY} (we owe user fiat)
func (s *FiatBridgeService) RecordFiatDeposit(ctx context.Context, workspaceID uuid.UUID, currency string, amount decimal.Decimal, reference string) (*domain.FiatTransaction, error) {
	currency = strings.ToUpper(currency)

	if !amount.IsPositive() {
		return nil, fmt.Errorf("deposit amount must be positive")
	}

	fiatAccount, err := s.findOrCreateAccount(ctx, workspaceID, currency)
	if err != nil {
		fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "error").Inc()
		return nil, err
	}

	txID := uuid.New()
	idempotencyKey := fmt.Sprintf("fiat_deposit:%s", txID.String())
	assetCode := fmt.Sprintf("ASSET:FIAT:%s", currency)
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", currency)
	description := fmt.Sprintf("Fiat deposit %s %s ref:%s", amount.StringFixed(2), currency, reference)
	amountCents := decimalToCents(amount)

	refType := "fiat_deposit"
	entryID, err := s.ledgerSvc.PostFiatEntry(ctx, idempotencyKey, "FIAT_DEPOSIT", description,
		assetCode, liabilityCode, currency, amountCents, refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "error").Inc()
		return nil, fmt.Errorf("post fiat deposit ledger entry: %w", err)
	}

	ref := reference
	tx := &domain.FiatTransaction{
		ID:            txID,
		WorkspaceID:   workspaceID,
		FiatAccountID: fiatAccount.ID,
		Type:          domain.FiatDeposit,
		Currency:      currency,
		Amount:        amount,
		Reference:     &ref,
		State:         domain.FiatStateCompleted,
		LedgerEntryID: entryID,
		Metadata:      map[string]interface{}{},
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "error").Inc()
		return nil, fmt.Errorf("create fiat transaction: %w", err)
	}

	// Balance update is mandatory — failure means data inconsistency
	newBalance := fiatAccount.Balance.Add(amount)
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		return nil, fmt.Errorf("update fiat balance after deposit: %w", err)
	}

	fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("currency", currency).
		Str("amount", amount.String()).
		Msg("fiat deposit recorded")

	return tx, nil
}

// RecordFiatWithdrawal records a fiat withdrawal and creates ledger entries.
// Debit: LIABILITY:USER:{CURRENCY} (reduce user balance)
// Credit: ASSET:FIAT:{CURRENCY} (fiat leaves our custody)
func (s *FiatBridgeService) RecordFiatWithdrawal(ctx context.Context, workspaceID uuid.UUID, currency string, amount decimal.Decimal, reference string) (*domain.FiatTransaction, error) {
	currency = strings.ToUpper(currency)

	if !amount.IsPositive() {
		return nil, fmt.Errorf("withdrawal amount must be positive")
	}

	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, workspaceID, currency)
	if err != nil {
		return nil, fmt.Errorf("no fiat account found: %w", err)
	}

	if fiatAccount.Balance.LessThan(amount) {
		return nil, fmt.Errorf("insufficient balance: have %s, need %s", fiatAccount.Balance.StringFixed(2), amount.StringFixed(2))
	}

	txID := uuid.New()
	idempotencyKey := fmt.Sprintf("fiat_withdrawal:%s", txID.String())
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", currency)
	assetCode := fmt.Sprintf("ASSET:FIAT:%s", currency)
	description := fmt.Sprintf("Fiat withdrawal %s %s ref:%s", amount.StringFixed(2), currency, reference)
	amountCents := decimalToCents(amount)

	refType := "fiat_withdrawal"
	entryID, err := s.ledgerSvc.PostFiatEntry(ctx, idempotencyKey, "FIAT_WITHDRAWAL", description,
		liabilityCode, assetCode, currency, amountCents, refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("WITHDRAWAL", currency, "error").Inc()
		return nil, fmt.Errorf("post fiat withdrawal ledger entry: %w", err)
	}

	ref := reference
	tx := &domain.FiatTransaction{
		ID:            txID,
		WorkspaceID:   workspaceID,
		FiatAccountID: fiatAccount.ID,
		Type:          domain.FiatWithdrawal,
		Currency:      currency,
		Amount:        amount,
		Reference:     &ref,
		State:         domain.FiatStateCompleted,
		LedgerEntryID: entryID,
		Metadata:      map[string]interface{}{},
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("WITHDRAWAL", currency, "error").Inc()
		return nil, fmt.Errorf("create fiat transaction: %w", err)
	}

	newBalance := fiatAccount.Balance.Sub(amount)
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		return nil, fmt.Errorf("update fiat balance after withdrawal: %w", err)
	}

	fiatOpsTotal.WithLabelValues("WITHDRAWAL", currency, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("currency", currency).
		Str("amount", amount.String()).
		Msg("fiat withdrawal recorded")

	return tx, nil
}

// ConvertCryptoToFiat records a crypto→fiat conversion using a single atomic 4-line
// journal entry. Both legs (crypto debit + fiat credit) are posted atomically —
// if either fails, neither commits, preventing fund loss from split-brain state.
//
// Ledger lines:
//   Debit  LIABILITY:USER:{CRYPTO}      — user's crypto obligation decreases
//   Credit ASSET:CUSTODY:EXCHANGE:{CRYPTO} — exchange received crypto for sale
//   Debit  ASSET:FIAT:{FIAT}            — exchange received fiat proceeds
//   Credit LIABILITY:USER:{FIAT}        — user's fiat obligation increases
func (s *FiatBridgeService) ConvertCryptoToFiat(ctx context.Context, req domain.ConversionRequest) (*domain.FiatTransaction, error) {
	from := strings.ToUpper(req.FromCurrency)
	to := strings.ToUpper(req.ToCurrency)

	if !req.Amount.IsPositive() {
		return nil, fmt.Errorf("conversion amount must be positive")
	}

	rate, err := s.getValidRate(ctx, from, to)
	if err != nil {
		return nil, err
	}

	fiatAmount := req.Amount.Mul(rate.Rate)
	txID := uuid.New()

	// Build single atomic 4-line conversion entry
	idempotencyKey := fmt.Sprintf("conversion:%s", txID.String())
	description := fmt.Sprintf("Sell %s %s → %s %s @ %s", req.Amount.String(), from, fiatAmount.StringFixed(2), to, rate.Rate.String())

	cryptoDebitCode := fmt.Sprintf("LIABILITY:USER:%s", from)
	cryptoCreditCode := fmt.Sprintf("ASSET:CUSTODY:EXCHANGE:%s", from)
	fiatDebitCode := fmt.Sprintf("ASSET:FIAT:%s", to)
	fiatCreditCode := fmt.Sprintf("LIABILITY:USER:%s", to)

	cryptoUnits := decimalToCryptoUnits(req.Amount, cryptoSubunitExponent)
	fiatCents := decimalToCents(fiatAmount)
	refType := "fiat_conversion"

	entryID, err := s.ledgerSvc.PostConversionEntry(ctx, idempotencyKey, description,
		cryptoDebitCode, cryptoCreditCode, from, cryptoUnits,
		fiatDebitCode, fiatCreditCode, to, fiatCents,
		refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", to, "error").Inc()
		return nil, fmt.Errorf("post conversion entry: %w", err)
	}

	fiatAccount, err := s.findOrCreateAccount(ctx, req.WorkspaceID, to)
	if err != nil {
		return nil, err
	}

	tx := &domain.FiatTransaction{
		ID:            txID,
		WorkspaceID:   req.WorkspaceID,
		FiatAccountID: fiatAccount.ID,
		Type:          domain.FiatConversion,
		Currency:      to,
		Amount:        fiatAmount,
		State:         domain.FiatStateCompleted,
		LedgerEntryID: entryID,
		Metadata: map[string]interface{}{
			"from_currency": from,
			"to_currency":   to,
			"crypto_amount": req.Amount.String(),
			"fiat_amount":   fiatAmount.String(),
			"rate":          rate.Rate.String(),
			"direction":     "SELL",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", to, "error").Inc()
		return nil, fmt.Errorf("create conversion transaction: %w", err)
	}

	newBalance := fiatAccount.Balance.Add(fiatAmount)
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		return nil, fmt.Errorf("update fiat balance after conversion: %w", err)
	}

	conversionTotal.WithLabelValues(from, to).Inc()
	fiatOpsTotal.WithLabelValues("CONVERSION", to, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("from", from).
		Str("to", to).
		Str("crypto_amount", req.Amount.String()).
		Str("fiat_amount", fiatAmount.String()).
		Str("rate", rate.Rate.String()).
		Msg("crypto to fiat conversion recorded")

	return tx, nil
}

// ConvertFiatToCrypto records a fiat→crypto conversion using a single atomic 4-line
// journal entry.
//
// Ledger lines:
//   Debit  LIABILITY:USER:{FIAT}            — user's fiat obligation decreases
//   Credit ASSET:FIAT:{FIAT}                — fiat leaves custody for purchase
//   Debit  ASSET:CUSTODY:EXCHANGE:{CRYPTO}  — exchange acquires crypto
//   Credit LIABILITY:USER:{CRYPTO}          — user's crypto obligation increases
func (s *FiatBridgeService) ConvertFiatToCrypto(ctx context.Context, req domain.ConversionRequest) (*domain.FiatTransaction, error) {
	from := strings.ToUpper(req.FromCurrency)
	to := strings.ToUpper(req.ToCurrency)

	if !req.Amount.IsPositive() {
		return nil, fmt.Errorf("conversion amount must be positive")
	}

	rate, err := s.getValidRate(ctx, from, to)
	if err != nil {
		return nil, err
	}

	cryptoAmount := req.Amount.Mul(rate.Rate)

	// Check fiat balance
	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, req.WorkspaceID, from)
	if err != nil {
		return nil, fmt.Errorf("no fiat account found: %w", err)
	}
	if fiatAccount.Balance.LessThan(req.Amount) {
		return nil, fmt.Errorf("insufficient fiat balance: have %s, need %s", fiatAccount.Balance.StringFixed(2), req.Amount.StringFixed(2))
	}

	txID := uuid.New()

	idempotencyKey := fmt.Sprintf("conversion:%s", txID.String())
	description := fmt.Sprintf("Buy %s %s ← %s %s @ %s", cryptoAmount.String(), to, req.Amount.StringFixed(2), from, rate.Rate.String())

	fiatDebitCode := fmt.Sprintf("LIABILITY:USER:%s", from)
	fiatCreditCode := fmt.Sprintf("ASSET:FIAT:%s", from)
	cryptoDebitCode := fmt.Sprintf("ASSET:CUSTODY:EXCHANGE:%s", to)
	cryptoCreditCode := fmt.Sprintf("LIABILITY:USER:%s", to)

	fiatCents := decimalToCents(req.Amount)
	cryptoUnits := decimalToCryptoUnits(cryptoAmount, cryptoSubunitExponent)
	refType := "fiat_conversion"

	entryID, err := s.ledgerSvc.PostConversionEntry(ctx, idempotencyKey, description,
		fiatDebitCode, fiatCreditCode, from, fiatCents,
		cryptoDebitCode, cryptoCreditCode, to, cryptoUnits,
		refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", from, "error").Inc()
		return nil, fmt.Errorf("post conversion entry: %w", err)
	}

	tx := &domain.FiatTransaction{
		ID:            txID,
		WorkspaceID:   req.WorkspaceID,
		FiatAccountID: fiatAccount.ID,
		Type:          domain.FiatConversion,
		Currency:      from,
		Amount:        req.Amount,
		State:         domain.FiatStateCompleted,
		LedgerEntryID: entryID,
		Metadata: map[string]interface{}{
			"from_currency": from,
			"to_currency":   to,
			"fiat_amount":   req.Amount.String(),
			"crypto_amount": cryptoAmount.String(),
			"rate":          rate.Rate.String(),
			"direction":     "BUY",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", from, "error").Inc()
		return nil, fmt.Errorf("create conversion transaction: %w", err)
	}

	newBalance := fiatAccount.Balance.Sub(req.Amount)
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		return nil, fmt.Errorf("update fiat balance after conversion: %w", err)
	}

	conversionTotal.WithLabelValues(from, to).Inc()
	fiatOpsTotal.WithLabelValues("CONVERSION", from, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("from", from).
		Str("to", to).
		Str("fiat_amount", req.Amount.String()).
		Str("crypto_amount", cryptoAmount.String()).
		Str("rate", rate.Rate.String()).
		Msg("fiat to crypto conversion recorded")

	return tx, nil
}

// GetRate returns the latest conversion rate for a currency pair
func (s *FiatBridgeService) GetRate(ctx context.Context, from, to string) (*domain.ConversionRate, error) {
	return s.fiatRepo.GetLatestRate(ctx, strings.ToUpper(from), strings.ToUpper(to))
}

// SetRate manually sets a conversion rate with bounds validation.
// Rejects rates with >50% delta from the previous rate (if one exists)
// to prevent misconfigured rates from producing absurd ledger entries.
func (s *FiatBridgeService) SetRate(ctx context.Context, from, to string, rate decimal.Decimal, source string) error {
	if !rate.IsPositive() {
		return fmt.Errorf("rate must be positive")
	}
	if source == "" {
		source = "MANUAL"
	}

	from = strings.ToUpper(from)
	to = strings.ToUpper(to)

	// Bounds check against previous rate
	prevRate, err := s.fiatRepo.GetLatestRate(ctx, from, to)
	if err == nil {
		delta := rate.Sub(prevRate.Rate).Abs()
		maxDelta := prevRate.Rate.Mul(decimal.NewFromInt(maxRateDeltaPercent)).Div(decimal.NewFromInt(100))
		if delta.GreaterThan(maxDelta) {
			return fmt.Errorf("rate change too large: previous=%s, new=%s, max delta=%d%%",
				prevRate.Rate.String(), rate.String(), maxRateDeltaPercent)
		}
	}
	// If no previous rate, any positive rate is valid (first-time setup)

	r := &domain.ConversionRate{
		ID:           uuid.New(),
		FromCurrency: from,
		ToCurrency:   to,
		Rate:         rate,
		Source:       source,
		ValidFrom:    time.Now(),
		CreatedAt:    time.Now(),
	}
	return s.fiatRepo.SaveRate(ctx, r)
}

// --- Internal helpers ---

// findOrCreateAccount locates or auto-creates an operating fiat account
func (s *FiatBridgeService) findOrCreateAccount(ctx context.Context, workspaceID uuid.UUID, currency string) (*domain.FiatAccount, error) {
	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, workspaceID, currency)
	if err != nil {
		fiatAccount = &domain.FiatAccount{
			ID:          uuid.New(),
			WorkspaceID: workspaceID,
			Currency:    currency,
			Provider:    "BANK",
			AccountType: "OPERATING",
			Balance:     decimal.Zero,
			IsActive:    true,
		}
		if err := s.fiatRepo.CreateFiatAccount(ctx, fiatAccount); err != nil {
			return nil, fmt.Errorf("create fiat account: %w", err)
		}
	}
	return fiatAccount, nil
}

// getValidRate fetches the latest rate and validates freshness.
// Rejects rates older than maxCryptoRateAge for crypto conversions.
func (s *FiatBridgeService) getValidRate(ctx context.Context, from, to string) (*domain.ConversionRate, error) {
	rate, err := s.fiatRepo.GetLatestRate(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("get conversion rate: %w", err)
	}

	age := time.Since(rate.ValidFrom)
	if age > maxCryptoRateAge {
		return nil, fmt.Errorf("rate for %s/%s is stale (age %s, max %s) — refresh rate before converting",
			from, to, age.Truncate(time.Second), maxCryptoRateAge)
	}

	return rate, nil
}

// decimalToCents converts a decimal fiat amount to cents as *big.Int.
// Uses decimal arithmetic exclusively — never passes through float64.
func decimalToCents(amount decimal.Decimal) *big.Int {
	cents := amount.Mul(decimal.NewFromInt(100)).BigInt()
	return cents
}

// decimalToCryptoUnits converts a decimal crypto amount to its smallest unit.
// exponent is the number of decimal places (e.g., 8 for Bitcoin satoshis).
// Uses decimal arithmetic exclusively — handles amounts that overflow int64.
func decimalToCryptoUnits(amount decimal.Decimal, exponent int32) *big.Int {
	multiplier := decimal.New(1, exponent) // 10^exponent
	return amount.Mul(multiplier).BigInt()
}
