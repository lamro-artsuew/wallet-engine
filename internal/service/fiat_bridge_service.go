package service

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
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

const cryptoPrecision = 100000000 // 10^8 for crypto amount conversion

// FiatBridgeService bridges fiat operations with the double-entry ledger
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
func (s *FiatBridgeService) RecordFiatDeposit(ctx context.Context, workspaceID uuid.UUID, currency string, amount float64, reference string) (*domain.FiatTransaction, error) {
	currency = strings.ToUpper(currency)

	if amount <= 0 {
		return nil, fmt.Errorf("deposit amount must be positive")
	}

	// Find or auto-create operating fiat account
	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, workspaceID, currency)
	if err != nil {
		fiatAccount = &domain.FiatAccount{
			ID:          uuid.New(),
			WorkspaceID: workspaceID,
			Currency:    currency,
			Provider:    "BANK",
			AccountType: "OPERATING",
			Balance:     0,
			IsActive:    true,
		}
		if err := s.fiatRepo.CreateFiatAccount(ctx, fiatAccount); err != nil {
			fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "error").Inc()
			return nil, fmt.Errorf("create fiat account: %w", err)
		}
	}

	txID := uuid.New()

	// Post ledger entry
	idempotencyKey := fmt.Sprintf("fiat_deposit:%s", txID.String())
	assetCode := fmt.Sprintf("ASSET:FIAT:%s", currency)
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", currency)
	description := fmt.Sprintf("Fiat deposit %.2f %s ref:%s", amount, currency, reference)
	amountCents := fiatToCents(amount)

	refType := "fiat_deposit"
	entryID, err := s.ledgerSvc.PostFiatEntry(ctx, idempotencyKey, "FIAT_DEPOSIT", description,
		assetCode, liabilityCode, currency, amountCents, refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "error").Inc()
		return nil, fmt.Errorf("post fiat deposit ledger entry: %w", err)
	}

	// Create fiat transaction record
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

	// Update fiat account balance
	newBalance := fiatAccount.Balance + amount
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		log.Warn().Err(err).Str("account_id", fiatAccount.ID.String()).Msg("failed to update fiat account balance")
	}

	fiatOpsTotal.WithLabelValues("DEPOSIT", currency, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("currency", currency).
		Float64("amount", amount).
		Msg("fiat deposit recorded")

	return tx, nil
}

// RecordFiatWithdrawal records a fiat withdrawal and creates ledger entries.
// Debit: LIABILITY:USER:{CURRENCY} (reduce user balance)
// Credit: ASSET:FIAT:{CURRENCY} (fiat leaves our custody)
func (s *FiatBridgeService) RecordFiatWithdrawal(ctx context.Context, workspaceID uuid.UUID, currency string, amount float64, reference string) (*domain.FiatTransaction, error) {
	currency = strings.ToUpper(currency)

	if amount <= 0 {
		return nil, fmt.Errorf("withdrawal amount must be positive")
	}

	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, workspaceID, currency)
	if err != nil {
		return nil, fmt.Errorf("no fiat account found: %w", err)
	}

	if fiatAccount.Balance < amount {
		return nil, fmt.Errorf("insufficient balance: have %.2f, need %.2f", fiatAccount.Balance, amount)
	}

	txID := uuid.New()

	idempotencyKey := fmt.Sprintf("fiat_withdrawal:%s", txID.String())
	liabilityCode := fmt.Sprintf("LIABILITY:USER:%s", currency)
	assetCode := fmt.Sprintf("ASSET:FIAT:%s", currency)
	description := fmt.Sprintf("Fiat withdrawal %.2f %s ref:%s", amount, currency, reference)
	amountCents := fiatToCents(amount)

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

	newBalance := fiatAccount.Balance - amount
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		log.Warn().Err(err).Str("account_id", fiatAccount.ID.String()).Msg("failed to update fiat account balance")
	}

	fiatOpsTotal.WithLabelValues("WITHDRAWAL", currency, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("currency", currency).
		Float64("amount", amount).
		Msg("fiat withdrawal recorded")

	return tx, nil
}

// ConvertCryptoToFiat records a crypto→fiat conversion.
// Creates two balanced ledger entries:
// 1. Crypto side: Debit LIABILITY:USER:{CRYPTO}, Credit ASSET:CONVERSION:{CRYPTO}
// 2. Fiat side: Debit ASSET:FIAT:{FIAT}, Credit LIABILITY:USER:{FIAT}
func (s *FiatBridgeService) ConvertCryptoToFiat(ctx context.Context, req domain.ConversionRequest) (*domain.FiatTransaction, error) {
	from := strings.ToUpper(req.FromCurrency)
	to := strings.ToUpper(req.ToCurrency)

	if req.Amount <= 0 {
		return nil, fmt.Errorf("conversion amount must be positive")
	}

	rate, err := s.fiatRepo.GetLatestRate(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("get conversion rate: %w", err)
	}

	fiatAmount := req.Amount * rate.Rate
	txID := uuid.New()
	refType := "fiat_conversion"

	// Crypto side: reduce user crypto liability
	cryptoIdempotencyKey := fmt.Sprintf("conversion_crypto:%s", txID.String())
	cryptoDebitCode := fmt.Sprintf("LIABILITY:USER:%s", from)
	cryptoCreditCode := fmt.Sprintf("ASSET:CONVERSION:%s", from)
	cryptoDesc := fmt.Sprintf("Conversion sell %.8f %s for %.2f %s", req.Amount, from, fiatAmount, to)
	cryptoAmount := cryptoToBigInt(req.Amount)

	if _, err := s.ledgerSvc.PostFiatEntry(ctx, cryptoIdempotencyKey, "FIAT_CONVERSION", cryptoDesc,
		cryptoDebitCode, cryptoCreditCode, from, cryptoAmount, refType, &txID); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", to, "error").Inc()
		return nil, fmt.Errorf("post crypto side entry: %w", err)
	}

	// Fiat side: credit user fiat liability
	fiatIdempotencyKey := fmt.Sprintf("conversion_fiat:%s", txID.String())
	fiatDebitCode := fmt.Sprintf("ASSET:FIAT:%s", to)
	fiatCreditCode := fmt.Sprintf("LIABILITY:USER:%s", to)
	fiatDesc := fmt.Sprintf("Conversion credit %.2f %s from %.8f %s", fiatAmount, to, req.Amount, from)
	fiatAmountCents := fiatToCents(fiatAmount)

	entryID, err := s.ledgerSvc.PostFiatEntry(ctx, fiatIdempotencyKey, "FIAT_CONVERSION", fiatDesc,
		fiatDebitCode, fiatCreditCode, to, fiatAmountCents, refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", to, "error").Inc()
		return nil, fmt.Errorf("post fiat side entry: %w", err)
	}

	// Find or create fiat account for balance tracking
	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, req.WorkspaceID, to)
	if err != nil {
		fiatAccount = &domain.FiatAccount{
			ID:          uuid.New(),
			WorkspaceID: req.WorkspaceID,
			Currency:    to,
			Provider:    "BANK",
			AccountType: "OPERATING",
			Balance:     0,
			IsActive:    true,
		}
		if err := s.fiatRepo.CreateFiatAccount(ctx, fiatAccount); err != nil {
			return nil, fmt.Errorf("create fiat account: %w", err)
		}
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
			"crypto_amount": req.Amount,
			"fiat_amount":   fiatAmount,
			"rate":          rate.Rate,
			"direction":     "SELL",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", to, "error").Inc()
		return nil, fmt.Errorf("create conversion transaction: %w", err)
	}

	newBalance := fiatAccount.Balance + fiatAmount
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		log.Warn().Err(err).Msg("failed to update fiat balance after conversion")
	}

	conversionTotal.WithLabelValues(from, to).Inc()
	fiatOpsTotal.WithLabelValues("CONVERSION", to, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("from", from).
		Str("to", to).
		Float64("crypto_amount", req.Amount).
		Float64("fiat_amount", fiatAmount).
		Float64("rate", rate.Rate).
		Msg("crypto to fiat conversion recorded")

	return tx, nil
}

// ConvertFiatToCrypto records a fiat→crypto conversion.
// Creates two balanced ledger entries:
// 1. Fiat side: Debit LIABILITY:USER:{FIAT}, Credit ASSET:FIAT:{FIAT}
// 2. Crypto side: Debit ASSET:CONVERSION:{CRYPTO}, Credit LIABILITY:USER:{CRYPTO}
func (s *FiatBridgeService) ConvertFiatToCrypto(ctx context.Context, req domain.ConversionRequest) (*domain.FiatTransaction, error) {
	from := strings.ToUpper(req.FromCurrency)
	to := strings.ToUpper(req.ToCurrency)

	if req.Amount <= 0 {
		return nil, fmt.Errorf("conversion amount must be positive")
	}

	rate, err := s.fiatRepo.GetLatestRate(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("get conversion rate: %w", err)
	}

	cryptoAmount := req.Amount * rate.Rate
	txID := uuid.New()

	// Check fiat balance
	fiatAccount, err := s.fiatRepo.FindOperatingAccount(ctx, req.WorkspaceID, from)
	if err != nil {
		return nil, fmt.Errorf("no fiat account found: %w", err)
	}
	if fiatAccount.Balance < req.Amount {
		return nil, fmt.Errorf("insufficient fiat balance: have %.2f, need %.2f", fiatAccount.Balance, req.Amount)
	}

	refType := "fiat_conversion"

	// Fiat side: reduce user fiat liability
	fiatIdempotencyKey := fmt.Sprintf("conversion_fiat:%s", txID.String())
	fiatDebitCode := fmt.Sprintf("LIABILITY:USER:%s", from)
	fiatCreditCode := fmt.Sprintf("ASSET:FIAT:%s", from)
	fiatDesc := fmt.Sprintf("Conversion buy %.8f %s for %.2f %s", cryptoAmount, to, req.Amount, from)
	fiatAmountCents := fiatToCents(req.Amount)

	if _, err := s.ledgerSvc.PostFiatEntry(ctx, fiatIdempotencyKey, "FIAT_CONVERSION", fiatDesc,
		fiatDebitCode, fiatCreditCode, from, fiatAmountCents, refType, &txID); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", from, "error").Inc()
		return nil, fmt.Errorf("post fiat side entry: %w", err)
	}

	// Crypto side: credit user crypto liability
	cryptoIdempotencyKey := fmt.Sprintf("conversion_crypto:%s", txID.String())
	cryptoDebitCode := fmt.Sprintf("ASSET:CONVERSION:%s", to)
	cryptoCreditCode := fmt.Sprintf("LIABILITY:USER:%s", to)
	cryptoDesc := fmt.Sprintf("Conversion credit %.8f %s from %.2f %s", cryptoAmount, to, req.Amount, from)
	cryptoBigInt := cryptoToBigInt(cryptoAmount)

	entryID, err := s.ledgerSvc.PostFiatEntry(ctx, cryptoIdempotencyKey, "FIAT_CONVERSION", cryptoDesc,
		cryptoDebitCode, cryptoCreditCode, to, cryptoBigInt, refType, &txID)
	if err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", from, "error").Inc()
		return nil, fmt.Errorf("post crypto side entry: %w", err)
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
			"fiat_amount":   req.Amount,
			"crypto_amount": cryptoAmount,
			"rate":          rate.Rate,
			"direction":     "BUY",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.fiatRepo.CreateTransaction(ctx, tx); err != nil {
		fiatOpsTotal.WithLabelValues("CONVERSION", from, "error").Inc()
		return nil, fmt.Errorf("create conversion transaction: %w", err)
	}

	newBalance := fiatAccount.Balance - req.Amount
	if err := s.fiatRepo.UpdateBalance(ctx, fiatAccount.ID, newBalance); err != nil {
		log.Warn().Err(err).Msg("failed to update fiat balance after conversion")
	}

	conversionTotal.WithLabelValues(from, to).Inc()
	fiatOpsTotal.WithLabelValues("CONVERSION", from, "success").Inc()
	log.Info().
		Str("tx_id", txID.String()).
		Str("from", from).
		Str("to", to).
		Float64("fiat_amount", req.Amount).
		Float64("crypto_amount", cryptoAmount).
		Float64("rate", rate.Rate).
		Msg("fiat to crypto conversion recorded")

	return tx, nil
}

// GetRate returns the latest conversion rate for a currency pair
func (s *FiatBridgeService) GetRate(ctx context.Context, from, to string) (*domain.ConversionRate, error) {
	return s.fiatRepo.GetLatestRate(ctx, strings.ToUpper(from), strings.ToUpper(to))
}

// SetRate manually sets a conversion rate
func (s *FiatBridgeService) SetRate(ctx context.Context, from, to string, rate float64, source string) error {
	if rate <= 0 {
		return fmt.Errorf("rate must be positive")
	}
	if source == "" {
		source = "MANUAL"
	}

	r := &domain.ConversionRate{
		ID:           uuid.New(),
		FromCurrency: strings.ToUpper(from),
		ToCurrency:   strings.ToUpper(to),
		Rate:         rate,
		Source:       source,
		ValidFrom:    time.Now(),
		CreatedAt:    time.Now(),
	}
	return s.fiatRepo.SaveRate(ctx, r)
}

// fiatToCents converts a fiat amount to cents as *big.Int
func fiatToCents(amount float64) *big.Int {
	cents := int64(math.Round(amount * 100))
	return big.NewInt(cents)
}

// cryptoToBigInt converts a crypto amount to its smallest unit (10^8 precision)
func cryptoToBigInt(amount float64) *big.Int {
	units := int64(math.Round(amount * cryptoPrecision))
	return big.NewInt(units)
}
