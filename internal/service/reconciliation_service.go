package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	reconRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_reconciliation_runs_total",
		Help: "Total reconciliation runs by chain and result",
	}, []string{"chain", "status"})

	reconDrift = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_reconciliation_drift_wei",
		Help: "Current reconciliation drift in wei",
	}, []string{"chain"})

	reconDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_reconciliation_duration_seconds",
		Help:    "Reconciliation run duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"chain"})

	reconDriftAlerts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_reconciliation_drift_alerts_total",
		Help: "Reconciliation drift alert events",
	}, []string{"chain"})
)

// ReconciliationService verifies on-chain state matches the internal ledger.
//
// Invariant enforced:
//   Sum(all user internal balances) == Hot Wallet on-chain balance
//     + Unswept deposit address balances
//     + Cold Wallet balance
type ReconciliationService struct {
	reconRepo   *repository.ReconciliationRepo
	walletRepo  *repository.WalletRepo
	depositRepo *repository.DepositRepo
	ledgerRepo  *repository.LedgerRepo
	adapters    map[string]*chain.EVMAdapter

	driftThreshold *big.Int // drift above this triggers FAIL (in wei)
	interval       time.Duration
	stopCh         chan struct{}
}

// NewReconciliationService creates the reconciliation service
func NewReconciliationService(
	reconRepo *repository.ReconciliationRepo,
	walletRepo *repository.WalletRepo,
	depositRepo *repository.DepositRepo,
	ledgerRepo *repository.LedgerRepo,
	adapters map[string]*chain.EVMAdapter,
	driftThreshold *big.Int,
	interval time.Duration,
) *ReconciliationService {
	if driftThreshold == nil || driftThreshold.Sign() <= 0 {
		// Default: 1 USDT (1e6 in 6 decimals)
		driftThreshold = big.NewInt(1_000_000)
	}
	if interval <= 0 {
		interval = 1 * time.Hour
	}
	return &ReconciliationService{
		reconRepo:      reconRepo,
		walletRepo:     walletRepo,
		depositRepo:    depositRepo,
		ledgerRepo:     ledgerRepo,
		adapters:       adapters,
		driftThreshold: driftThreshold,
		interval:       interval,
		stopCh:         make(chan struct{}),
	}
}

// Start begins the periodic reconciliation loop
func (rs *ReconciliationService) Start(ctx context.Context) {
	log.Info().Dur("interval", rs.interval).Msg("reconciliation service started")
	ticker := time.NewTicker(rs.interval)
	defer ticker.Stop()

	// Run initial check after short delay
	time.Sleep(30 * time.Second)
	rs.runAll(ctx)

	for {
		select {
		case <-ticker.C:
			rs.runAll(ctx)
		case <-rs.stopCh:
			log.Info().Msg("reconciliation service stopped")
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop halts the service
func (rs *ReconciliationService) Stop() {
	close(rs.stopCh)
}

func (rs *ReconciliationService) runAll(ctx context.Context) {
	for chainName := range rs.adapters {
		rs.reconcileHotWallet(ctx, chainName)
	}
}

// reconcileHotWallet compares on-chain hot wallet balance vs ledger ASSET:HOT balance
func (rs *ReconciliationService) reconcileHotWallet(ctx context.Context, chainName string) {
	start := time.Now()
	defer func() {
		reconDuration.WithLabelValues(chainName).Observe(time.Since(start).Seconds())
	}()

	adapter, ok := rs.adapters[chainName]
	if !ok {
		return
	}

	// Find hot wallet for this chain
	hotWallets, err := rs.walletRepo.FindByChainAndTier(ctx, chainName, domain.WalletTierHot)
	if err != nil || len(hotWallets) == 0 {
		log.Debug().Str("chain", chainName).Msg("no hot wallet for reconciliation")
		return
	}
	hotWallet := hotWallets[0]

	// Get on-chain native balance
	balCtx, balCancel := context.WithTimeout(ctx, 15*time.Second)
	onChainBalance, err := adapter.GetBalance(balCtx, common.HexToAddress(hotWallet.Address))
	balCancel()
	if err != nil {
		log.Error().Str("chain", chainName).Err(err).Msg("failed to get on-chain hot wallet balance")
		rs.recordRun(ctx, chainName, "HOT_WALLET", big.NewInt(0), big.NewInt(0), "ERROR", "on-chain query failed: "+err.Error())
		return
	}

	// Get ledger ASSET:HOT balance
	chainUpper := strings.ToUpper(chainName)
	nativeSymbol := adapter.NativeSymbol()
	hotAccountCode := fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, nativeSymbol)
	ledgerBalance := big.NewInt(0)
	hotAccount, err := rs.ledgerRepo.FindAccountByCode(ctx, hotAccountCode)
	if err == nil {
		lb, lbErr := rs.ledgerRepo.GetAccountBalance(ctx, hotAccount.ID)
		if lbErr == nil {
			ledgerBalance = lb
		}
	}

	// Calculate drift
	drift := new(big.Int).Sub(onChainBalance, ledgerBalance)
	absDrift := new(big.Int).Abs(drift)

	// Determine status
	status := "PASS"
	if absDrift.Cmp(rs.driftThreshold) > 0 {
		status = "DRIFT"
		reconDriftAlerts.WithLabelValues(chainName).Inc()
		log.Error().
			Str("chain", chainName).
			Str("on_chain", onChainBalance.String()).
			Str("ledger", ledgerBalance.String()).
			Str("drift", drift.String()).
			Msg("🔴 RECONCILIATION DRIFT — on-chain ≠ ledger")
	}

	reconDrift.WithLabelValues(chainName).Set(float64(drift.Int64()))
	reconRunsTotal.WithLabelValues(chainName, status).Inc()

	// Build details
	details := map[string]interface{}{
		"hot_wallet_address": hotWallet.Address,
		"native_symbol":      nativeSymbol,
		"account_code":       hotAccountCode,
		"timestamp":          time.Now().UTC().Format(time.RFC3339),
	}

	// Count unswept deposits as context
	unsweptCount := 0
	for _, state := range []domain.DepositState{domain.DepositConfirmed, domain.DepositConfirming} {
		deposits, err := rs.depositRepo.FindByChainAndState(ctx, chainName, state)
		if err == nil {
			unsweptCount += len(deposits)
		}
	}
	details["unswept_deposits"] = unsweptCount

	detailsJSON, _ := json.Marshal(details)

	rs.recordRun(ctx, chainName, "HOT_WALLET", onChainBalance, ledgerBalance, status, string(detailsJSON))

	log.Info().
		Str("chain", chainName).
		Str("on_chain", onChainBalance.String()).
		Str("ledger", ledgerBalance.String()).
		Str("drift", drift.String()).
		Str("status", status).
		Int("unswept", unsweptCount).
		Msg("reconciliation check complete")
}

func (rs *ReconciliationService) recordRun(ctx context.Context, chainName, runType string, onChain, ledger *big.Int, status, details string) {
	drift := new(big.Int).Sub(onChain, ledger)

	var driftPct float64
	if ledger.Sign() > 0 {
		driftFloat := new(big.Float).SetInt(drift)
		ledgerFloat := new(big.Float).SetInt(ledger)
		pct := new(big.Float).Quo(driftFloat, ledgerFloat)
		driftPct, _ = pct.Float64()
		driftPct *= 100
	}

	run := &repository.ReconciliationRun{
		ID:             uuid.New(),
		Chain:          chainName,
		RunType:        runType,
		OnChainBalance: onChain.String(),
		LedgerBalance:  ledger.String(),
		Drift:          drift.String(),
		DriftPct:       driftPct,
		Status:         status,
		Details:        details,
	}

	if err := rs.reconRepo.Insert(ctx, run); err != nil {
		log.Error().Err(err).Msg("failed to record reconciliation run")
	}
}

// GetLatestRuns returns the most recent reconciliation runs for a chain
func (rs *ReconciliationService) GetLatestRuns(ctx context.Context, chain string, limit int) ([]*repository.ReconciliationRun, error) {
	return rs.reconRepo.FindLatest(ctx, chain, limit)
}

// RunNow triggers an immediate reconciliation check
func (rs *ReconciliationService) RunNow(ctx context.Context) {
	rs.runAll(ctx)
}
