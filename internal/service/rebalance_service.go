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
)

// Metric result labels (bounded set to prevent cardinality explosion)
const (
	metricResultCreated          = "created"
	metricResultApproved         = "approved"
	metricResultRejected         = "rejected"
	metricResultDeduplicated     = "deduplicated"
	metricResultBelowMinimum     = "below_minimum"
	metricResultInsufficientBal  = "insufficient_balance"
)

var (
	rebalanceOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_rebalance_operations_total",
		Help: "Total rebalance operations by chain and tier",
	}, []string{"chain", "from_tier", "to_tier", "result"})

	tierBalancePct = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_tier_balance_pct",
		Help: "Current balance percentage per tier",
	}, []string{"chain", "token", "tier"})

	rebalanceEvalErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_rebalance_eval_errors_total",
		Help: "Rebalance evaluation errors by type",
	}, []string{"chain", "error_type"})
)

// Default operation expiry: ops not acted on within this window are eligible for cleanup
const defaultOpExpiryDuration = 24 * time.Hour

// RebalanceService manages warm/cold wallet rebalancing
type RebalanceService struct {
	rebalanceRepo *repository.RebalanceRepo
	walletRepo    *repository.WalletRepo
	ledgerSvc     *LedgerService
}

// NewRebalanceService creates a new rebalance service
func NewRebalanceService(
	rebalanceRepo *repository.RebalanceRepo,
	walletRepo *repository.WalletRepo,
	ledgerSvc *LedgerService,
) *RebalanceService {
	return &RebalanceService{
		rebalanceRepo: rebalanceRepo,
		walletRepo:    walletRepo,
		ledgerSvc:     ledgerSvc,
	}
}

// EvaluateRebalance checks if rebalancing is needed for a chain/token and
// creates a pending operation if thresholds are breached.
// Deduplicates against existing PENDING/APPROVED ops for the same chain/token/direction.
func (s *RebalanceService) EvaluateRebalance(ctx context.Context, chain string, token string) (*domain.RebalanceOperation, error) {
	policy, err := s.rebalanceRepo.GetPolicy(ctx, chain, token)
	if err != nil {
		return nil, fmt.Errorf("get rebalance policy for %s/%s: %w", chain, token, err)
	}
	if !policy.IsActive {
		return nil, fmt.Errorf("rebalance policy for %s/%s is inactive", chain, token)
	}

	tb, err := s.computeTierBalance(ctx, chain, token)
	if err != nil {
		rebalanceEvalErrors.WithLabelValues(chain, "balance_compute").Inc()
		return nil, fmt.Errorf("compute tier balances: %w", err)
	}

	// Update Prometheus gauges
	tierBalancePct.WithLabelValues(chain, token, "HOT").Set(tb.HotPct)
	tierBalancePct.WithLabelValues(chain, token, "WARM").Set(tb.WarmPct)
	tierBalancePct.WithLabelValues(chain, token, "COLD").Set(tb.ColdPct)

	if tb.TotalBalance.Sign() == 0 {
		return nil, fmt.Errorf("zero total balance for %s/%s, nothing to rebalance", chain, token)
	}

	// Check if hot exceeds max → sweep HOT→WARM
	if tb.HotPct > policy.HotMaxPct {
		targetAmount := s.computeRebalanceAmount(tb.TotalBalance, tb.HotBalance, policy.HotTargetPct)
		if targetAmount.Sign() > 0 {
			return s.tryCreateRebalanceOp(ctx, chain, token, domain.WalletTierHot, domain.WalletTierWarm,
				targetAmount, tb.HotBalance, policy)
		}
	}

	// Check if hot below min → refill WARM→HOT
	if tb.HotPct < policy.HotMinPct {
		targetAmount := s.computeRebalanceAmount(tb.TotalBalance, tb.HotBalance, policy.HotTargetPct)
		targetAmount = new(big.Int).Abs(targetAmount)
		// Cap at available warm balance
		if targetAmount.Cmp(tb.WarmBalance) > 0 {
			targetAmount = new(big.Int).Set(tb.WarmBalance)
		}
		if targetAmount.Sign() > 0 {
			return s.tryCreateRebalanceOp(ctx, chain, token, domain.WalletTierWarm, domain.WalletTierHot,
				targetAmount, tb.WarmBalance, policy)
		}
	}

	// Check if warm exceeds max → sweep WARM→COLD
	if tb.WarmPct > policy.WarmMaxPct {
		targetAmount := s.computeRebalanceAmount(tb.TotalBalance, tb.WarmBalance, policy.WarmTargetPct)
		if targetAmount.Sign() > 0 {
			return s.tryCreateRebalanceOp(ctx, chain, token, domain.WalletTierWarm, domain.WalletTierCold,
				targetAmount, tb.WarmBalance, policy)
		}
	}

	log.Info().
		Str("chain", chain).
		Str("token", token).
		Float64("hot_pct", tb.HotPct).
		Float64("warm_pct", tb.WarmPct).
		Float64("cold_pct", tb.ColdPct).
		Msg("rebalance evaluation: within thresholds")

	return nil, nil
}

// ApproveRebalance approves a pending rebalance operation after re-validating
// that the source wallet still has sufficient balance.
func (s *RebalanceService) ApproveRebalance(ctx context.Context, id uuid.UUID, approvedBy string) error {
	op, err := s.rebalanceRepo.GetOperation(ctx, id)
	if err != nil {
		return fmt.Errorf("get operation: %w", err)
	}

	if op.State != domain.RebalancePending {
		return fmt.Errorf("operation %s is in state %s, expected PENDING", id, op.State)
	}

	// Re-validate source balance before approval (balances may have changed)
	chainUpper := strings.ToUpper(op.Chain)
	fromCode := fmt.Sprintf("ASSET:%s:%s:%s", op.FromTier, chainUpper, op.TokenSymbol)
	fromBal, err := s.ledgerSvc.GetAccountBalance(ctx, fromCode)
	if err != nil {
		return fmt.Errorf("re-validate source balance for %s: %w", fromCode, err)
	}
	if fromBal == nil || fromBal.Cmp(op.Amount) < 0 {
		rebalanceOpsTotal.WithLabelValues(op.Chain, string(op.FromTier), string(op.ToTier), metricResultInsufficientBal).Inc()
		return fmt.Errorf("insufficient source balance in %s: have %s, need %s",
			fromCode, safeIntString(fromBal), op.Amount.String())
	}

	if err := s.rebalanceRepo.UpdateOperationState(ctx, id, domain.RebalanceApproved, nil); err != nil {
		return fmt.Errorf("approve operation: %w", err)
	}

	// Record who approved
	if err := s.rebalanceRepo.SetApprovedBy(ctx, id, approvedBy); err != nil {
		log.Warn().Err(err).Str("id", id.String()).Msg("failed to record approver")
	}

	rebalanceOpsTotal.WithLabelValues(op.Chain, string(op.FromTier), string(op.ToTier), metricResultApproved).Inc()

	log.Info().
		Str("id", id.String()).
		Str("approved_by", approvedBy).
		Str("from_tier", string(op.FromTier)).
		Str("to_tier", string(op.ToTier)).
		Str("amount", op.Amount.String()).
		Msg("rebalance operation approved")

	return nil
}

// GetTierBalances returns balance breakdown per tier for all chain/token pairs
func (s *RebalanceService) GetTierBalances(ctx context.Context) ([]domain.TierBalance, error) {
	policies, err := s.rebalanceRepo.ListPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("list policies: %w", err)
	}

	var balances []domain.TierBalance
	for _, p := range policies {
		if err := ctx.Err(); err != nil {
			return balances, fmt.Errorf("context cancelled: %w", err)
		}
		tb, err := s.computeTierBalance(ctx, p.Chain, p.TokenSymbol)
		if err != nil {
			rebalanceEvalErrors.WithLabelValues(p.Chain, "tier_balance").Inc()
			log.Warn().Err(err).Str("chain", p.Chain).Str("token", p.TokenSymbol).Msg("skip tier balance computation")
			continue
		}
		balances = append(balances, *tb)

		tierBalancePct.WithLabelValues(p.Chain, p.TokenSymbol, "HOT").Set(tb.HotPct)
		tierBalancePct.WithLabelValues(p.Chain, p.TokenSymbol, "WARM").Set(tb.WarmPct)
		tierBalancePct.WithLabelValues(p.Chain, p.TokenSymbol, "COLD").Set(tb.ColdPct)
	}

	return balances, nil
}

// ListPolicies returns all rebalance policies
func (s *RebalanceService) ListPolicies(ctx context.Context) ([]*domain.RebalancePolicy, error) {
	return s.rebalanceRepo.ListPolicies(ctx)
}

// UpdatePolicy updates a rebalance policy
func (s *RebalanceService) UpdatePolicy(ctx context.Context, p *domain.RebalancePolicy) error {
	return s.rebalanceRepo.UpdatePolicy(ctx, p)
}

// ListOperations returns rebalance operations with optional filters
func (s *RebalanceService) ListOperations(ctx context.Context, chain, state string, limit int) ([]*domain.RebalanceOperation, error) {
	return s.rebalanceRepo.ListOperations(ctx, chain, state, limit)
}

// ExpireStaleOperations transitions PENDING ops older than the expiry window to REJECTED.
func (s *RebalanceService) ExpireStaleOperations(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-defaultOpExpiryDuration)
	return s.rebalanceRepo.ExpireOperations(ctx, cutoff)
}

// computeTierBalance calculates the balance distribution across HOT/WARM/COLD
// for a given chain/token by querying the ledger.
// Returns an error (not zero) if the ledger is unreachable.
func (s *RebalanceService) computeTierBalance(ctx context.Context, chain, token string) (*domain.TierBalance, error) {
	chainUpper := strings.ToUpper(chain)

	hotBalance, hotErr := s.getAccountBalance(ctx, fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, token))
	warmBalance, warmErr := s.getAccountBalance(ctx, fmt.Sprintf("ASSET:WARM:%s:%s", chainUpper, token))
	coldBalance, coldErr := s.getAccountBalance(ctx, fmt.Sprintf("ASSET:COLD:%s:%s", chainUpper, token))

	// If ALL three fail, the ledger is likely down — do not return misleading zeros
	if hotErr != nil && warmErr != nil && coldErr != nil {
		return nil, fmt.Errorf("ledger unavailable for %s/%s: hot=%v, warm=%v, cold=%v",
			chain, token, hotErr, warmErr, coldErr)
	}

	// Individual missing accounts are expected (account may not exist yet)
	hot := zeroIfNil(hotBalance)
	warm := zeroIfNil(warmBalance)
	cold := zeroIfNil(coldBalance)

	total := new(big.Int).Add(hot, warm)
	total.Add(total, cold)

	tb := &domain.TierBalance{
		Chain:        chain,
		TokenSymbol:  token,
		HotBalance:   hot,
		WarmBalance:  warm,
		ColdBalance:  cold,
		TotalBalance: total,
	}

	if total.Sign() > 0 {
		totalFloat := new(big.Float).SetInt(total)
		hundred := new(big.Float).SetFloat64(100.0)

		pct := new(big.Float)
		pct.Quo(new(big.Float).SetInt(hot), totalFloat)
		pct.Mul(pct, hundred)
		tb.HotPct, _ = pct.Float64()

		pct.Quo(new(big.Float).SetInt(warm), totalFloat)
		pct.Mul(pct, hundred)
		tb.WarmPct, _ = pct.Float64()

		pct.Quo(new(big.Float).SetInt(cold), totalFloat)
		pct.Mul(pct, hundred)
		tb.ColdPct, _ = pct.Float64()
	}

	return tb, nil
}

// getAccountBalance queries the ledger and returns (balance, error).
// Unlike the old getAccountBalanceSafe, errors are propagated for upstream decision.
func (s *RebalanceService) getAccountBalance(ctx context.Context, code string) (*big.Int, error) {
	bal, err := s.ledgerSvc.GetAccountBalance(ctx, code)
	if err != nil {
		return nil, err
	}
	if bal == nil {
		return big.NewInt(0), nil
	}
	if bal.Sign() < 0 {
		return big.NewInt(0), nil
	}
	return bal, nil
}

// computeRebalanceAmount calculates the excess amount above the target percentage.
// Returns: currentBalance - (totalBalance * targetPct / 100)
func (s *RebalanceService) computeRebalanceAmount(total, current *big.Int, targetPct float64) *big.Int {
	targetBig := new(big.Float).SetInt(total)
	targetBig.Mul(targetBig, new(big.Float).SetFloat64(targetPct))
	targetBig.Quo(targetBig, new(big.Float).SetFloat64(100.0))

	targetInt, _ := targetBig.Int(nil)
	return new(big.Int).Sub(current, targetInt)
}

// tryCreateRebalanceOp validates preconditions and creates a rebalance operation:
// 1. Check minimum amount threshold
// 2. Dedup against existing PENDING/APPROVED ops for same chain/token/direction
// 3. Verify source wallet has sufficient balance
// 4. Select source wallet not already involved in a pending op
func (s *RebalanceService) tryCreateRebalanceOp(
	ctx context.Context,
	chain, token string,
	fromTier, toTier domain.WalletTier,
	amount *big.Int,
	sourceBalance *big.Int,
	policy *domain.RebalancePolicy,
) (*domain.RebalanceOperation, error) {
	// 1. Minimum amount check
	if policy.MinRebalanceAmount.Sign() > 0 && amount.Cmp(policy.MinRebalanceAmount) < 0 {
		rebalanceOpsTotal.WithLabelValues(chain, string(fromTier), string(toTier), metricResultBelowMinimum).Inc()
		log.Debug().
			Str("chain", chain).Str("token", token).
			Str("amount", amount.String()).
			Str("min", policy.MinRebalanceAmount.String()).
			Msg("rebalance amount below minimum threshold")
		return nil, nil
	}

	// 2. Dedup: check for existing active ops in the same direction
	existingOps, err := s.rebalanceRepo.FindActiveOps(ctx, chain, token, fromTier, toTier)
	if err != nil {
		return nil, fmt.Errorf("check existing ops: %w", err)
	}
	if len(existingOps) > 0 {
		rebalanceOpsTotal.WithLabelValues(chain, string(fromTier), string(toTier), metricResultDeduplicated).Inc()
		log.Info().
			Str("chain", chain).Str("token", token).
			Str("existing_op", existingOps[0].ID.String()).
			Str("existing_state", string(existingOps[0].State)).
			Msg("rebalance op already exists, skipping")
		return existingOps[0], nil
	}

	// 3. Balance sufficiency: verify source tier has enough
	if sourceBalance.Cmp(amount) < 0 {
		rebalanceOpsTotal.WithLabelValues(chain, string(fromTier), string(toTier), metricResultInsufficientBal).Inc()
		log.Warn().
			Str("chain", chain).Str("token", token).
			Str("from_tier", string(fromTier)).
			Str("source_bal", sourceBalance.String()).
			Str("needed", amount.String()).
			Msg("insufficient source balance for rebalance")
		// Cap at available source balance
		amount = new(big.Int).Set(sourceBalance)
		if amount.Sign() == 0 {
			return nil, nil
		}
		// Re-check minimum after capping
		if policy.MinRebalanceAmount.Sign() > 0 && amount.Cmp(policy.MinRebalanceAmount) < 0 {
			return nil, nil
		}
	}

	// 4. Select wallets (skip wallets already in pending ops)
	fromWallets, err := s.walletRepo.FindByChainAndTier(ctx, chain, fromTier)
	if err != nil || len(fromWallets) == 0 {
		return nil, fmt.Errorf("no active %s wallet for chain %s", fromTier, chain)
	}
	toWallets, err := s.walletRepo.FindByChainAndTier(ctx, chain, toTier)
	if err != nil || len(toWallets) == 0 {
		return nil, fmt.Errorf("no active %s wallet for chain %s", toTier, chain)
	}

	// Pick first wallet not already a source in a pending/approved op
	pendingWallets, _ := s.rebalanceRepo.PendingSourceWalletIDs(ctx, chain)
	fromWallet := selectAvailableWallet(fromWallets, pendingWallets)
	if fromWallet == nil {
		return nil, fmt.Errorf("all %s wallets for %s are busy with pending operations", fromTier, chain)
	}

	// Determine state and approval attribution
	state := domain.RebalancePending
	var approvedBy *string
	if policy.AutoRebalance {
		state = domain.RebalanceApproved
		systemPrincipal := "SYSTEM:auto-rebalance"
		approvedBy = &systemPrincipal
	}

	op := &domain.RebalanceOperation{
		ID:               uuid.New(),
		Chain:            chain,
		TokenSymbol:      token,
		FromTier:         fromTier,
		ToTier:           toTier,
		FromWalletID:     fromWallet.ID,
		ToWalletID:       toWallets[0].ID,
		Amount:           amount,
		State:            state,
		RequiresApproval: !policy.AutoRebalance,
		ApprovedBy:       approvedBy,
	}

	if err := s.rebalanceRepo.CreateOperation(ctx, op); err != nil {
		return nil, fmt.Errorf("create rebalance operation: %w", err)
	}

	rebalanceOpsTotal.WithLabelValues(chain, string(fromTier), string(toTier), metricResultCreated).Inc()

	log.Info().
		Str("id", op.ID.String()).
		Str("chain", chain).
		Str("token", token).
		Str("from_tier", string(fromTier)).
		Str("to_tier", string(toTier)).
		Str("from_wallet", fromWallet.ID.String()).
		Str("amount", amount.String()).
		Bool("auto_approved", policy.AutoRebalance).
		Msg("rebalance operation created")

	return op, nil
}

// selectAvailableWallet picks the first wallet not in the busy set
func selectAvailableWallet(wallets []*domain.Wallet, busyIDs map[uuid.UUID]bool) *domain.Wallet {
	for _, w := range wallets {
		if !busyIDs[w.ID] {
			return w
		}
	}
	return nil
}

func zeroIfNil(v *big.Int) *big.Int {
	if v == nil || v.Sign() < 0 {
		return big.NewInt(0)
	}
	return v
}

func safeIntString(v *big.Int) string {
	if v == nil {
		return "nil"
	}
	return v.String()
}
