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
	rebalanceOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_rebalance_operations_total",
		Help: "Total rebalance operations by chain and tier",
	}, []string{"chain", "from_tier", "to_tier", "result"})

	tierBalancePct = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_tier_balance_pct",
		Help: "Current balance percentage per tier",
	}, []string{"chain", "token", "tier"})
)

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
		if targetAmount.Sign() > 0 && (policy.MinRebalanceAmount.Sign() == 0 || targetAmount.Cmp(policy.MinRebalanceAmount) >= 0) {
			return s.createRebalanceOp(ctx, chain, token, domain.WalletTierHot, domain.WalletTierWarm, targetAmount, policy)
		}
	}

	// Check if hot below min → refill WARM→HOT
	if tb.HotPct < policy.HotMinPct {
		targetAmount := s.computeRebalanceAmount(tb.TotalBalance, tb.HotBalance, policy.HotTargetPct)
		// targetAmount is negative when hot is below target; we need the absolute value
		targetAmount = new(big.Int).Abs(targetAmount)
		// Cap at available warm balance
		if targetAmount.Cmp(tb.WarmBalance) > 0 {
			targetAmount = new(big.Int).Set(tb.WarmBalance)
		}
		if targetAmount.Sign() > 0 && (policy.MinRebalanceAmount.Sign() == 0 || targetAmount.Cmp(policy.MinRebalanceAmount) >= 0) {
			return s.createRebalanceOp(ctx, chain, token, domain.WalletTierWarm, domain.WalletTierHot, targetAmount, policy)
		}
	}

	// Check if warm exceeds max → sweep WARM→COLD
	if tb.WarmPct > policy.WarmMaxPct {
		targetAmount := s.computeRebalanceAmount(tb.TotalBalance, tb.WarmBalance, policy.WarmTargetPct)
		if targetAmount.Sign() > 0 && (policy.MinRebalanceAmount.Sign() == 0 || targetAmount.Cmp(policy.MinRebalanceAmount) >= 0) {
			return s.createRebalanceOp(ctx, chain, token, domain.WalletTierWarm, domain.WalletTierCold, targetAmount, policy)
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

// ApproveRebalance approves a pending rebalance operation
func (s *RebalanceService) ApproveRebalance(ctx context.Context, id uuid.UUID, approvedBy string) error {
	op, err := s.rebalanceRepo.GetOperation(ctx, id)
	if err != nil {
		return fmt.Errorf("get operation: %w", err)
	}

	if op.State != domain.RebalancePending {
		return fmt.Errorf("operation %s is in state %s, expected PENDING", id, op.State)
	}

	if err := s.rebalanceRepo.UpdateOperationState(ctx, id, domain.RebalanceApproved, nil); err != nil {
		return fmt.Errorf("approve operation: %w", err)
	}

	rebalanceOpsTotal.WithLabelValues(op.Chain, string(op.FromTier), string(op.ToTier), "approved").Inc()

	log.Info().
		Str("id", id.String()).
		Str("approved_by", approvedBy).
		Str("from_tier", string(op.FromTier)).
		Str("to_tier", string(op.ToTier)).
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
		tb, err := s.computeTierBalance(ctx, p.Chain, p.TokenSymbol)
		if err != nil {
			log.Warn().Err(err).Str("chain", p.Chain).Str("token", p.TokenSymbol).Msg("skip tier balance computation")
			continue
		}
		balances = append(balances, *tb)

		// Update Prometheus gauges
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

// computeTierBalance calculates the balance distribution across HOT/WARM/COLD
// for a given chain/token by querying the ledger.
func (s *RebalanceService) computeTierBalance(ctx context.Context, chain, token string) (*domain.TierBalance, error) {
	chainUpper := strings.ToUpper(chain)

	hotBalance := s.getAccountBalanceSafe(ctx, fmt.Sprintf("ASSET:HOT:%s:%s", chainUpper, token))
	warmBalance := s.getAccountBalanceSafe(ctx, fmt.Sprintf("ASSET:WARM:%s:%s", chainUpper, token))
	coldBalance := s.getAccountBalanceSafe(ctx, fmt.Sprintf("ASSET:COLD:%s:%s", chainUpper, token))

	total := new(big.Int).Add(hotBalance, warmBalance)
	total.Add(total, coldBalance)

	tb := &domain.TierBalance{
		Chain:        chain,
		TokenSymbol:  token,
		HotBalance:   hotBalance,
		WarmBalance:  warmBalance,
		ColdBalance:  coldBalance,
		TotalBalance: total,
	}

	if total.Sign() > 0 {
		totalFloat := new(big.Float).SetInt(total)
		hotFloat := new(big.Float).SetInt(hotBalance)
		warmFloat := new(big.Float).SetInt(warmBalance)
		coldFloat := new(big.Float).SetInt(coldBalance)

		pct := new(big.Float)
		hundred := new(big.Float).SetFloat64(100.0)

		pct.Quo(hotFloat, totalFloat)
		pct.Mul(pct, hundred)
		tb.HotPct, _ = pct.Float64()

		pct.Quo(warmFloat, totalFloat)
		pct.Mul(pct, hundred)
		tb.WarmPct, _ = pct.Float64()

		pct.Quo(coldFloat, totalFloat)
		pct.Mul(pct, hundred)
		tb.ColdPct, _ = pct.Float64()
	}

	return tb, nil
}

func (s *RebalanceService) getAccountBalanceSafe(ctx context.Context, code string) *big.Int {
	bal, err := s.ledgerSvc.GetAccountBalance(ctx, code)
	if err != nil || bal == nil {
		return big.NewInt(0)
	}
	// Ensure non-negative
	if bal.Sign() < 0 {
		return big.NewInt(0)
	}
	return bal
}

// computeRebalanceAmount calculates the excess amount above the target percentage.
// Returns: currentBalance - (totalBalance * targetPct / 100)
func (s *RebalanceService) computeRebalanceAmount(total, current *big.Int, targetPct float64) *big.Int {
	// target = total * targetPct / 100
	targetBig := new(big.Float).SetInt(total)
	targetBig.Mul(targetBig, new(big.Float).SetFloat64(targetPct))
	targetBig.Quo(targetBig, new(big.Float).SetFloat64(100.0))

	targetInt, _ := targetBig.Int(nil)

	// excess = current - target
	return new(big.Int).Sub(current, targetInt)
}

func (s *RebalanceService) createRebalanceOp(
	ctx context.Context,
	chain, token string,
	fromTier, toTier domain.WalletTier,
	amount *big.Int,
	policy *domain.RebalancePolicy,
) (*domain.RebalanceOperation, error) {
	// Find source and destination wallets
	fromWallets, err := s.walletRepo.FindByChainAndTier(ctx, chain, fromTier)
	if err != nil || len(fromWallets) == 0 {
		return nil, fmt.Errorf("no active %s wallet for chain %s", fromTier, chain)
	}
	toWallets, err := s.walletRepo.FindByChainAndTier(ctx, chain, toTier)
	if err != nil || len(toWallets) == 0 {
		return nil, fmt.Errorf("no active %s wallet for chain %s", toTier, chain)
	}

	requiresApproval := !policy.AutoRebalance
	state := domain.RebalancePending
	if policy.AutoRebalance {
		state = domain.RebalanceApproved
	}

	op := &domain.RebalanceOperation{
		ID:               uuid.New(),
		Chain:            chain,
		TokenSymbol:      token,
		FromTier:         fromTier,
		ToTier:           toTier,
		FromWalletID:     fromWallets[0].ID,
		ToWalletID:       toWallets[0].ID,
		Amount:           amount,
		State:            state,
		RequiresApproval: requiresApproval,
	}

	if err := s.rebalanceRepo.CreateOperation(ctx, op); err != nil {
		return nil, fmt.Errorf("create rebalance operation: %w", err)
	}

	rebalanceOpsTotal.WithLabelValues(chain, string(fromTier), string(toTier), "created").Inc()

	log.Info().
		Str("id", op.ID.String()).
		Str("chain", chain).
		Str("token", token).
		Str("from_tier", string(fromTier)).
		Str("to_tier", string(toTier)).
		Str("amount", amount.String()).
		Bool("auto_approved", policy.AutoRebalance).
		Msg("rebalance operation created")

	return op, nil
}
