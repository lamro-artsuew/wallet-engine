package service

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/messaging"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	withdrawalsCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_withdrawals_created_total",
		Help: "Total withdrawals created per chain",
	}, []string{"chain", "token"})

	withdrawalStateTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_withdrawal_state_transitions_total",
		Help: "Withdrawal state transitions",
	}, []string{"chain", "from_state", "to_state"})
)

// Valid state transitions for withdrawal state machine
var validWithdrawalTransitions = map[domain.WithdrawalState][]domain.WithdrawalState{
	domain.WithdrawalInitiated:      {domain.WithdrawalRiskCheck, domain.WithdrawalFailed},
	domain.WithdrawalRiskCheck:      {domain.WithdrawalRiskApproved, domain.WithdrawalRiskRejected, domain.WithdrawalManualReview},
	domain.WithdrawalRiskApproved:   {domain.WithdrawalSigning, domain.WithdrawalFailed},
	domain.WithdrawalRiskRejected:   {}, // terminal
	domain.WithdrawalSigning:        {domain.WithdrawalSigned, domain.WithdrawalFailed},
	domain.WithdrawalSigned:         {domain.WithdrawalBroadcast, domain.WithdrawalFailed},
	domain.WithdrawalBroadcast:      {domain.WithdrawalPendingConfirm, domain.WithdrawalFailed, domain.WithdrawalExpired},
	domain.WithdrawalPendingConfirm: {domain.WithdrawalConfirmed, domain.WithdrawalFailed, domain.WithdrawalExpired},
	domain.WithdrawalConfirmed:      {}, // terminal
	domain.WithdrawalFailed:         {domain.WithdrawalInitiated}, // retry
	domain.WithdrawalExpired:        {domain.WithdrawalInitiated}, // retry
	domain.WithdrawalManualReview:   {domain.WithdrawalRiskApproved, domain.WithdrawalRiskRejected},
}

// WithdrawalService manages withdrawal lifecycle
type WithdrawalService struct {
	withdrawalRepo *repository.WithdrawalRepo
	walletRepo     *repository.WalletRepo
	ledgerSvc      *LedgerService
	producer       *messaging.RedpandaProducer
	blacklistSvc   *BlacklistService
	velocitySvc    *VelocityService
}

// NewWithdrawalService creates a new withdrawal service
func NewWithdrawalService(
	withdrawalRepo *repository.WithdrawalRepo,
	walletRepo *repository.WalletRepo,
	ledgerSvc *LedgerService,
	producer *messaging.RedpandaProducer,
	blacklistSvc *BlacklistService,
	velocitySvc *VelocityService,
) *WithdrawalService {
	return &WithdrawalService{
		withdrawalRepo: withdrawalRepo,
		walletRepo:     walletRepo,
		ledgerSvc:      ledgerSvc,
		producer:       producer,
		blacklistSvc:   blacklistSvc,
		velocitySvc:    velocitySvc,
	}
}

// SetBlacklistService sets the blacklist service (used for late initialization)
func (s *WithdrawalService) SetBlacklistService(svc *BlacklistService) {
	s.blacklistSvc = svc
}

// CreateWithdrawalRequest holds the data for creating a withdrawal
type CreateWithdrawalRequest struct {
	IdempotencyKey string
	WorkspaceID    uuid.UUID
	UserID         uuid.UUID
	Chain          string
	ToAddress      string
	TokenAddress   string
	TokenSymbol    string
	Amount         *big.Int
	Decimals       int
	SourceIP       string
	CountryCode    string
}

// CreateWithdrawal initiates a new withdrawal request
func (s *WithdrawalService) CreateWithdrawal(ctx context.Context, req CreateWithdrawalRequest) (*domain.Withdrawal, error) {
	// Idempotency check
	if existing, err := s.withdrawalRepo.FindByIdempotencyKey(ctx, req.IdempotencyKey); err == nil {
		log.Info().Str("id", existing.ID.String()).Msg("duplicate withdrawal request, returning existing")
		return existing, nil
	}

	// Validate destination address
	if !ValidateAddress(req.Chain, req.ToAddress) {
		return nil, fmt.Errorf("invalid destination address %s for chain %s", req.ToAddress, req.Chain)
	}

	// Check destination against blacklist
	if s.blacklistSvc != nil {
		result, err := s.blacklistSvc.CheckAddress(ctx, req.Chain, req.ToAddress)
		if err != nil {
			return nil, fmt.Errorf("blacklist check failed: %w", err)
		}
		if result.IsBlacklisted {
			return nil, fmt.Errorf("destination address %s is blacklisted (sources: %s)",
				req.ToAddress, strings.Join(result.Sources, ", "))
		}
	}

	// Velocity check — enforce rate limits and geo restrictions before creating the withdrawal
	if s.velocitySvc != nil {
		preCheck := &domain.Withdrawal{
			ID:          uuid.New(),
			WorkspaceID: req.WorkspaceID,
			UserID:      req.UserID,
			Chain:       req.Chain,
			TokenSymbol: req.TokenSymbol,
			Amount:      req.Amount,
		}
		velocityResult, err := s.velocitySvc.CheckWithdrawal(ctx, preCheck, req.SourceIP, req.CountryCode)
		if err != nil {
			return nil, fmt.Errorf("velocity check failed: %w", err)
		}
		if !velocityResult.Allowed {
			return nil, fmt.Errorf("velocity check rejected: %s", velocityResult.RejectionReason)
		}
	}

	// Find hot wallet for the chain
	hotWallets, err := s.walletRepo.FindByChainAndTier(ctx, req.Chain, domain.WalletTierHot)
	if err != nil || len(hotWallets) == 0 {
		return nil, fmt.Errorf("no active hot wallet for chain %s", req.Chain)
	}
	sourceWallet := hotWallets[0]

	// Determine required confirmations based on chain
	requiredConfs := chainConfirmations(req.Chain)

	w := &domain.Withdrawal{
		ID:             uuid.New(),
		IdempotencyKey: req.IdempotencyKey,
		WorkspaceID:    req.WorkspaceID,
		UserID:         req.UserID,
		Chain:          req.Chain,
		ToAddress:      req.ToAddress,
		TokenAddress:   req.TokenAddress,
		TokenSymbol:    req.TokenSymbol,
		Amount:         req.Amount,
		Decimals:       req.Decimals,
		State:          domain.WithdrawalInitiated,
		SourceWalletID: sourceWallet.ID,
		RequiredConfs:  requiredConfs,
	}

	if err := s.withdrawalRepo.Create(ctx, w); err != nil {
		return nil, fmt.Errorf("create withdrawal: %w", err)
	}

	withdrawalsCreated.WithLabelValues(req.Chain, req.TokenSymbol).Inc()

	log.Info().
		Str("id", w.ID.String()).
		Str("chain", w.Chain).
		Str("to", w.ToAddress).
		Str("token", w.TokenSymbol).
		Str("amount", w.Amount.String()).
		Msg("withdrawal created")

	// Publish event
	if s.producer != nil {
		s.producer.PublishWithdrawal(ctx, w)
	}

	return w, nil
}

// TransitionState moves a withdrawal to a new state with validation
func (s *WithdrawalService) TransitionState(ctx context.Context, id uuid.UUID, newState domain.WithdrawalState, errorMsg *string) error {
	w, err := s.withdrawalRepo.FindByID(ctx, id)
	if err != nil {
		return fmt.Errorf("find withdrawal: %w", err)
	}

	if !isValidTransition(w.State, newState) {
		return fmt.Errorf("invalid state transition: %s → %s", w.State, newState)
	}

	if err := s.withdrawalRepo.UpdateState(ctx, id, w.State, newState, errorMsg); err != nil {
		return fmt.Errorf("update state: %w", err)
	}

	withdrawalStateTransitions.WithLabelValues(w.Chain, string(w.State), string(newState)).Inc()

	log.Info().
		Str("id", id.String()).
		Str("from", string(w.State)).
		Str("to", string(newState)).
		Msg("withdrawal state transition")

	// Post ledger entry on confirmation
	if newState == domain.WithdrawalConfirmed && s.ledgerSvc != nil {
		// Refresh to get updated fields
		w, _ = s.withdrawalRepo.FindByID(ctx, id)
		if err := s.ledgerSvc.PostWithdrawalEntry(ctx, w); err != nil {
			log.Error().Err(err).Str("id", id.String()).Msg("failed to post withdrawal ledger entry")
		}
	}

	// Publish state change
	if s.producer != nil {
		w.State = newState
		s.producer.PublishWithdrawal(ctx, w)
	}

	return nil
}

// GetWithdrawal returns a withdrawal by ID
func (s *WithdrawalService) GetWithdrawal(ctx context.Context, id uuid.UUID) (*domain.Withdrawal, error) {
	return s.withdrawalRepo.FindByID(ctx, id)
}

// ListByUser returns withdrawals for a user
func (s *WithdrawalService) ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*domain.Withdrawal, error) {
	return s.withdrawalRepo.FindByUser(ctx, userID, limit, offset)
}

// ListByWorkspace returns withdrawals for a workspace
func (s *WithdrawalService) ListByWorkspace(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*domain.Withdrawal, error) {
	return s.withdrawalRepo.FindByWorkspace(ctx, workspaceID, limit, offset)
}

// ListAll returns all withdrawals with pagination (admin view)
func (s *WithdrawalService) ListAll(ctx context.Context, limit, offset int) ([]*domain.Withdrawal, error) {
	return s.withdrawalRepo.FindAll(ctx, limit, offset)
}

// ListByChainAndState returns withdrawals for a chain in a state (bounded)
func (s *WithdrawalService) ListByChainAndState(ctx context.Context, chain string, state domain.WithdrawalState) ([]*domain.Withdrawal, error) {
	return s.withdrawalRepo.FindByChainAndState(ctx, chain, state, 100)
}

func isValidTransition(from, to domain.WithdrawalState) bool {
	validTargets, ok := validWithdrawalTransitions[from]
	if !ok {
		return false
	}
	for _, valid := range validTargets {
		if valid == to {
			return true
		}
	}
	return false
}

func chainConfirmations(chain string) int {
	switch strings.ToLower(chain) {
	case "ethereum":
		return 12
	case "bsc":
		return 15
	case "polygon":
		return 64
	case "arbitrum":
		return 10
	case "optimism":
		return 10
	case "avalanche":
		return 12
	case "tron":
		return 19
	default:
		return 12
	}
}
