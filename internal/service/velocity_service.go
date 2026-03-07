package service

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	velocityChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_velocity_checks_total",
		Help: "Velocity check results",
	}, []string{"result", "rule"})
)

// VelocityService enforces per-transaction velocity limits and geo checks
type VelocityService struct {
	velocityRepo   *repository.VelocityRepo
	withdrawalRepo *repository.WithdrawalRepo
}

// NewVelocityService creates a new velocity service
func NewVelocityService(velocityRepo *repository.VelocityRepo, withdrawalRepo *repository.WithdrawalRepo) *VelocityService {
	return &VelocityService{
		velocityRepo:   velocityRepo,
		withdrawalRepo: withdrawalRepo,
	}
}

// CheckWithdrawal evaluates all applicable velocity limits for a withdrawal.
// Returns a VelocityCheckResult indicating whether the withdrawal is allowed.
func (s *VelocityService) CheckWithdrawal(ctx context.Context, w *domain.Withdrawal, sourceIP string, countryCode string) (*domain.VelocityCheckResult, error) {
	limits, err := s.velocityRepo.FindLimits(ctx, w.WorkspaceID, w.UserID, w.Chain, w.TokenSymbol)
	if err != nil {
		return nil, fmt.Errorf("fetch velocity limits: %w", err)
	}

	// If no limits configured, allow by default
	if len(limits) == 0 {
		s.logAttempt(ctx, w, sourceIP, countryCode, true, "")
		return &domain.VelocityCheckResult{
			Allowed:         true,
			ApplicableRules: []string{},
		}, nil
	}

	result := &domain.VelocityCheckResult{
		Allowed:         true,
		ApplicableRules: []string{},
	}

	now := time.Now().UTC()
	oneHourAgo := now.Add(-1 * time.Hour)
	oneDayAgo := now.Add(-24 * time.Hour)

	for _, limit := range limits {
		ruleName := fmt.Sprintf("%s:%s", limit.Scope, limit.ID.String()[:8])
		result.ApplicableRules = append(result.ApplicableRules, ruleName)

		// a. Per-tx amount limit
		if limit.MaxAmountPerTx != nil && w.Amount.Cmp(limit.MaxAmountPerTx) > 0 {
			reason := fmt.Sprintf("exceeds per-tx limit (%s > %s) [rule: %s]", w.Amount.String(), limit.MaxAmountPerTx.String(), ruleName)
			s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "per_tx_amount")
			result.Allowed = false
			result.RejectionReason = reason
			return result, nil
		}

		// b. Hourly amount limit
		if limit.MaxAmountPerHour != nil {
			hourlySum, err := s.velocityRepo.SumWithdrawalsInWindow(ctx, w.UserID, w.Chain, w.TokenSymbol, oneHourAgo)
			if err != nil {
				return nil, fmt.Errorf("query hourly sum: %w", err)
			}
			projected := new(big.Int).Add(hourlySum, w.Amount)
			if projected.Cmp(limit.MaxAmountPerHour) > 0 {
				reason := fmt.Sprintf("exceeds hourly amount limit (%s + %s > %s) [rule: %s]", hourlySum.String(), w.Amount.String(), limit.MaxAmountPerHour.String(), ruleName)
				s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "hourly_amount")
				result.Allowed = false
				result.RejectionReason = reason
				return result, nil
			}
		}

		// c. Daily amount limit
		if limit.MaxAmountPerDay != nil {
			dailySum, err := s.velocityRepo.SumWithdrawalsInWindow(ctx, w.UserID, w.Chain, w.TokenSymbol, oneDayAgo)
			if err != nil {
				return nil, fmt.Errorf("query daily sum: %w", err)
			}
			projected := new(big.Int).Add(dailySum, w.Amount)
			if projected.Cmp(limit.MaxAmountPerDay) > 0 {
				reason := fmt.Sprintf("exceeds daily amount limit (%s + %s > %s) [rule: %s]", dailySum.String(), w.Amount.String(), limit.MaxAmountPerDay.String(), ruleName)
				s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "daily_amount")
				result.Allowed = false
				result.RejectionReason = reason
				return result, nil
			}
		}

		// d. Hourly count limit
		if limit.MaxCountPerHour != nil {
			hourlyCount, err := s.velocityRepo.CountWithdrawalsInWindow(ctx, w.UserID, w.Chain, oneHourAgo)
			if err != nil {
				return nil, fmt.Errorf("query hourly count: %w", err)
			}
			if hourlyCount+1 > *limit.MaxCountPerHour {
				reason := fmt.Sprintf("exceeds hourly count limit (%d >= %d) [rule: %s]", hourlyCount, *limit.MaxCountPerHour, ruleName)
				s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "hourly_count")
				result.Allowed = false
				result.RejectionReason = reason
				return result, nil
			}
		}

		// e. Daily count limit
		if limit.MaxCountPerDay != nil {
			dailyCount, err := s.velocityRepo.CountWithdrawalsInWindow(ctx, w.UserID, w.Chain, oneDayAgo)
			if err != nil {
				return nil, fmt.Errorf("query daily count: %w", err)
			}
			if dailyCount+1 > *limit.MaxCountPerDay {
				reason := fmt.Sprintf("exceeds daily count limit (%d >= %d) [rule: %s]", dailyCount, *limit.MaxCountPerDay, ruleName)
				s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "daily_count")
				result.Allowed = false
				result.RejectionReason = reason
				return result, nil
			}
		}

		// f. Cooldown check
		if limit.CooldownSeconds > 0 {
			lastTime, err := s.velocityRepo.LastWithdrawalTime(ctx, w.UserID, w.Chain)
			if err != nil {
				return nil, fmt.Errorf("query last withdrawal time: %w", err)
			}
			if lastTime != nil {
				cooldownEnd := lastTime.Add(time.Duration(limit.CooldownSeconds) * time.Second)
				if now.Before(cooldownEnd) {
					remaining := cooldownEnd.Sub(now).Seconds()
					reason := fmt.Sprintf("cooldown active (%.0fs remaining) [rule: %s]", remaining, ruleName)
					s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "cooldown")
					result.Allowed = false
					result.RejectionReason = reason
					return result, nil
				}
			}
		}

		// g. Geo checks
		if countryCode != "" {
			// Check blocked countries
			if len(limit.GeoBlockedCountries) > 0 {
				for _, blocked := range limit.GeoBlockedCountries {
					if blocked == countryCode {
						reason := fmt.Sprintf("country %s is blocked [rule: %s]", countryCode, ruleName)
						s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "geo_blocked")
						result.Allowed = false
						result.RejectionReason = reason
						return result, nil
					}
				}
			}

			// Check allowed countries (if list is set, country must be in it)
			if len(limit.GeoAllowedCountries) > 0 {
				allowed := false
				for _, a := range limit.GeoAllowedCountries {
					if a == countryCode {
						allowed = true
						break
					}
				}
				if !allowed {
					reason := fmt.Sprintf("country %s not in allowed list [rule: %s]", countryCode, ruleName)
					s.rejectWithdrawal(ctx, w, sourceIP, countryCode, reason, "geo_not_allowed")
					result.Allowed = false
					result.RejectionReason = reason
					return result, nil
				}
			}
		}
	}

	// All checks passed
	s.logAttempt(ctx, w, sourceIP, countryCode, true, "")
	velocityChecksTotal.WithLabelValues("allowed", "all").Inc()

	log.Info().
		Str("user_id", w.UserID.String()).
		Str("chain", w.Chain).
		Str("token", w.TokenSymbol).
		Int("rules_checked", len(result.ApplicableRules)).
		Msg("velocity check passed")

	return result, nil
}

// ListLimits returns all velocity limits for admin listing
func (s *VelocityService) ListLimits(ctx context.Context) ([]*domain.VelocityLimit, error) {
	return s.velocityRepo.FindAllLimits(ctx)
}

// CreateLimit creates a new velocity limit
func (s *VelocityService) CreateLimit(ctx context.Context, vl *domain.VelocityLimit) error {
	if vl.ID == uuid.Nil {
		vl.ID = uuid.New()
	}
	return s.velocityRepo.CreateLimit(ctx, vl)
}

// UpdateLimit updates an existing velocity limit
func (s *VelocityService) UpdateLimit(ctx context.Context, vl *domain.VelocityLimit) error {
	return s.velocityRepo.UpdateLimit(ctx, vl)
}

// DeleteLimit soft-deletes a velocity limit
func (s *VelocityService) DeleteLimit(ctx context.Context, id uuid.UUID) error {
	return s.velocityRepo.DeleteLimit(ctx, id)
}

func (s *VelocityService) rejectWithdrawal(ctx context.Context, w *domain.Withdrawal, sourceIP, countryCode, reason, rule string) {
	velocityChecksTotal.WithLabelValues("rejected", rule).Inc()
	s.logAttempt(ctx, w, sourceIP, countryCode, false, reason)

	log.Warn().
		Str("user_id", w.UserID.String()).
		Str("chain", w.Chain).
		Str("token", w.TokenSymbol).
		Str("amount", w.Amount.String()).
		Str("rule", rule).
		Str("reason", reason).
		Msg("velocity check rejected withdrawal")
}

func (s *VelocityService) logAttempt(ctx context.Context, w *domain.Withdrawal, sourceIP, countryCode string, passed bool, reason string) {
	attempt := &domain.WithdrawalAttempt{
		ID:                  uuid.New(),
		WithdrawalID:        w.ID,
		WorkspaceID:         w.WorkspaceID,
		UserID:              w.UserID,
		Chain:               w.Chain,
		TokenSymbol:         w.TokenSymbol,
		Amount:              w.Amount,
		VelocityCheckPassed: passed,
	}
	if sourceIP != "" {
		attempt.SourceIP = &sourceIP
	}
	if countryCode != "" {
		attempt.CountryCode = &countryCode
	}
	if reason != "" {
		attempt.RejectionReason = &reason
	}

	if err := s.velocityRepo.LogAttempt(ctx, attempt); err != nil {
		log.Error().Err(err).Str("withdrawal_id", w.ID.String()).Msg("failed to log withdrawal attempt")
	}
}
