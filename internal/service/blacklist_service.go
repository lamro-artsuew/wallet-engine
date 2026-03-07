package service

import (
	"context"
	"fmt"
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
	blacklistChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_blacklist_checks_total",
		Help: "Blacklist check results",
	}, []string{"chain", "result"})

	blacklistedAddressesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_engine_blacklisted_addresses_total",
		Help: "Total blacklisted addresses",
	})
)

// BlacklistService manages address blacklist/freeze monitoring
type BlacklistService struct {
	blacklistRepo *repository.BlacklistRepo
	chains        map[string]*chain.EVMAdapter
}

// NewBlacklistService creates a new blacklist service
func NewBlacklistService(
	blacklistRepo *repository.BlacklistRepo,
	chains map[string]*chain.EVMAdapter,
) *BlacklistService {
	return &BlacklistService{
		blacklistRepo: blacklistRepo,
		chains:        chains,
	}
}

// CheckAddress checks if an address is blacklisted and/or frozen on-chain
func (s *BlacklistService) CheckAddress(ctx context.Context, chainName string, address string) (*domain.BlacklistCheckResult, error) {
	result := &domain.BlacklistCheckResult{
		FreezeStatus: make(map[string]bool),
	}

	// 1. Check local blacklist DB (primary source)
	entries, err := s.blacklistRepo.IsBlacklisted(ctx, chainName, address)
	if err != nil {
		return nil, fmt.Errorf("check blacklist DB: %w", err)
	}

	for _, entry := range entries {
		result.IsBlacklisted = true
		result.Sources = append(result.Sources, entry.Source)
		if entry.Reason != nil {
			result.Reasons = append(result.Reasons, *entry.Reason)
		}
	}

	// 2. Check on-chain blacklist status for stablecoin contracts (fail-open)
	adapter, hasAdapter := s.chains[chainName]
	if hasAdapter {
		contracts, err := s.blacklistRepo.GetStablecoinContracts(ctx, chainName)
		if err != nil {
			log.Warn().Err(err).Str("chain", chainName).Msg("failed to load stablecoin contracts, skipping on-chain check")
		} else {
			for _, contract := range contracts {
				if !contract.HasBlacklist || contract.BlacklistMethod == nil {
					continue
				}

				isBlocked, err := adapter.CheckAddressBlacklist(
					ctx,
					common.HexToAddress(contract.ContractAddress),
					*contract.BlacklistMethod,
					common.HexToAddress(address),
				)
				if err != nil {
					log.Warn().Err(err).
						Str("chain", chainName).
						Str("contract", contract.ContractAddress).
						Str("symbol", contract.Symbol).
						Str("address", address).
						Msg("on-chain blacklist check failed (fail-open)")
					continue
				}

				if isBlocked {
					result.IsBlacklisted = true
					source := fmt.Sprintf("%s_BLACKLIST", strings.ToUpper(contract.Symbol))
					result.Sources = append(result.Sources, source)
					result.FreezeStatus[contract.ContractAddress] = true

					s.blacklistRepo.LogFreezeEvent(ctx, &domain.FreezeEvent{
						Chain:           chainName,
						Address:         address,
						ContractAddress: contract.ContractAddress,
						EventType:       "BLACKLISTED",
					})
				}
			}
		}
	}

	if result.IsBlacklisted {
		blacklistChecksTotal.WithLabelValues(chainName, "blocked").Inc()
	} else {
		blacklistChecksTotal.WithLabelValues(chainName, "clean").Inc()
	}

	return result, nil
}

// CheckDepositSource checks if a deposit sender is blacklisted
func (s *BlacklistService) CheckDepositSource(ctx context.Context, chainName string, fromAddress string) (*domain.BlacklistCheckResult, error) {
	return s.CheckAddress(ctx, chainName, fromAddress)
}

// SyncOFACList syncs the OFAC SDN list (stub — logs that it would sync)
func (s *BlacklistService) SyncOFACList(ctx context.Context) error {
	log.Info().Msg("OFAC SDN list sync would run here — not yet implemented")
	return nil
}

// AddToBlacklist adds an address to the blacklist
func (s *BlacklistService) AddToBlacklist(ctx context.Context, entry *domain.BlacklistedAddress) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.DetectedAt.IsZero() {
		entry.DetectedAt = time.Now()
	}
	entry.IsActive = true

	if err := s.blacklistRepo.AddToBlacklist(ctx, entry); err != nil {
		return err
	}

	blacklistedAddressesTotal.Inc()
	log.Info().
		Str("chain", entry.Chain).
		Str("address", entry.Address).
		Str("source", entry.Source).
		Msg("address added to blacklist")
	return nil
}

// RemoveFromBlacklist soft-deletes a blacklist entry
func (s *BlacklistService) RemoveFromBlacklist(ctx context.Context, id uuid.UUID) error {
	if err := s.blacklistRepo.RemoveFromBlacklist(ctx, id); err != nil {
		return err
	}
	blacklistedAddressesTotal.Dec()
	log.Info().Str("id", id.String()).Msg("blacklist entry deactivated")
	return nil
}

// ListBlacklisted returns paginated blacklist entries
func (s *BlacklistService) ListBlacklisted(ctx context.Context, chain string, limit, offset int) ([]*domain.BlacklistedAddress, error) {
	return s.blacklistRepo.ListBlacklisted(ctx, chain, limit, offset)
}

// ListStablecoins returns all active stablecoin contracts
func (s *BlacklistService) ListStablecoins(ctx context.Context) ([]*domain.StablecoinContract, error) {
	return s.blacklistRepo.GetAllStablecoinContracts(ctx)
}
