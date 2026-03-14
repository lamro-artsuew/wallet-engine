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

	// blacklistedAddressesTotal is refreshed from DB on each Prometheus scrape,
	// not incremented/decremented in-process (which drifts on restart, ON CONFLICT,
	// or multi-instance deployments).
	blacklistedAddressesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_engine_blacklisted_addresses_total",
		Help: "Total active blacklisted addresses (refreshed from DB)",
	})
)

const (
	// onchainCheckTimeout is the aggregate timeout for all on-chain blacklist checks
	// within a single CheckAddress call.
	onchainCheckTimeout = 10 * time.Second
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

// RefreshBlacklistGauge queries the DB for the actual count of active blacklisted
// addresses and updates the Prometheus gauge. Call periodically (e.g., every 60s)
// or from a Prometheus Collector.
func (s *BlacklistService) RefreshBlacklistGauge(ctx context.Context) {
	count, err := s.blacklistRepo.CountActiveBlacklisted(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to refresh blacklist gauge from DB")
		return
	}
	blacklistedAddressesTotal.Set(float64(count))
}

// CheckAddress checks if an address is blacklisted and/or frozen on-chain.
// Returns a result with CheckedAt timestamp for staleness detection by callers.
func (s *BlacklistService) CheckAddress(ctx context.Context, chainName string, address string) (*domain.BlacklistCheckResult, error) {
	result := &domain.BlacklistCheckResult{
		FreezeStatus: make(map[string]bool),
		CheckedAt:    time.Now(),
	}

	// Normalize EVM addresses to lowercase at the service boundary
	if _, hasAdapter := s.chains[chainName]; hasAdapter {
		address = strings.ToLower(address)
	}

	// 1. Check local blacklist DB (primary source — detail needed for sources/reasons)
	entries, err := s.blacklistRepo.GetBlacklistEntries(ctx, chainName, address)
	if err != nil {
		return nil, fmt.Errorf("check blacklist DB: %w", err)
	}

	seen := make(map[string]bool)
	for _, entry := range entries {
		result.IsBlacklisted = true
		if !seen[entry.Source] {
			result.Sources = append(result.Sources, entry.Source)
			seen[entry.Source] = true
		}
		if entry.Reason != nil {
			result.Reasons = append(result.Reasons, *entry.Reason)
		}
	}

	// 2. Check on-chain blacklist status for stablecoin contracts (fail-open)
	adapter, hasAdapter := s.chains[chainName]
	if hasAdapter {
		// Apply aggregate timeout for all on-chain RPC calls
		onchainCtx, cancel := context.WithTimeout(ctx, onchainCheckTimeout)
		defer cancel()

		contracts, err := s.blacklistRepo.GetStablecoinContracts(onchainCtx, chainName)
		if err != nil {
			log.Warn().Err(err).Str("chain", chainName).Msg("failed to load stablecoin contracts, skipping on-chain check")
		} else {
			for _, contract := range contracts {
				if !contract.HasBlacklist || contract.BlacklistMethod == nil {
					continue
				}

				isBlocked, err := adapter.CheckAddressBlacklist(
					onchainCtx,
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
					if !seen[source] {
						result.Sources = append(result.Sources, source)
						seen[source] = true
					}
					result.FreezeStatus[contract.ContractAddress] = true

					// Freeze event audit logging — failure is CRITICAL (audit trail gap)
					if err := s.blacklistRepo.LogFreezeEvent(onchainCtx, &domain.FreezeEvent{
						ID:              uuid.New(),
						Chain:           chainName,
						Address:         address,
						ContractAddress: contract.ContractAddress,
						EventType:       "BLACKLISTED",
						DetectedAt:      time.Now(),
					}); err != nil {
						log.Error().Err(err).
							Str("address", address).
							Str("contract", contract.ContractAddress).
							Str("chain", chainName).
							Msg("CRITICAL: failed to log freeze event — audit trail gap")
					}
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

// CheckDepositSource checks if a deposit sender is blacklisted.
// Delegates to CheckAddress — exists as a named entry point for deposit flow clarity.
func (s *BlacklistService) CheckDepositSource(ctx context.Context, chainName string, fromAddress string) (*domain.BlacklistCheckResult, error) {
	return s.CheckAddress(ctx, chainName, fromAddress)
}

// SyncOFACList syncs the OFAC SDN list.
// TODO(P0-COMPLIANCE): This is a stub. DFSA Category 3C requires OFAC screening
// on every withdrawal. Without this, the system is non-compliant for sanctioned
// address detection on non-stablecoin chains. Implement using OFAC SDN CSV feed
// or a sanctions screening API (e.g., Chainalysis, Elliptic).
func (s *BlacklistService) SyncOFACList(ctx context.Context) error {
	log.Warn().Msg("OFAC SDN list sync is NOT IMPLEMENTED — regulatory compliance gap")
	return nil
}

// AddToBlacklist adds an address to the blacklist.
// ID and timestamp assignment are handled by the repo layer (single owner).
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

	// Refresh gauge from DB instead of in-process increment
	s.RefreshBlacklistGauge(ctx)
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
	s.RefreshBlacklistGauge(ctx)
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
