package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/config"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	reorgsDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_reorgs_detected_total",
		Help: "Total chain reorganizations detected",
	}, []string{"chain"})

	reorgDepthHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_reorg_depth",
		Help:    "Depth of detected reorgs",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	}, []string{"chain"})

	reorgAffectedDeposits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_reorg_affected_deposits_total",
		Help: "Deposits affected by reorgs",
	}, []string{"chain"})

	chainFrozen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_chain_frozen",
		Help: "Whether a chain is frozen due to reorg (1=frozen, 0=normal)",
	}, []string{"chain"})
)

// ReorgDetector monitors block hashes to detect chain reorganizations.
// When a reorg is detected, it freezes the chain, marks affected deposits
// as REORGED, and waits for stabilization before resuming.
type ReorgDetector struct {
	reorgRepo   *repository.ReorgRepo
	depositRepo *repository.DepositRepo
	indexRepo   *repository.ChainIndexRepo
	adapters    map[string]*chain.EVMAdapter
	chains      []config.ChainConfig

	frozenChains map[string]bool
	mu           sync.RWMutex
	interval     time.Duration
	stopCh       chan struct{}
}

// NewReorgDetector creates a reorg detector
func NewReorgDetector(
	reorgRepo *repository.ReorgRepo,
	depositRepo *repository.DepositRepo,
	indexRepo *repository.ChainIndexRepo,
	adapters map[string]*chain.EVMAdapter,
	chains []config.ChainConfig,
) *ReorgDetector {
	return &ReorgDetector{
		reorgRepo:    reorgRepo,
		depositRepo:  depositRepo,
		indexRepo:    indexRepo,
		adapters:     adapters,
		chains:       chains,
		frozenChains: make(map[string]bool),
		interval:     30 * time.Second,
		stopCh:       make(chan struct{}),
	}
}

// Start begins the reorg detection loop
func (rd *ReorgDetector) Start(ctx context.Context) {
	log.Info().Int("chains", len(rd.adapters)).Msg("reorg detector started")
	ticker := time.NewTicker(rd.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rd.checkAllChains(ctx)
		case <-rd.stopCh:
			log.Info().Msg("reorg detector stopped")
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop halts the detector
func (rd *ReorgDetector) Stop() {
	close(rd.stopCh)
}

// IsChainFrozen returns whether a chain is frozen due to reorg
func (rd *ReorgDetector) IsChainFrozen(chain string) bool {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	return rd.frozenChains[chain]
}

func (rd *ReorgDetector) checkAllChains(ctx context.Context) {
	for _, cfg := range rd.chains {
		if !cfg.Enabled || cfg.ReorgDepth == 0 {
			continue
		}

		adapter, ok := rd.adapters[cfg.Name]
		if !ok {
			continue
		}

		// Skip if chain is already frozen (check stabilization instead)
		if rd.IsChainFrozen(cfg.Name) {
			rd.checkStabilization(ctx, cfg.Name, adapter)
			continue
		}

		if err := rd.checkChain(ctx, cfg.Name, adapter, cfg.ReorgDepth); err != nil {
			log.Error().Str("chain", cfg.Name).Err(err).Msg("reorg check failed")
		}
	}
}

// checkChain verifies block hash continuity for a chain
func (rd *ReorgDetector) checkChain(ctx context.Context, chainName string, adapter *chain.EVMAdapter, maxReorgDepth int) error {
	// Get our stored last block
	state, err := rd.indexRepo.GetState(ctx, chainName)
	if err != nil {
		return nil // no state yet, skip
	}

	// Verify the block hash we stored matches what the chain currently reports
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	currentHash, err := adapter.GetBlockHashHex(checkCtx, uint64(state.LastBlockNumber))
	cancel()
	if err != nil {
		return fmt.Errorf("get current block hash at %d: %w", state.LastBlockNumber, err)
	}

	if strings.EqualFold(currentHash, state.LastBlockHash) {
		return nil // no reorg at our tip
	}

	// REORG DETECTED — find the fork point
	log.Warn().
		Str("chain", chainName).
		Int64("block", state.LastBlockNumber).
		Str("expected", state.LastBlockHash).
		Str("actual", currentHash).
		Msg("⚠️ REORG DETECTED — block hash mismatch")

	forkBlock := state.LastBlockNumber
	depth := 0

	// Walk back to find where chain diverged
	for i := 1; i <= maxReorgDepth; i++ {
		blockNum := state.LastBlockNumber - int64(i)
		if blockNum < 0 {
			break
		}

		storedHash, err := rd.reorgRepo.GetBlockHash(ctx, chainName, blockNum)
		if err != nil {
			break // no stored hash that far back
		}

		walkCtx, walkCancel := context.WithTimeout(ctx, 10*time.Second)
		chainHash, err := adapter.GetBlockHashHex(walkCtx, uint64(blockNum))
		walkCancel()
		if err != nil {
			break
		}

		if strings.EqualFold(storedHash, chainHash) {
			forkBlock = blockNum + 1
			depth = i
			break
		}
	}

	if depth == 0 {
		depth = 1
	}

	reorgsDetected.WithLabelValues(chainName).Inc()
	reorgDepthHistogram.WithLabelValues(chainName).Observe(float64(depth))

	// Freeze the chain
	rd.mu.Lock()
	rd.frozenChains[chainName] = true
	rd.mu.Unlock()
	chainFrozen.WithLabelValues(chainName).Set(1)

	// Mark affected deposits as REORGED
	affectedCount := 0
	for _, depositState := range []domain.DepositState{
		domain.DepositDetected, domain.DepositPending, domain.DepositConfirming, domain.DepositConfirmed,
	} {
		deposits, err := rd.depositRepo.FindByChainAndState(ctx, chainName, depositState)
		if err != nil {
			continue
		}
		for _, d := range deposits {
			if d.BlockNumber >= forkBlock {
				if err := rd.depositRepo.UpdateState(ctx, d.ID, domain.DepositReorged, d.Confirmations); err != nil {
					log.Error().Str("deposit_id", d.ID.String()).Err(err).Msg("failed to mark deposit as REORGED")
				} else {
					affectedCount++
				}
			}
		}
	}

	reorgAffectedDeposits.WithLabelValues(chainName).Add(float64(affectedCount))

	// Record the reorg event
	reorgEvent := &repository.ReorgEvent{
		ID:               uuid.New(),
		Chain:            chainName,
		DetectedAtBlock:  state.LastBlockNumber,
		ForkBlock:        forkBlock,
		Depth:            depth,
		OldHash:          state.LastBlockHash,
		NewHash:          currentHash,
		AffectedDeposits: affectedCount,
	}
	if err := rd.reorgRepo.InsertReorgEvent(ctx, reorgEvent); err != nil {
		log.Error().Err(err).Msg("failed to record reorg event")
	}

	log.Error().
		Str("chain", chainName).
		Int("depth", depth).
		Int64("fork_block", forkBlock).
		Int("affected_deposits", affectedCount).
		Msg("🔴 CHAIN FROZEN — reorg detected and deposits marked")

	return nil
}

// checkStabilization verifies a frozen chain has stabilized
func (rd *ReorgDetector) checkStabilization(ctx context.Context, chainName string, adapter *chain.EVMAdapter) {
	// Check if new blocks are being produced consistently
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	latestBlock, err := adapter.LatestBlock(checkCtx)
	cancel()
	if err != nil {
		return
	}

	// Verify the latest block hash is stable across a few checks
	hash1Ctx, h1Cancel := context.WithTimeout(ctx, 10*time.Second)
	hash1, err := adapter.GetBlockHashHex(hash1Ctx, latestBlock-5)
	h1Cancel()
	if err != nil {
		return
	}

	// Wait briefly and check again
	time.Sleep(3 * time.Second)

	hash2Ctx, h2Cancel := context.WithTimeout(ctx, 10*time.Second)
	hash2, err := adapter.GetBlockHashHex(hash2Ctx, latestBlock-5)
	h2Cancel()
	if err != nil {
		return
	}

	if !strings.EqualFold(hash1, hash2) {
		log.Warn().Str("chain", chainName).Msg("chain still unstable — remaining frozen")
		return
	}

	// Chain appears stable — unfreeze
	rd.mu.Lock()
	rd.frozenChains[chainName] = false
	rd.mu.Unlock()
	chainFrozen.WithLabelValues(chainName).Set(0)

	// Resolve unresolved reorg events
	events, err := rd.reorgRepo.FindUnresolved(ctx, chainName)
	if err == nil {
		for _, e := range events {
			rd.reorgRepo.ResolveReorgEvent(ctx, e.ID)
		}
	}

	log.Info().Str("chain", chainName).Msg("✅ chain unfrozen — reorg resolved, re-indexing from fork point")
}

// SaveBlockHash stores a block hash for future reorg detection.
// Should be called by the deposit indexer for each processed block.
func (rd *ReorgDetector) SaveBlockHash(ctx context.Context, chain string, blockNum int64, hash string) error {
	return rd.reorgRepo.SaveBlockHash(ctx, chain, blockNum, hash)
}

// PruneOldHashes removes old block hashes to prevent unbounded storage growth
func (rd *ReorgDetector) PruneOldHashes(ctx context.Context, chain string, keepLatest int64) {
	pruned, err := rd.reorgRepo.PruneOldHashes(ctx, chain, keepLatest)
	if err != nil {
		log.Warn().Str("chain", chain).Err(err).Msg("failed to prune old block hashes")
		return
	}
	if pruned > 0 {
		log.Debug().Str("chain", chain).Int64("pruned", pruned).Msg("pruned old block hashes")
	}
}
