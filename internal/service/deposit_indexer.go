package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/messaging"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/config"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	blocksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_blocks_processed_total",
		Help: "Total blocks processed per chain",
	}, []string{"chain"})

	depositsDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_deposits_detected_total",
		Help: "Total deposits detected per chain",
	}, []string{"chain", "token"})

	indexerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_indexer_lag_blocks",
		Help: "Number of blocks behind chain tip",
	}, []string{"chain"})

	indexerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_indexer_errors_total",
		Help: "Indexer errors per chain",
	}, []string{"chain", "error_type"})

	scanDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_block_scan_duration_seconds",
		Help:    "Time to scan a block for transfers",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"chain"})
)

// DepositIndexer runs per-chain goroutines to detect inbound transfers
type DepositIndexer struct {
	chains       map[string]*chainIndexer
	depositRepo  *repository.DepositRepo
	addrRepo     *repository.DepositAddressRepo
	indexRepo    *repository.ChainIndexRepo
	producer     *messaging.RedpandaProducer
	blacklistSvc *BlacklistService
	mu           sync.RWMutex
	cancel       context.CancelFunc
}

type chainIndexer struct {
	adapter       chain.ChainAdapter
	cfg           config.ChainConfig
	logger        zerolog.Logger
	watchAddrs    map[string]*domain.DepositAddress // canonical address string → deposit address
	watchAddrsMu  sync.RWMutex
}

// NewDepositIndexer creates a new deposit indexer
func NewDepositIndexer(
	chainConfigs []config.ChainConfig,
	depositRepo *repository.DepositRepo,
	addrRepo *repository.DepositAddressRepo,
	indexRepo *repository.ChainIndexRepo,
	producer *messaging.RedpandaProducer,
) *DepositIndexer {
	chains := make(map[string]*chainIndexer, len(chainConfigs))
	for _, cfg := range chainConfigs {
		if !cfg.Enabled {
			continue
		}
		var adapter chain.ChainAdapter
		switch cfg.ChainType {
		case "tron":
			opts := []chain.TRONAdapterOption{}
			if cfg.APIKey != "" {
				opts = append(opts, chain.WithTRONAPIKey(cfg.APIKey))
			}
			adapter = chain.NewTRONAdapter(cfg.Name, cfg.RPCURL, cfg.TrackedTokens, opts...)
		default: // "evm" or unset
			adapter = chain.NewEVMAdapter(
				cfg.Name, cfg.ChainID, cfg.RPCURL, cfg.NativeSymbol, cfg.TrackedTokens,
				chain.WithRPCURLs(cfg.RPCURLs),
				chain.WithNativeDecimals(cfg.NativeDecimals),
				chain.WithRPCTimeout(time.Duration(cfg.RPCTimeout)*time.Second),
				chain.WithRPCRetries(cfg.RPCRetries),
			)
		}
		chains[cfg.Name] = &chainIndexer{
			adapter:    adapter,
			cfg:        cfg,
			logger:     log.With().Str("chain", cfg.Name).Logger(),
			watchAddrs: make(map[string]*domain.DepositAddress),
		}
	}

	return &DepositIndexer{
		chains:      chains,
		depositRepo: depositRepo,
		addrRepo:    addrRepo,
		indexRepo:   indexRepo,
		producer:    producer,
	}
}

// Start begins indexing all configured chains
func (di *DepositIndexer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	di.cancel = cancel

	for name, ci := range di.chains {
		if err := ci.adapter.Connect(ctx); err != nil {
			log.Warn().Err(err).Str("chain", name).Msg("failed to connect, will retry in background")
			go di.retryConnect(ctx, ci)
			continue
		}

		// Load existing deposit addresses into watch set
		if err := di.refreshWatchAddresses(ctx, ci); err != nil {
			log.Error().Err(err).Str("chain", name).Msg("failed to load watch addresses")
		}

		go di.runChainIndexer(ctx, ci)
	}

	return nil
}

// retryConnect periodically attempts to connect to a chain that was unavailable at startup
func (di *DepositIndexer) retryConnect(ctx context.Context, ci *chainIndexer) {
	retryInterval := 30 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}

		if err := ci.adapter.Connect(ctx); err != nil {
			ci.logger.Debug().Err(err).Msg("chain still unavailable, retrying...")
			if retryInterval < 5*time.Minute {
				retryInterval = retryInterval * 2
			}
			continue
		}

		ci.logger.Info().Msg("chain connected after retry, starting indexer")
		if err := di.refreshWatchAddresses(ctx, ci); err != nil {
			ci.logger.Error().Err(err).Msg("failed to load watch addresses")
		}
		go di.runChainIndexer(ctx, ci)
		return
	}
}

// Stop halts all indexers
func (di *DepositIndexer) Stop() {
	if di.cancel != nil {
		di.cancel()
	}
	for _, ci := range di.chains {
		ci.adapter.Close()
	}
}

// AddWatchAddress adds an address to the watch set for a chain
func (di *DepositIndexer) AddWatchAddress(chainName string, addr string, da *domain.DepositAddress) {
	if ci, ok := di.chains[chainName]; ok {
		ci.watchAddrsMu.Lock()
		ci.watchAddrs[strings.ToLower(addr)] = da
		ci.watchAddrsMu.Unlock()
	}
}

// GetEVMAdapters returns EVM-specific adapters for services that need them (e.g., BlacklistService).
// TRON and other non-EVM chains are excluded.
func (di *DepositIndexer) GetEVMAdapters() map[string]*chain.EVMAdapter {
	adapters := make(map[string]*chain.EVMAdapter)
	for name, ci := range di.chains {
		if evm, ok := ci.adapter.(*chain.EVMAdapter); ok {
			adapters[name] = evm
		}
	}
	return adapters
}

// SetBlacklistService sets the blacklist service for deposit screening
func (di *DepositIndexer) SetBlacklistService(svc *BlacklistService) {
	di.blacklistSvc = svc
}

func (di *DepositIndexer) refreshWatchAddresses(ctx context.Context, ci *chainIndexer) error {
	addrs, err := di.addrRepo.FindByChain(ctx, ci.cfg.Name)
	if err != nil {
		return err
	}
	ci.watchAddrsMu.Lock()
	defer ci.watchAddrsMu.Unlock()
	for _, a := range addrs {
		ci.watchAddrs[strings.ToLower(a.Address)] = a
	}
	ci.logger.Info().Int("addresses", len(ci.watchAddrs)).Msg("loaded watch addresses")
	return nil
}

func (di *DepositIndexer) runChainIndexer(ctx context.Context, ci *chainIndexer) {
	ci.logger.Info().Msg("starting chain indexer")

	// Determine start block: saved state > configured StartBlock > current chain head
	startBlock := ci.cfg.StartBlock
	state, err := di.indexRepo.GetState(ctx, ci.cfg.Name)
	if err == nil && state.LastBlockNumber > startBlock {
		startBlock = state.LastBlockNumber + 1
	} else if startBlock <= 0 {
		// No saved state and no configured start block — start from current head.
		// Custody wallets only need to detect deposits from deployment time forward.
		latest, latestErr := ci.adapter.LatestBlock(ctx)
		if latestErr == nil && latest > 0 {
			startBlock = int64(latest) - int64(ci.cfg.Confirmations)
			if startBlock < 1 {
				startBlock = 1
			}
			ci.logger.Info().Int64("startBlock", startBlock).Uint64("chainHead", latest).
				Msg("no saved state, starting from near chain head")
		} else {
			ci.logger.Warn().Err(latestErr).Msg("cannot determine chain head, will retry")
			startBlock = 0
		}
	}

	pollInterval := time.Duration(ci.cfg.BlockTime) * time.Second
	if pollInterval < 1*time.Second {
		pollInterval = 1 * time.Second
	}

	currentBlock := startBlock
	consecutiveErrors := 0
	maxConsecutiveErrors := 10

	for {
		select {
		case <-ctx.Done():
			ci.logger.Info().Msg("chain indexer stopped")
			return
		default:
		}

		latestBlock, err := ci.adapter.LatestBlock(ctx)
		if err != nil {
			consecutiveErrors++
			indexerErrors.WithLabelValues(ci.cfg.Name, "rpc_latest_block").Inc()
			ci.logger.Error().Err(err).Int("consecutive", consecutiveErrors).Msg("failed to get latest block")

			if consecutiveErrors >= maxConsecutiveErrors {
				ci.logger.Error().Msg("too many consecutive errors, backing off")
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
					return
				}
				consecutiveErrors = 0
			}
			continue
		}
		consecutiveErrors = 0

		// If we still don't have a valid start block (initial head query failed), set it now
		if currentBlock <= 0 {
			currentBlock = int64(latestBlock) - int64(ci.cfg.Confirmations)
			if currentBlock < 1 {
				currentBlock = 1
			}
			ci.logger.Info().Int64("startBlock", currentBlock).Uint64("chainHead", latestBlock).
				Msg("resolved start block from chain head")
		}

		// Fast-forward if indexer is too far behind (>10K blocks).
		// Custody deposit detection only needs real-time monitoring —
		// scanning millions of old blocks wastes resources.
		maxLag := int64(10000)
		lag := int64(latestBlock) - currentBlock
		if lag > maxLag {
			newStart := int64(latestBlock) - int64(ci.cfg.Confirmations)
			ci.logger.Warn().
				Int64("oldBlock", currentBlock).
				Int64("newBlock", newStart).
				Int64("skipped", lag).
				Msg("indexer too far behind, fast-forwarding to near chain head")
			currentBlock = newStart
			// Persist fast-forward position immediately so other replicas see it
			if uerr := di.indexRepo.UpsertState(ctx, ci.cfg.Name, currentBlock, "fast-forward"); uerr != nil {
				ci.logger.Error().Err(uerr).Msg("failed to persist fast-forward position")
			}
		}

		// Calculate lag
		if int64(latestBlock) > currentBlock {
			indexerLag.WithLabelValues(ci.cfg.Name).Set(float64(int64(latestBlock) - currentBlock))
		}

		// Process blocks up to (latest - confirmations) for safety
		safeBlock := int64(latestBlock) - int64(ci.cfg.Confirmations)
		if safeBlock < 0 {
			safeBlock = 0
		}

		for currentBlock <= safeBlock {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Use a timeout for individual block processing to avoid hanging on stalled RPC
			blockCtx, blockCancel := context.WithTimeout(ctx, 30*time.Second)
			err := di.processBlock(blockCtx, ci, uint64(currentBlock))
			blockCancel()
			if err != nil {
				indexerErrors.WithLabelValues(ci.cfg.Name, "process_block").Inc()
				ci.logger.Error().Err(err).Int64("block", currentBlock).Msg("failed to process block")
				time.Sleep(2 * time.Second)
				break
			}

			blocksProcessed.WithLabelValues(ci.cfg.Name).Inc()
			currentBlock++
		}

		// Also update confirmation counts for pending deposits
		di.updateConfirmations(ctx, ci, int64(latestBlock))

		// Wait for new blocks
		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return
		}
	}
}

func (di *DepositIndexer) processBlock(ctx context.Context, ci *chainIndexer, blockNum uint64) error {
	start := time.Now()
	defer func() {
		scanDuration.WithLabelValues(ci.cfg.Name).Observe(time.Since(start).Seconds())
	}()

	// Build current watch set (string-based, chain-agnostic)
	ci.watchAddrsMu.RLock()
	watchSet := make(map[string]bool, len(ci.watchAddrs))
	for addr := range ci.watchAddrs {
		watchSet[addr] = true
	}
	ci.watchAddrsMu.RUnlock()

	if len(watchSet) == 0 {
		// No addresses to watch, just update cursor
		blockHash, err := ci.adapter.GetBlockHashHex(ctx, blockNum)
		if err != nil {
			return err
		}
		return di.indexRepo.UpsertState(ctx, ci.cfg.Name, int64(blockNum), blockHash)
	}

	// Scan block for transfers to our addresses (chain-agnostic interface)
	events, err := ci.adapter.ScanBlockForDeposits(ctx, blockNum, watchSet)
	if err != nil {
		return fmt.Errorf("scan block %d: %w", blockNum, err)
	}

	// Process detected transfers
	for _, evt := range events {
		ci.watchAddrsMu.RLock()
		da := ci.watchAddrs[strings.ToLower(evt.To)]
		ci.watchAddrsMu.RUnlock()

		if da == nil {
			continue
		}

		// Determine token symbol
		tokenSymbol := ci.adapter.GetTokenSymbolByAddr(evt.TokenAddress)
		if evt.IsNative {
			tokenSymbol = ci.adapter.NativeSymbol()
		}

		deposit := &domain.Deposit{
			ID:               uuid.New(),
			IdempotencyKey:   fmt.Sprintf("%s:%s:%d", ci.cfg.Name, evt.TxHash, evt.LogIndex),
			Chain:            ci.cfg.Name,
			TxHash:           evt.TxHash,
			LogIndex:         int(evt.LogIndex),
			BlockNumber:      int64(evt.BlockNumber),
			BlockHash:        evt.BlockHash,
			FromAddress:      evt.From,
			ToAddress:        evt.To,
			TokenAddress:     evt.TokenAddress,
			TokenSymbol:      tokenSymbol,
			Amount:           evt.Amount,
			Decimals:         evt.Decimals,
			State:            domain.DepositDetected,
			Confirmations:    0,
			RequiredConfs:    ci.cfg.Confirmations,
			DepositAddressID: da.ID,
			WorkspaceID:      da.WorkspaceID,
			UserID:           da.UserID,
			DetectedAt:       time.Now(),
		}

		// Check deposit source against blacklist
		if di.blacklistSvc != nil {
			blResult, blErr := di.blacklistSvc.CheckDepositSource(ctx, ci.cfg.Name, evt.From)
			if blErr != nil {
				ci.logger.Warn().Err(blErr).Str("from", evt.From).
					Msg("blacklist check failed for deposit source, recording deposit anyway")
			} else if blResult.IsBlacklisted {
				deposit.IsFromBlacklisted = true
				ci.logger.Warn().
					Str("from", evt.From).
					Str("to", evt.To).
					Strs("sources", blResult.Sources).
					Str("tx", evt.TxHash).
					Msg("deposit from blacklisted address detected — flagged")
			}
		}

		if err := di.depositRepo.Upsert(ctx, deposit); err != nil {
			ci.logger.Error().Err(err).
				Str("tx", evt.TxHash).
				Str("to", evt.To).
				Msg("failed to record deposit")
			continue
		}

		depositsDetected.WithLabelValues(ci.cfg.Name, deposit.TokenSymbol).Inc()

		// Publish event to Redpanda
		if di.producer != nil {
			di.producer.PublishDeposit(ctx, deposit)
		}

		ci.logger.Info().
			Str("tx", evt.TxHash).
			Str("from", evt.From).
			Str("to", evt.To).
			Str("token", deposit.TokenSymbol).
			Str("amount", evt.Amount.String()).
			Bool("native", evt.IsNative).
			Int64("block", int64(evt.BlockNumber)).
			Msg("deposit detected")
	}

	// Update cursor
	blockHash, err := ci.adapter.GetBlockHashHex(ctx, blockNum)
	if err != nil {
		return err
	}
	return di.indexRepo.UpsertState(ctx, ci.cfg.Name, int64(blockNum), blockHash)
}

// updateConfirmations updates confirmation count for pending deposits
func (di *DepositIndexer) updateConfirmations(ctx context.Context, ci *chainIndexer, latestBlock int64) {
	// Find deposits that are still confirming
	for _, state := range []domain.DepositState{domain.DepositDetected, domain.DepositPending, domain.DepositConfirming} {
		deposits, err := di.depositRepo.FindByChainAndState(ctx, ci.cfg.Name, state)
		if err != nil {
			ci.logger.Error().Err(err).Str("state", string(state)).Msg("failed to query pending deposits")
			continue
		}

		for _, d := range deposits {
			confs := int(latestBlock - d.BlockNumber)
			if confs < 0 {
				confs = 0
			}

			var newState domain.DepositState
			switch {
			case confs >= d.RequiredConfs:
				newState = domain.DepositConfirmed
			case confs > 0:
				newState = domain.DepositConfirming
			default:
				newState = domain.DepositPending
			}

			if newState != d.State || confs != d.Confirmations {
				if err := di.depositRepo.UpdateState(ctx, d.ID, newState, confs); err != nil {
					ci.logger.Error().Err(err).Str("deposit", d.ID.String()).Msg("failed to update deposit state")
				}
			}
		}
	}
}

// GetChainHealth returns health info for each chain
func (di *DepositIndexer) GetChainHealth(ctx context.Context) map[string]interface{} {
	result := make(map[string]interface{})
	for name, ci := range di.chains {
		health := map[string]interface{}{
			"connected": false,
			"chain_id":  ci.cfg.ChainID,
		}
		if err := ci.adapter.Health(ctx); err == nil {
			health["connected"] = true
			if latest, err := ci.adapter.LatestBlock(ctx); err == nil {
				health["latest_block"] = latest
			}
		}
		if state, err := di.indexRepo.GetState(ctx, name); err == nil {
			health["last_indexed_block"] = state.LastBlockNumber
		}
		result[name] = health
	}
	return result
}
