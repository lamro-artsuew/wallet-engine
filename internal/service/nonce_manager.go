package service

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	nonceReserved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_nonce_reserved_total",
		Help: "Total nonces reserved per chain",
	}, []string{"chain", "purpose"})

	nonceCollisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_nonce_collisions_total",
		Help: "Nonce reservation collisions (retry needed)",
	}, []string{"chain"})

	nonceSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_nonce_sync_errors_total",
		Help: "Errors syncing on-chain nonce",
	}, []string{"chain"})
)

// NonceManager provides atomic nonce management per chain/address.
// It reconciles on-chain nonce with local reservations to prevent collisions.
type NonceManager struct {
	nonceRepo *repository.NonceRepo
	adapters  map[string]*chain.EVMAdapter
	mu        sync.Mutex // serialize reservations to prevent race conditions
}

// NewNonceManager creates a nonce manager
func NewNonceManager(nonceRepo *repository.NonceRepo, adapters map[string]*chain.EVMAdapter) *NonceManager {
	return &NonceManager{
		nonceRepo: nonceRepo,
		adapters:  adapters,
	}
}

// ReserveNonce atomically reserves the next nonce for an address on a chain.
// It queries the on-chain pending nonce and takes the max of (on-chain, local max + 1).
func (nm *NonceManager) ReserveNonce(ctx context.Context, chainName, address, purpose string, refID *uuid.UUID) (int64, *uuid.UUID, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	adapter, ok := nm.adapters[chainName]
	if !ok {
		return 0, nil, fmt.Errorf("no adapter for chain %s", chainName)
	}

	// Get on-chain pending nonce
	onChainNonce, err := adapter.PendingNonceAt(ctx, common.HexToAddress(address))
	if err != nil {
		nonceSyncErrors.WithLabelValues(chainName).Inc()
		return 0, nil, fmt.Errorf("get on-chain nonce for %s on %s: %w", address, chainName, err)
	}

	// Reserve atomically (DB handles max of on-chain vs local)
	reservation, err := nm.nonceRepo.ReserveNext(ctx, chainName, address, int64(onChainNonce), purpose, refID)
	if err != nil {
		nonceCollisions.WithLabelValues(chainName).Inc()
		return 0, nil, fmt.Errorf("reserve nonce: %w", err)
	}

	nonceReserved.WithLabelValues(chainName, purpose).Inc()
	log.Debug().
		Str("chain", chainName).
		Str("address", address).
		Int64("nonce", reservation.Nonce).
		Str("purpose", purpose).
		Msg("nonce reserved")

	return reservation.Nonce, &reservation.ID, nil
}

// ConfirmNonce marks a reserved nonce as broadcast (tx submitted to mempool)
func (nm *NonceManager) ConfirmBroadcast(ctx context.Context, reservationID uuid.UUID, txHash string) error {
	return nm.nonceRepo.UpdateState(ctx, reservationID, "BROADCAST", txHash)
}

// ConfirmNonce marks a nonce as confirmed (tx included in block)
func (nm *NonceManager) ConfirmOnChain(ctx context.Context, reservationID uuid.UUID) error {
	return nm.nonceRepo.UpdateState(ctx, reservationID, "CONFIRMED", "")
}

// FailNonce marks a nonce reservation as failed
func (nm *NonceManager) FailNonce(ctx context.Context, reservationID uuid.UUID) error {
	return nm.nonceRepo.UpdateState(ctx, reservationID, "FAILED", "")
}

// ReconcileOnChain checks if the on-chain nonce matches our local tracking.
// Returns (expected, actual, matched).
func (nm *NonceManager) ReconcileOnChain(ctx context.Context, chainName, address string) (int64, int64, bool, error) {
	adapter, ok := nm.adapters[chainName]
	if !ok {
		return 0, 0, false, fmt.Errorf("no adapter for chain %s", chainName)
	}

	onChainNonce, err := adapter.NonceAt(ctx, common.HexToAddress(address))
	if err != nil {
		return 0, 0, false, fmt.Errorf("get on-chain nonce: %w", err)
	}

	localMax, err := nm.nonceRepo.GetMaxReservedNonce(ctx, chainName, address)
	if err != nil {
		return 0, int64(onChainNonce), false, fmt.Errorf("get local max nonce: %w", err)
	}

	// On-chain confirmed nonce should be <= max reserved + 1
	matched := int64(onChainNonce) <= localMax+1
	if !matched {
		log.Warn().
			Str("chain", chainName).
			Str("address", address).
			Int64("on_chain", int64(onChainNonce)).
			Int64("local_max", localMax).
			Msg("nonce mismatch detected — possible external transaction")
	}

	return localMax, int64(onChainNonce), matched, nil
}

// Stub to suppress unused import warning
var _ = new(big.Int)
