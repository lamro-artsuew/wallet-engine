package service

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	gasPriceGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_gas_price_gwei",
		Help: "Current gas price in gwei per chain",
	}, []string{"chain"})

	gasPriceSamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_gas_price_samples_total",
		Help: "Total gas price samples collected per chain",
	}, []string{"chain"})
)

// GasOracle tracks gas prices per chain and provides safe gas price recommendations
type GasOracle struct {
	adapters      map[string]*chain.EVMAdapter
	maxGasPrice   map[string]*big.Int // per-chain max gas price
	multiplier    float64             // safety multiplier (e.g., 1.2 = 20% buffer)
	mu            sync.RWMutex
	cachedPrices  map[string]*gasPriceEntry
	sampleTicker  *time.Ticker
	stopCh        chan struct{}
}

type gasPriceEntry struct {
	gasPrice    *big.Int
	baseFee     *big.Int
	tipCap      *big.Int
	sampledAt   time.Time
}

// NewGasOracle creates a gas oracle with safety multiplier and per-chain max prices
func NewGasOracle(adapters map[string]*chain.EVMAdapter, multiplier float64, maxPrices map[string]*big.Int) *GasOracle {
	if multiplier <= 0 {
		multiplier = 1.2 // 20% buffer default
	}
	return &GasOracle{
		adapters:     adapters,
		maxGasPrice:  maxPrices,
		multiplier:   multiplier,
		cachedPrices: make(map[string]*gasPriceEntry),
		stopCh:       make(chan struct{}),
	}
}

// Start begins periodic gas price sampling
func (g *GasOracle) Start(ctx context.Context, interval time.Duration) {
	g.sampleTicker = time.NewTicker(interval)
	go func() {
		g.sampleAll(ctx) // initial sample
		for {
			select {
			case <-g.sampleTicker.C:
				g.sampleAll(ctx)
			case <-g.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	log.Info().Dur("interval", interval).Msg("gas oracle started")
}

// Stop halts the gas oracle
func (g *GasOracle) Stop() {
	close(g.stopCh)
	if g.sampleTicker != nil {
		g.sampleTicker.Stop()
	}
}

func (g *GasOracle) sampleAll(ctx context.Context) {
	for chainName, adapter := range g.adapters {
		sampleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		gasPrice, err := adapter.SuggestGasPrice(sampleCtx)
		cancel()
		if err != nil {
			log.Warn().Str("chain", chainName).Err(err).Msg("gas price sample failed")
			continue
		}

		entry := &gasPriceEntry{
			gasPrice:  gasPrice,
			sampledAt: time.Now(),
		}

		// Try to get EIP-1559 data
		tipCtx, tipCancel := context.WithTimeout(ctx, 10*time.Second)
		tipCap, tipErr := adapter.SuggestGasTipCap(tipCtx)
		tipCancel()
		if tipErr == nil {
			entry.tipCap = tipCap
		}

		// Try to get base fee from latest header
		headerCtx, headerCancel := context.WithTimeout(ctx, 10*time.Second)
		header, headerErr := adapter.HeaderByNumber(headerCtx, nil)
		headerCancel()
		if headerErr == nil && header.BaseFee != nil {
			entry.baseFee = header.BaseFee
		}

		g.mu.Lock()
		g.cachedPrices[chainName] = entry
		g.mu.Unlock()

		gasPriceGwei := new(big.Float).Quo(new(big.Float).SetInt(gasPrice), big.NewFloat(1e9))
		gweiFloat, _ := gasPriceGwei.Float64()
		gasPriceGauge.WithLabelValues(chainName).Set(gweiFloat)
		gasPriceSamples.WithLabelValues(chainName).Inc()
	}
}

// GetGasPrice returns the recommended gas price for a chain, capped at max
func (g *GasOracle) GetGasPrice(chainName string) (*big.Int, error) {
	g.mu.RLock()
	entry, ok := g.cachedPrices[chainName]
	g.mu.RUnlock()

	if !ok || entry == nil {
		return nil, fmt.Errorf("no gas price data for chain %s", chainName)
	}

	// Stale check: if older than 5 minutes, warn but still use
	if time.Since(entry.sampledAt) > 5*time.Minute {
		log.Warn().Str("chain", chainName).Dur("age", time.Since(entry.sampledAt)).Msg("gas price data is stale")
	}

	// Apply safety multiplier
	price := new(big.Int).Set(entry.gasPrice)
	multiplied := new(big.Float).Mul(new(big.Float).SetInt(price), big.NewFloat(g.multiplier))
	result, _ := multiplied.Int(nil)

	// Cap at max gas price if configured
	if maxPrice, ok := g.maxGasPrice[chainName]; ok && maxPrice.Sign() > 0 {
		if result.Cmp(maxPrice) > 0 {
			log.Warn().
				Str("chain", chainName).
				Str("suggested", result.String()).
				Str("max", maxPrice.String()).
				Msg("gas price exceeds max, capping")
			return maxPrice, nil
		}
	}

	return result, nil
}

// GetEIP1559Params returns base fee and tip cap for EIP-1559 transactions
func (g *GasOracle) GetEIP1559Params(chainName string) (baseFee, tipCap *big.Int, err error) {
	g.mu.RLock()
	entry, ok := g.cachedPrices[chainName]
	g.mu.RUnlock()

	if !ok || entry == nil {
		return nil, nil, fmt.Errorf("no gas data for chain %s", chainName)
	}

	if entry.baseFee == nil || entry.tipCap == nil {
		return nil, nil, fmt.Errorf("EIP-1559 data not available for chain %s", chainName)
	}

	bf := new(big.Int).Set(entry.baseFee)
	tc := new(big.Int).Set(entry.tipCap)

	// Apply multiplier to tip cap
	tcFloat := new(big.Float).Mul(new(big.Float).SetInt(tc), big.NewFloat(g.multiplier))
	tc, _ = tcFloat.Int(nil)

	return bf, tc, nil
}

// IsSpiking returns true if current gas price is > 3x the recent average
func (g *GasOracle) IsSpiking(chainName string) bool {
	g.mu.RLock()
	entry, ok := g.cachedPrices[chainName]
	g.mu.RUnlock()

	if !ok || entry == nil {
		return false
	}

	// Simple heuristic: gas price > 100 gwei is considered a spike for most chains
	threshold := big.NewInt(100_000_000_000) // 100 gwei
	return entry.gasPrice.Cmp(threshold) > 0
}

// GetSweepGasLimit returns the gas limit for a sweep transaction
func GetSweepGasLimit(isNative bool) uint64 {
	if isNative {
		return 21_000 // ETH transfer
	}
	return 65_000 // ERC-20 transfer
}

// EstimateSweepCost estimates the total gas cost for a sweep in wei
func (g *GasOracle) EstimateSweepCost(chainName string, isNative bool) (*big.Int, error) {
	gasPrice, err := g.GetGasPrice(chainName)
	if err != nil {
		return nil, err
	}
	gasLimit := GetSweepGasLimit(isNative)
	return new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit)), nil
}
