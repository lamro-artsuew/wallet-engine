package service

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/chain"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/hkdf"
)

var (
	sweepsExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_sweeps_executed_total",
		Help: "Total sweeps executed per chain and result",
	}, []string{"chain", "result"})

	sweepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_sweep_duration_seconds",
		Help:    "Sweep execution duration",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	}, []string{"chain"})

	sweepQueueSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wallet_engine_sweep_queue_size",
		Help: "Number of pending sweeps per chain",
	}, []string{"chain"})

	sweepGasCost = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_sweep_gas_cost_gwei",
		Help:    "Gas cost per sweep in gwei",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{"chain"})
)

// SweepWorker polls for confirmed deposits and executes sweep transactions
// to consolidate funds from deposit addresses into the hot wallet.
type SweepWorker struct {
	depositRepo *repository.DepositRepo
	sweepRepo   *repository.SweepRepo
	walletRepo  *repository.WalletRepo
	addrRepo    *repository.DepositAddressRepo
	nonceManager *NonceManager
	gasOracle    *GasOracle
	ledgerSvc    *LedgerService
	adapters     map[string]*chain.EVMAdapter

	masterSeed  []byte
	interval    time.Duration
	batchSize   int
	staleTxAge  time.Duration // age after which a broadcast sweep is considered stuck

	mu       sync.Mutex
	stopCh   chan struct{}
	stopped  bool
}

// NewSweepWorker creates a sweep worker
func NewSweepWorker(
	depositRepo *repository.DepositRepo,
	sweepRepo *repository.SweepRepo,
	walletRepo *repository.WalletRepo,
	addrRepo *repository.DepositAddressRepo,
	nonceManager *NonceManager,
	gasOracle *GasOracle,
	ledgerSvc *LedgerService,
	adapters map[string]*chain.EVMAdapter,
	masterSeed []byte,
	interval time.Duration,
	batchSize int,
) *SweepWorker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 10
	}
	return &SweepWorker{
		depositRepo:  depositRepo,
		sweepRepo:    sweepRepo,
		walletRepo:   walletRepo,
		addrRepo:     addrRepo,
		nonceManager: nonceManager,
		gasOracle:    gasOracle,
		ledgerSvc:    ledgerSvc,
		adapters:     adapters,
		masterSeed:   masterSeed,
		interval:     interval,
		batchSize:    batchSize,
		staleTxAge:   15 * time.Minute,
		stopCh:       make(chan struct{}),
	}
}

// Start begins the sweep worker loop
func (sw *SweepWorker) Start(ctx context.Context) {
	log.Info().Dur("interval", sw.interval).Int("batch_size", sw.batchSize).Msg("sweep worker started")
	ticker := time.NewTicker(sw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sw.runSweepCycle(ctx)
			sw.runConfirmationCheck(ctx)
		case <-sw.stopCh:
			log.Info().Msg("sweep worker stopped")
			return
		case <-ctx.Done():
			log.Info().Msg("sweep worker context cancelled")
			return
		}
	}
}

// Stop halts the sweep worker
func (sw *SweepWorker) Stop() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if !sw.stopped {
		close(sw.stopCh)
		sw.stopped = true
	}
}

// runSweepCycle processes confirmed deposits that need sweeping
func (sw *SweepWorker) runSweepCycle(ctx context.Context) {
	for chainName, adapter := range sw.adapters {
		if err := sw.sweepChain(ctx, chainName, adapter); err != nil {
			log.Error().Str("chain", chainName).Err(err).Msg("sweep cycle failed")
		}
	}
}

func (sw *SweepWorker) sweepChain(ctx context.Context, chainName string, adapter *chain.EVMAdapter) error {
	// Find confirmed deposits not yet swept
	deposits, err := sw.depositRepo.FindByChainAndState(ctx, chainName, domain.DepositConfirmed)
	if err != nil {
		return fmt.Errorf("find confirmed deposits: %w", err)
	}

	sweepQueueSize.WithLabelValues(chainName).Set(float64(len(deposits)))

	if len(deposits) == 0 {
		return nil
	}

	// Check gas spike — defer sweeps if gas is extremely high
	if sw.gasOracle.IsSpiking(chainName) {
		log.Warn().Str("chain", chainName).Int("pending", len(deposits)).Msg("gas price spiking, deferring sweeps")
		return nil
	}

	// Find hot wallet for this chain
	hotWallets, err := sw.walletRepo.FindByChainAndTier(ctx, chainName, domain.WalletTierHot)
	if err != nil || len(hotWallets) == 0 {
		return fmt.Errorf("no hot wallet for chain %s", chainName)
	}
	hotWallet := hotWallets[0]

	// Process in batches
	processed := 0
	for _, deposit := range deposits {
		if processed >= sw.batchSize {
			break
		}

		// Skip blacklisted deposits
		if deposit.IsFromBlacklisted {
			log.Warn().Str("deposit_id", deposit.ID.String()).Msg("skipping blacklisted deposit for sweep")
			continue
		}

		if err := sw.sweepDeposit(ctx, chainName, adapter, deposit, hotWallet); err != nil {
			log.Error().
				Str("chain", chainName).
				Str("deposit_id", deposit.ID.String()).
				Err(err).
				Msg("sweep failed for deposit")
			sweepsExecuted.WithLabelValues(chainName, "error").Inc()
			continue
		}

		sweepsExecuted.WithLabelValues(chainName, "success").Inc()
		processed++
	}

	if processed > 0 {
		log.Info().Str("chain", chainName).Int("swept", processed).Msg("sweep cycle completed")
	}

	return nil
}

func (sw *SweepWorker) sweepDeposit(ctx context.Context, chainName string, adapter *chain.EVMAdapter, deposit *domain.Deposit, hotWallet *domain.Wallet) error {
	start := time.Now()
	defer func() {
		sweepDuration.WithLabelValues(chainName).Observe(time.Since(start).Seconds())
	}()

	toAddr := common.HexToAddress(hotWallet.Address)
	isNative := deposit.TokenAddress == "" || deposit.TokenAddress == strings.ToLower(common.Address{}.Hex())

	// Get gas price
	gasPrice, err := sw.gasOracle.GetGasPrice(chainName)
	if err != nil {
		return fmt.Errorf("get gas price: %w", err)
	}

	// Determine sweep amount
	sweepAmount := new(big.Int).Set(deposit.Amount)

	if isNative {
		// For native sweeps, deduct gas cost from amount
		gasLimit := GetSweepGasLimit(true)
		gasCost := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit))
		sweepAmount.Sub(sweepAmount, gasCost)
		if sweepAmount.Sign() <= 0 {
			log.Warn().
				Str("deposit_id", deposit.ID.String()).
				Str("amount", deposit.Amount.String()).
				Str("gas_cost", gasCost.String()).
				Msg("deposit amount too small to cover gas, skipping")
			return nil
		}
	}

	// Reserve nonce atomically
	nonce, nonceReservationID, err := sw.nonceManager.ReserveNonce(ctx, chainName, deposit.ToAddress, "SWEEP", &deposit.ID)
	if err != nil {
		return fmt.Errorf("reserve nonce: %w", err)
	}

	// Derive private key for the deposit address
	privKey, err := sw.derivePrivateKey(chainName, deposit.ToAddress)
	if err != nil {
		sw.nonceManager.FailNonce(ctx, *nonceReservationID)
		return fmt.Errorf("derive private key: %w", err)
	}
	defer zeroKey(privKey) // explicit key zeroing

	// Build and sign the transaction
	var signedTx *types.Transaction
	chainID := big.NewInt(adapter.ChainID())
	gasLimit := GetSweepGasLimit(isNative)

	if isNative {
		// Native transfer
		tx := types.NewTx(&types.LegacyTx{
			Nonce:    uint64(nonce),
			To:       &toAddr,
			Value:    sweepAmount,
			Gas:      gasLimit,
			GasPrice: gasPrice,
		})
		signedTx, err = types.SignTx(tx, types.NewEIP155Signer(chainID), privKey)
	} else {
		// ERC-20 transfer
		tokenAddr := common.HexToAddress(deposit.TokenAddress)
		transferData := buildERC20TransferData(toAddr, sweepAmount)

		tx := types.NewTx(&types.LegacyTx{
			Nonce:    uint64(nonce),
			To:       &tokenAddr,
			Value:    big.NewInt(0),
			Gas:      gasLimit,
			GasPrice: gasPrice,
			Data:     transferData,
		})
		signedTx, err = types.SignTx(tx, types.NewEIP155Signer(chainID), privKey)
	}

	if err != nil {
		sw.nonceManager.FailNonce(ctx, *nonceReservationID)
		return fmt.Errorf("sign transaction: %w", err)
	}

	// Create sweep record BEFORE broadcast (pre-broadcast recording)
	gasCostEstimate := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit))
	sweepRecord := &domain.SweepRecord{
		ID:           uuid.New(),
		Chain:        chainName,
		FromAddress:  deposit.ToAddress,
		ToAddress:    hotWallet.Address,
		TokenAddress: deposit.TokenAddress,
		Amount:       sweepAmount,
		TxHash:       signedTx.Hash().Hex(),
		GasCost:      gasCostEstimate,
		State:        "PENDING",
		DepositID:    deposit.ID,
	}

	if err := sw.sweepRepo.Insert(ctx, sweepRecord); err != nil {
		sw.nonceManager.FailNonce(ctx, *nonceReservationID)
		return fmt.Errorf("insert sweep record: %w", err)
	}

	// Broadcast transaction
	if err := adapter.SendTransaction(ctx, signedTx); err != nil {
		sw.sweepRepo.UpdateState(ctx, sweepRecord.ID, "FAILED", "")
		sw.nonceManager.FailNonce(ctx, *nonceReservationID)
		return fmt.Errorf("broadcast sweep tx: %w", err)
	}

	// Update sweep and nonce states to BROADCAST
	sw.sweepRepo.UpdateState(ctx, sweepRecord.ID, "BROADCAST", signedTx.Hash().Hex())
	sw.nonceManager.ConfirmBroadcast(ctx, *nonceReservationID, signedTx.Hash().Hex())

	log.Info().
		Str("chain", chainName).
		Str("deposit_id", deposit.ID.String()).
		Str("sweep_id", sweepRecord.ID.String()).
		Str("tx_hash", signedTx.Hash().Hex()).
		Str("amount", sweepAmount.String()).
		Int64("nonce", nonce).
		Msg("sweep transaction broadcast")

	return nil
}

// runConfirmationCheck checks broadcast sweeps for on-chain confirmation
func (sw *SweepWorker) runConfirmationCheck(ctx context.Context) {
	for chainName, adapter := range sw.adapters {
		sweeps, err := sw.sweepRepo.FindByState(ctx, chainName, "BROADCAST")
		if err != nil {
			log.Error().Str("chain", chainName).Err(err).Msg("failed to find broadcast sweeps")
			continue
		}

		for _, sweep := range sweeps {
			if sweep.TxHash == "" {
				continue
			}

			receipt, err := adapter.TransactionReceipt(ctx, common.HexToHash(sweep.TxHash))
			if err != nil {
				// TX not yet mined — check if stale
				if time.Since(sweep.CreatedAt) > sw.staleTxAge {
					log.Warn().
						Str("chain", chainName).
						Str("sweep_id", sweep.ID.String()).
						Str("tx_hash", sweep.TxHash).
						Dur("age", time.Since(sweep.CreatedAt)).
						Msg("sweep TX potentially stuck — consider RBF")
				}
				continue
			}

			if receipt.Status == types.ReceiptStatusSuccessful {
				// Calculate actual gas cost
				actualGas := new(big.Int).Mul(
					new(big.Int).SetUint64(receipt.GasUsed),
					receipt.EffectiveGasPrice,
				)

				// Update sweep record
				sw.sweepRepo.UpdateState(ctx, sweep.ID, "CONFIRMED", sweep.TxHash)
				sw.sweepRepo.UpdateGasCost(ctx, sweep.ID, actualGas.String())

				// Update deposit state to SWEPT
				sw.depositRepo.UpdateState(ctx, sweep.DepositID, domain.DepositSwept, 0)

				// Post ledger entry
				sweep.GasCost = actualGas
				tokenSymbol := sw.getTokenSymbol(chainName, sweep.TokenAddress)
				if err := sw.ledgerSvc.PostSweepEntry(ctx, sweep, tokenSymbol); err != nil {
					log.Error().
						Str("sweep_id", sweep.ID.String()).
						Err(err).
						Msg("failed to post sweep ledger entry — RECONCILIATION NEEDED")
				}

				gasGwei := new(big.Float).Quo(new(big.Float).SetInt(actualGas), big.NewFloat(1e9))
				gweiF, _ := gasGwei.Float64()
				sweepGasCost.WithLabelValues(chainName).Observe(gweiF)

				log.Info().
					Str("chain", chainName).
					Str("sweep_id", sweep.ID.String()).
					Str("tx_hash", sweep.TxHash).
					Str("gas_cost", actualGas.String()).
					Msg("sweep confirmed on-chain")
			} else {
				// TX reverted
				sw.sweepRepo.UpdateState(ctx, sweep.ID, "FAILED", sweep.TxHash)
				log.Error().
					Str("chain", chainName).
					Str("sweep_id", sweep.ID.String()).
					Str("tx_hash", sweep.TxHash).
					Msg("sweep TX reverted on-chain")
			}
		}
	}
}

// derivePrivateKey derives the ECDSA private key for a deposit address using the
// same HKDF derivation as AddressService, given the deposit address.
// It finds the derivation index from the deposit_addresses table.
func (sw *SweepWorker) derivePrivateKey(chainName, address string) (*ecdsa.PrivateKey, error) {
	ctx := context.Background()
	// Look up the deposit address record to get the derivation index
	addrs, err := sw.addrRepo.FindByChain(ctx, chainName)
	if err != nil {
		return nil, fmt.Errorf("find deposit addresses: %w", err)
	}

	var derivationIdx int64 = -1
	for _, a := range addrs {
		if strings.EqualFold(a.Address, address) {
			derivationIdx = a.DerivationIdx
			break
		}
	}

	if derivationIdx < 0 {
		return nil, fmt.Errorf("deposit address %s not found for chain %s", address, chainName)
	}

	// Replicate HKDF derivation from AddressService
	salt := sha256.Sum256([]byte("wallet-engine-address-derivation-v1"))
	info := make([]byte, 0, len(chainName)+1+8)
	info = append(info, []byte(chainName)...)
	info = append(info, 0) // null separator
	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, uint64(derivationIdx))
	info = append(info, idxBytes...)

	reader := hkdf.New(sha256.New, sw.masterSeed, salt[:], info)
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("HKDF derivation: %w", err)
	}

	privKey, err := crypto.ToECDSA(keyMaterial)
	// Zero key material immediately
	for i := range keyMaterial {
		keyMaterial[i] = 0
	}
	if err != nil {
		return nil, fmt.Errorf("parse derived key: %w", err)
	}

	return privKey, nil
}

// getTokenSymbol returns the token symbol for a token address on a chain
func (sw *SweepWorker) getTokenSymbol(chainName, tokenAddr string) string {
	if tokenAddr == "" || tokenAddr == strings.ToLower(common.Address{}.Hex()) {
		if adapter, ok := sw.adapters[chainName]; ok {
			return adapter.NativeSymbol()
		}
		return "NATIVE"
	}
	if adapter, ok := sw.adapters[chainName]; ok {
		sym := adapter.GetTokenSymbolByAddr(tokenAddr)
		if sym != "UNKNOWN" {
			return sym
		}
	}
	return "UNKNOWN"
}

// buildERC20TransferData builds the calldata for ERC-20 transfer(address,uint256)
func buildERC20TransferData(to common.Address, amount *big.Int) []byte {
	// transfer(address,uint256) selector
	selector := crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]
	paddedTo := common.LeftPadBytes(to.Bytes(), 32)
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)

	data := make([]byte, 0, 68) // 4 + 32 + 32
	data = append(data, selector...)
	data = append(data, paddedTo...)
	data = append(data, paddedAmount...)
	return data
}

// zeroKey overwrites an ECDSA private key's D value with zeros.
// This is a best-effort defense — Go's GC may have copied the bytes.
func zeroKey(key *ecdsa.PrivateKey) {
	if key == nil || key.D == nil {
		return
	}
	key.D.SetInt64(0)
}
