package chain

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ERC-20 Transfer event signature: Transfer(address,address,uint256)
var erc20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// TransferEvent represents a detected on-chain transfer
type TransferEvent struct {
	Chain        string
	TxHash       common.Hash
	LogIndex     uint
	BlockNumber  uint64
	BlockHash    common.Hash
	From         common.Address
	To           common.Address
	TokenAddress common.Address // zero address = native transfer
	Amount       *big.Int
	IsNative     bool
}

// EVMAdapter connects to an EVM-compatible chain via JSON-RPC
type EVMAdapter struct {
	name          string
	chainID       int64
	rpcURL        string
	client        *ethclient.Client
	trackedTokens map[common.Address]string // address → symbol
	mu            sync.RWMutex
	logger        zerolog.Logger
}

// NewEVMAdapter creates a new EVM chain adapter
func NewEVMAdapter(name string, chainID int64, rpcURL string, trackedTokens map[string]string) *EVMAdapter {
	tokens := make(map[common.Address]string, len(trackedTokens))
	for addr, symbol := range trackedTokens {
		tokens[common.HexToAddress(addr)] = symbol
	}
	return &EVMAdapter{
		name:          name,
		chainID:       chainID,
		rpcURL:        rpcURL,
		trackedTokens: tokens,
		logger:        log.With().Str("chain", name).Logger(),
	}
}

// Connect establishes the RPC connection
func (a *EVMAdapter) Connect(ctx context.Context) error {
	client, err := ethclient.DialContext(ctx, a.rpcURL)
	if err != nil {
		return fmt.Errorf("failed to connect to %s at %s: %w", a.name, a.rpcURL, err)
	}

	// Verify chain ID
	cid, err := client.ChainID(ctx)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to get chain ID from %s: %w", a.name, err)
	}
	if cid.Int64() != a.chainID {
		client.Close()
		return fmt.Errorf("chain ID mismatch for %s: expected %d, got %d", a.name, a.chainID, cid.Int64())
	}

	a.mu.Lock()
	a.client = client
	a.mu.Unlock()

	a.logger.Info().
		Str("rpc", a.rpcURL).
		Int64("chain_id", a.chainID).
		Msg("connected to chain")
	return nil
}

// Close disconnects from the chain
func (a *EVMAdapter) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		a.client.Close()
	}
}

// Name returns the chain name
func (a *EVMAdapter) Name() string { return a.name }

// ChainID returns the chain ID
func (a *EVMAdapter) ChainID() int64 { return a.chainID }

// LatestBlock returns the latest block number
func (a *EVMAdapter) LatestBlock(ctx context.Context) (uint64, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return 0, fmt.Errorf("%s: not connected", a.name)
	}
	return c.BlockNumber(ctx)
}

// GetBlock returns a block by number
func (a *EVMAdapter) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return nil, fmt.Errorf("%s: not connected", a.name)
	}
	return c.BlockByNumber(ctx, new(big.Int).SetUint64(number))
}

// GetBlockHash returns the hash of a block at the given number
func (a *EVMAdapter) GetBlockHash(ctx context.Context, number uint64) (common.Hash, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return common.Hash{}, fmt.Errorf("%s: not connected", a.name)
	}

	header, err := c.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		return common.Hash{}, err
	}
	return header.Hash(), nil
}

// ScanBlockForTransfers scans a block for ERC-20 transfers to any of the given addresses
func (a *EVMAdapter) ScanBlockForTransfers(ctx context.Context, blockNum uint64, watchAddresses map[common.Address]bool) ([]TransferEvent, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return nil, fmt.Errorf("%s: not connected", a.name)
	}

	// Build list of tracked token contract addresses
	tokenAddrs := make([]common.Address, 0, len(a.trackedTokens))
	for addr := range a.trackedTokens {
		tokenAddrs = append(tokenAddrs, addr)
	}

	if len(tokenAddrs) == 0 {
		return nil, nil
	}

	blockBig := new(big.Int).SetUint64(blockNum)
	query := ethereum.FilterQuery{
		FromBlock: blockBig,
		ToBlock:   blockBig,
		Addresses: tokenAddrs,
		Topics:    [][]common.Hash{{erc20TransferTopic}},
	}

	logs, err := c.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("filter logs at block %d: %w", blockNum, err)
	}

	var events []TransferEvent
	for _, vLog := range logs {
		if len(vLog.Topics) < 3 {
			continue
		}

		from := common.BytesToAddress(vLog.Topics[1].Bytes())
		to := common.BytesToAddress(vLog.Topics[2].Bytes())

		// Only care about transfers TO our managed addresses
		if !watchAddresses[to] {
			continue
		}

		amount := new(big.Int).SetBytes(vLog.Data)
		events = append(events, TransferEvent{
			Chain:        a.name,
			TxHash:       vLog.TxHash,
			LogIndex:     vLog.Index,
			BlockNumber:  vLog.BlockNumber,
			BlockHash:    vLog.BlockHash,
			From:         from,
			To:           to,
			TokenAddress: vLog.Address,
			Amount:       amount,
			IsNative:     false,
		})
	}

	return events, nil
}

// GetBalance returns the native balance of an address
func (a *EVMAdapter) GetBalance(ctx context.Context, address common.Address) (*big.Int, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return nil, fmt.Errorf("%s: not connected", a.name)
	}
	return c.BalanceAt(ctx, address, nil)
}

// GetTokenSymbol returns the symbol for a tracked token address
func (a *EVMAdapter) GetTokenSymbol(tokenAddr common.Address) string {
	if sym, ok := a.trackedTokens[tokenAddr]; ok {
		return sym
	}
	return "UNKNOWN"
}

// IsTrackedToken checks if a token address is being tracked
func (a *EVMAdapter) IsTrackedToken(tokenAddr common.Address) bool {
	_, ok := a.trackedTokens[tokenAddr]
	return ok
}

// Health checks if the RPC connection is healthy
func (a *EVMAdapter) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return fmt.Errorf("%s: not connected", a.name)
	}

	_, err := c.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("%s health check failed: %w", a.name, err)
	}
	return nil
}

// SanitizeRPCURL returns the RPC URL with credentials masked
func (a *EVMAdapter) SanitizeRPCURL() string {
	if strings.Contains(a.rpcURL, "@") {
		parts := strings.SplitN(a.rpcURL, "@", 2)
		return "***@" + parts[1]
	}
	return a.rpcURL
}

// CheckAddressBlacklist calls a stablecoin contract to check if an address is blacklisted.
// Uses eth_call (read-only) against the contract's blacklist checking method.
func (a *EVMAdapter) CheckAddressBlacklist(ctx context.Context, contractAddr common.Address, methodSig string, targetAddr common.Address) (bool, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c == nil {
		return false, fmt.Errorf("%s: not connected", a.name)
	}

	selector := crypto.Keccak256([]byte(methodSig))[:4]
	paddedAddr := common.LeftPadBytes(targetAddr.Bytes(), 32)

	callData := make([]byte, 0, 36)
	callData = append(callData, selector...)
	callData = append(callData, paddedAddr...)

	msg := ethereum.CallMsg{
		To:   &contractAddr,
		Data: callData,
	}

	result, err := c.CallContract(ctx, msg, nil)
	if err != nil {
		return false, fmt.Errorf("eth_call %s on %s: %w", methodSig, contractAddr.Hex(), err)
	}

	if len(result) < 32 {
		return false, fmt.Errorf("unexpected result length: %d", len(result))
	}

	return new(big.Int).SetBytes(result).Sign() != 0, nil
}
