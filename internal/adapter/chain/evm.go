package chain

import (
	"context"
	"fmt"
	"math/big"
	"net/url"
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

// TrackedToken holds metadata for a tracked ERC-20 token
type TrackedToken struct {
	Symbol   string
	Decimals int
}

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
	Decimals     int
	IsNative     bool
}

// EVMAdapter connects to an EVM-compatible chain via JSON-RPC
type EVMAdapter struct {
	name          string
	chainID       int64
	rpcURL        string
	nativeSymbol  string
	client        *ethclient.Client
	trackedTokens map[common.Address]TrackedToken // address → token metadata
	mu            sync.RWMutex
	logger        zerolog.Logger
}

// Known token decimals for major stablecoins (USDT/USDC use 6 on most chains)
var knownDecimals = map[string]int{
	"USDT": 6,
	"USDC": 6,
	"DAI":  18,
	"BUSD": 18,
}

// NewEVMAdapter creates a new EVM chain adapter
func NewEVMAdapter(name string, chainID int64, rpcURL string, nativeSymbol string, trackedTokens map[string]string) *EVMAdapter {
	tokens := make(map[common.Address]TrackedToken, len(trackedTokens))
	for addr, symbol := range trackedTokens {
		decimals := 18 // default
		if d, ok := knownDecimals[symbol]; ok {
			decimals = d
		}
		tokens[common.HexToAddress(addr)] = TrackedToken{Symbol: symbol, Decimals: decimals}
	}
	return &EVMAdapter{
		name:          name,
		chainID:       chainID,
		rpcURL:        rpcURL,
		nativeSymbol:  nativeSymbol,
		trackedTokens: tokens,
		logger:        log.With().Str("chain", name).Logger(),
	}
}

// Connect establishes the RPC connection
func (a *EVMAdapter) Connect(ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	client, err := ethclient.DialContext(connectCtx, a.rpcURL)
	if err != nil {
		return fmt.Errorf("failed to connect to %s at %s: %w", a.name, a.rpcURL, err)
	}

	// Verify chain ID
	cid, err := client.ChainID(connectCtx)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to get chain ID from %s: %w", a.name, err)
	}
	if cid.Int64() != a.chainID {
		client.Close()
		return fmt.Errorf("chain ID mismatch for %s: expected %d, got %d", a.name, a.chainID, cid.Int64())
	}

	a.mu.Lock()
	if a.client != nil {
		a.client.Close()
	}
	a.client = client
	a.mu.Unlock()

	a.logger.Info().
		Str("rpc", a.SanitizeRPCURL()).
		Int64("chain_id", a.chainID).
		Msg("connected to chain")
	return nil
}

// getClient returns the current client, reconnecting if necessary
func (a *EVMAdapter) getClient(ctx context.Context) (*ethclient.Client, error) {
	a.mu.RLock()
	c := a.client
	a.mu.RUnlock()
	if c != nil {
		return c, nil
	}

	// Try to reconnect
	a.logger.Warn().Msg("client disconnected, attempting reconnect")
	if err := a.Connect(ctx); err != nil {
		return nil, fmt.Errorf("%s: reconnect failed: %w", a.name, err)
	}
	a.mu.RLock()
	c = a.client
	a.mu.RUnlock()
	return c, nil
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
	c, err := a.getClient(ctx)
	if err != nil {
		return 0, err
	}
	return c.BlockNumber(ctx)
}

// GetBlock returns a block by number (includes transactions)
func (a *EVMAdapter) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	c, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	return c.BlockByNumber(ctx, new(big.Int).SetUint64(number))
}

// GetBlockHash returns the hash of a block at the given number
func (a *EVMAdapter) GetBlockHash(ctx context.Context, number uint64) (common.Hash, error) {
	c, err := a.getClient(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	header, err := c.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		return common.Hash{}, err
	}
	return header.Hash(), nil
}

// ScanBlockForTransfers scans a block for both native and ERC-20 transfers to watched addresses
func (a *EVMAdapter) ScanBlockForTransfers(ctx context.Context, blockNum uint64, watchAddresses map[common.Address]bool) ([]TransferEvent, error) {
	c, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}

	var events []TransferEvent

	// --- 1. Scan native transfers via block transactions ---
	block, err := c.BlockByNumber(ctx, new(big.Int).SetUint64(blockNum))
	if err != nil {
		return nil, fmt.Errorf("get block %d: %w", blockNum, err)
	}

	signer := types.LatestSignerForChainID(big.NewInt(a.chainID))
	for _, tx := range block.Transactions() {
		to := tx.To()
		if to == nil {
			continue // contract creation
		}

		// Only care about transfers TO our watched addresses with nonzero value
		if !watchAddresses[*to] || tx.Value().Sign() == 0 {
			continue
		}

		from, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}

		events = append(events, TransferEvent{
			Chain:        a.name,
			TxHash:       tx.Hash(),
			LogIndex:     0, // native transfers have no log index
			BlockNumber:  blockNum,
			BlockHash:    block.Hash(),
			From:         from,
			To:           *to,
			TokenAddress: common.Address{}, // zero = native
			Amount:       tx.Value(),
			Decimals:     18, // all EVM native tokens use 18 decimals
			IsNative:     true,
		})
	}

	// --- 2. Scan ERC-20 Transfer logs ---
	// Filter by Transfer topic only (not by contract address) for RPC compatibility
	blockBig := new(big.Int).SetUint64(blockNum)
	query := ethereum.FilterQuery{
		FromBlock: blockBig,
		ToBlock:   blockBig,
		Topics:    [][]common.Hash{{erc20TransferTopic}},
	}

	logs, err := c.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("filter logs at block %d: %w", blockNum, err)
	}

	for _, vLog := range logs {
		if len(vLog.Topics) < 3 {
			continue
		}

		// Only process tracked tokens
		token, tracked := a.trackedTokens[vLog.Address]
		if !tracked {
			continue
		}

		to := common.BytesToAddress(vLog.Topics[2].Bytes())

		// Only care about transfers TO our watched addresses
		if !watchAddresses[to] {
			continue
		}

		// Validate ERC-20 amount data (must be 32 bytes for uint256)
		if len(vLog.Data) < 32 {
			a.logger.Warn().
				Str("tx", vLog.TxHash.Hex()).
				Int("dataLen", len(vLog.Data)).
				Msg("skipping ERC-20 log with invalid data length")
			continue
		}

		from := common.BytesToAddress(vLog.Topics[1].Bytes())
		amount := new(big.Int).SetBytes(vLog.Data[:32])

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
			Decimals:     token.Decimals,
			IsNative:     false,
		})
	}

	return events, nil
}

// GetBalance returns the native balance of an address
func (a *EVMAdapter) GetBalance(ctx context.Context, address common.Address) (*big.Int, error) {
	c, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	return c.BalanceAt(ctx, address, nil)
}

// GetTokenSymbol returns the symbol for a tracked token address
func (a *EVMAdapter) GetTokenSymbol(tokenAddr common.Address) string {
	if t, ok := a.trackedTokens[tokenAddr]; ok {
		return t.Symbol
	}
	return "UNKNOWN"
}

// GetTokenDecimals returns the decimals for a tracked token (18 for native)
func (a *EVMAdapter) GetTokenDecimals(tokenAddr common.Address) int {
	if tokenAddr == (common.Address{}) {
		return 18 // native
	}
	if t, ok := a.trackedTokens[tokenAddr]; ok {
		return t.Decimals
	}
	return 18
}

// NativeSymbol returns the native token symbol (ETH, BNB, etc.)
func (a *EVMAdapter) NativeSymbol() string { return a.nativeSymbol }

// IsTrackedToken checks if a token address is being tracked
func (a *EVMAdapter) IsTrackedToken(tokenAddr common.Address) bool {
	_, ok := a.trackedTokens[tokenAddr]
	return ok
}

// Health checks if the RPC connection is healthy
func (a *EVMAdapter) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	c, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	_, err = c.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("%s health check failed: %w", a.name, err)
	}
	return nil
}

// SanitizeRPCURL returns the RPC URL with credentials masked
func (a *EVMAdapter) SanitizeRPCURL() string {
	u, err := url.Parse(a.rpcURL)
	if err != nil || u.User == nil {
		if strings.Contains(a.rpcURL, "@") {
			parts := strings.SplitN(a.rpcURL, "@", 2)
			return "***@" + parts[1]
		}
		return a.rpcURL
	}
	u.User = url.User("***")
	return u.String()
}

// CheckAddressBlacklist calls a stablecoin contract to check if an address is blacklisted.
func (a *EVMAdapter) CheckAddressBlacklist(ctx context.Context, contractAddr common.Address, methodSig string, targetAddr common.Address) (bool, error) {
	c, err := a.getClient(ctx)
	if err != nil {
		return false, err
	}

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	selector := crypto.Keccak256([]byte(methodSig))[:4]
	paddedAddr := common.LeftPadBytes(targetAddr.Bytes(), 32)

	callData := make([]byte, 0, 36)
	callData = append(callData, selector...)
	callData = append(callData, paddedAddr...)

	msg := ethereum.CallMsg{
		To:   &contractAddr,
		Data: callData,
	}

	result, err := c.CallContract(callCtx, msg, nil)
	if err != nil {
		return false, fmt.Errorf("eth_call %s on %s: %w", methodSig, contractAddr.Hex(), err)
	}

	if len(result) < 32 {
		return false, fmt.Errorf("unexpected result length: %d", len(result))
	}

	return new(big.Int).SetBytes(result).Sign() != 0, nil
}
