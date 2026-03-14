package chain

import (
	"context"
	"math/big"
)

// ChainAdapter is the interface for blockchain adapters (EVM, TRON, etc.).
// All address and hash fields are strings — chain-specific encoding is handled
// by each adapter implementation.
type ChainAdapter interface {
	Connect(ctx context.Context) error
	Close()
	Name() string
	LatestBlock(ctx context.Context) (uint64, error)
	GetBlockHashHex(ctx context.Context, blockNum uint64) (string, error)
	ScanBlockForDeposits(ctx context.Context, blockNum uint64, watchAddresses map[string]bool) ([]DepositEvent, error)
	GetTokenSymbolByAddr(tokenAddr string) string
	NativeSymbol() string
	Health(ctx context.Context) error
}

// DepositEvent represents a detected incoming transfer, chain-agnostic.
// All addresses are in the chain's canonical string format (EVM hex, TRON T-prefix).
type DepositEvent struct {
	TxHash       string
	LogIndex     uint
	BlockNumber  uint64
	BlockHash    string
	From         string
	To           string
	TokenAddress string // empty string = native transfer
	Amount       *big.Int
	Decimals     int
	IsNative     bool
}
