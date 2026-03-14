package chain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/lamro-artsuew/wallet-engine/internal/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Compile-time interface check
var _ ChainAdapter = (*TRONAdapter)(nil)

const (
	tronAddrPrefix  = byte(0x41)
	tronBase58Alpha = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
)

// TRC-20 Transfer event topic (same keccak as ERC-20: Transfer(address,address,uint256))
var trc20TransferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// TRONAdapter connects to the TRON blockchain via TronGrid REST API
type TRONAdapter struct {
	name           string
	rpcURL         string // TronGrid base URL (e.g., https://api.trongrid.io)
	apiKey         string // optional TronGrid API key
	nativeDecimals int
	trackedTokens  map[string]TrackedToken // hex address (lowercase, no prefix) → metadata
	httpClient     *http.Client
	logger         zerolog.Logger
}

// NewTRONAdapter creates a TRON chain adapter
func NewTRONAdapter(name string, rpcURL string, trackedTokens []config.TokenConfig, opts ...TRONAdapterOption) *TRONAdapter {
	tokens := make(map[string]TrackedToken, len(trackedTokens))
	for _, tok := range trackedTokens {
		decimals := tok.Decimals
		if decimals == 0 {
			if d, ok := knownDecimals[tok.Symbol]; ok {
				decimals = d
			} else {
				decimals = 6 // TRC-20 default
			}
		}
		// Store by hex address (lowercase, without 41 prefix) for log matching
		hexAddr := tronAddressToHexRaw(tok.Address)
		tokens[hexAddr] = TrackedToken{Symbol: tok.Symbol, Decimals: decimals}
	}

	a := &TRONAdapter{
		name:           name,
		rpcURL:         strings.TrimRight(rpcURL, "/"),
		nativeDecimals: 6, // TRX uses 6 decimals
		trackedTokens:  tokens,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: log.With().Str("chain", name).Logger(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// TRONAdapterOption configures optional TRON adapter parameters
type TRONAdapterOption func(*TRONAdapter)

func WithTRONAPIKey(key string) TRONAdapterOption {
	return func(a *TRONAdapter) { a.apiKey = key }
}

func (a *TRONAdapter) Connect(ctx context.Context) error {
	// Verify TronGrid connectivity
	_, err := a.LatestBlock(ctx)
	if err != nil {
		return fmt.Errorf("tron connect: %w", err)
	}
	a.logger.Info().Str("rpc", a.rpcURL).Msg("connected to TRON")
	return nil
}

func (a *TRONAdapter) Close() {
	a.httpClient.CloseIdleConnections()
}

func (a *TRONAdapter) Name() string { return a.name }

func (a *TRONAdapter) NativeSymbol() string { return "TRX" }

func (a *TRONAdapter) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := a.LatestBlock(ctx)
	if err != nil {
		return fmt.Errorf("%s health check failed: %w", a.name, err)
	}
	return nil
}

// LatestBlock returns the current block number
func (a *TRONAdapter) LatestBlock(ctx context.Context) (uint64, error) {
	var resp tronBlockResponse
	if err := a.postJSON(ctx, "/wallet/getnowblock", nil, &resp); err != nil {
		return 0, fmt.Errorf("getnowblock: %w", err)
	}
	return uint64(resp.BlockHeader.RawData.Number), nil
}

// GetBlockHashHex returns the block ID (hash) as hex string
func (a *TRONAdapter) GetBlockHashHex(ctx context.Context, blockNum uint64) (string, error) {
	var resp tronBlockResponse
	body := map[string]uint64{"num": blockNum}
	if err := a.postJSON(ctx, "/wallet/getblockbynum", body, &resp); err != nil {
		return "", fmt.Errorf("getblockbynum %d: %w", blockNum, err)
	}
	if resp.BlockID == "" {
		return "", fmt.Errorf("empty block ID for block %d", blockNum)
	}
	return resp.BlockID, nil
}

// ScanBlockForDeposits scans a TRON block for TRX and TRC-20 transfers to watched addresses
func (a *TRONAdapter) ScanBlockForDeposits(ctx context.Context, blockNum uint64, watchAddresses map[string]bool) ([]DepositEvent, error) {
	// Convert watch addresses to hex format for comparison
	hexWatch := make(map[string]bool, len(watchAddresses))
	addrMap := make(map[string]string, len(watchAddresses)) // hex → original
	for addr := range watchAddresses {
		hexAddr := strings.ToLower(tronAddressToHex(addr))
		hexWatch[hexAddr] = true
		addrMap[hexAddr] = addr
	}

	// 1. Get block with transactions
	var block tronBlockResponse
	body := map[string]uint64{"num": blockNum}
	if err := a.postJSON(ctx, "/wallet/getblockbynum", body, &block); err != nil {
		return nil, fmt.Errorf("getblockbynum %d: %w", blockNum, err)
	}

	var events []DepositEvent

	// 2. Scan native TRX transfers
	for txIdx, tx := range block.Transactions {
		if len(tx.Ret) > 0 && tx.Ret[0].ContractRet != "SUCCESS" {
			continue // failed transaction
		}
		for _, contract := range tx.RawData.Contract {
			if contract.Type != "TransferContract" {
				continue
			}
			toHex := strings.ToLower(contract.Parameter.Value.ToAddress)
			if !hexWatch[toHex] {
				continue
			}
			amount := new(big.Int).SetInt64(contract.Parameter.Value.Amount)
			if amount.Sign() <= 0 {
				continue
			}

			events = append(events, DepositEvent{
				TxHash:      tx.TxID,
				LogIndex:    uint(txIdx),
				BlockNumber: blockNum,
				BlockHash:   block.BlockID,
				From:        tronHexToBase58(contract.Parameter.Value.OwnerAddress),
				To:          addrMap[toHex],
				Amount:      amount,
				Decimals:    a.nativeDecimals,
				IsNative:    true,
			})
		}
	}

	// 3. Get transaction info for TRC-20 event logs
	var txInfos []tronTransactionInfo
	infoBody := map[string]uint64{"num": blockNum}
	if err := a.postJSON(ctx, "/wallet/gettransactioninfobyblocknum", infoBody, &txInfos); err != nil {
		// Non-fatal — we still detected native transfers
		a.logger.Warn().Err(err).Uint64("block", blockNum).
			Msg("failed to get transaction info for TRC-20 scanning")
		return events, nil
	}

	// 4. Scan TRC-20 Transfer logs
	for _, info := range txInfos {
		for logIdx, eventLog := range info.Log {
			if len(eventLog.Topics) < 3 {
				continue
			}
			// Check for Transfer event topic
			if eventLog.Topics[0] != trc20TransferTopic {
				continue
			}
			// Check if this is a tracked token
			contractHex := strings.ToLower(eventLog.Address)
			token, tracked := a.trackedTokens[contractHex]
			if !tracked {
				continue
			}
			// Parse 'to' address from topic[2] (last 20 bytes of 32-byte topic, with 41 prefix)
			toTopicHex := eventLog.Topics[2]
			if len(toTopicHex) < 40 {
				continue
			}
			toRaw := "41" + toTopicHex[len(toTopicHex)-40:]
			toHex := strings.ToLower(toRaw)
			if !hexWatch[toHex] {
				continue
			}
			// Parse amount from data
			if len(eventLog.Data) < 64 {
				continue
			}
			amount := new(big.Int)
			amount.SetString(eventLog.Data[:64], 16)
			if amount.Sign() <= 0 {
				continue
			}
			// Parse 'from' address from topic[1]
			fromTopicHex := eventLog.Topics[1]
			fromRaw := "41" + fromTopicHex[len(fromTopicHex)-40:]

			events = append(events, DepositEvent{
				TxHash:       info.ID,
				LogIndex:     uint(logIdx),
				BlockNumber:  blockNum,
				BlockHash:    block.BlockID,
				From:         tronHexToBase58(fromRaw),
				To:           addrMap[toHex],
				TokenAddress: tronHexToBase58("41" + contractHex),
				Amount:       amount,
				Decimals:     token.Decimals,
				IsNative:     false,
			})
		}
	}

	return events, nil
}

// GetTokenSymbolByAddr returns the symbol for a tracked token by T-prefix address
func (a *TRONAdapter) GetTokenSymbolByAddr(tokenAddr string) string {
	hexAddr := tronAddressToHexRaw(tokenAddr)
	if t, ok := a.trackedTokens[hexAddr]; ok {
		return t.Symbol
	}
	return "UNKNOWN"
}

// --- TronGrid HTTP helpers ---

func (a *TRONAdapter) postJSON(ctx context.Context, path string, body interface{}, result interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	} else {
		bodyReader = bytes.NewReader([]byte("{}"))
	}

	req, err := http.NewRequestWithContext(ctx, "POST", a.rpcURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if a.apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", a.apiKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("POST %s: status %d: %s", path, resp.StatusCode, string(respBody))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}

// --- TronGrid response types ---

type tronBlockResponse struct {
	BlockID     string            `json:"blockID"`
	BlockHeader tronBlockHeader   `json:"block_header"`
	Transactions []tronTransaction `json:"transactions"`
}

type tronBlockHeader struct {
	RawData struct {
		Number    int64 `json:"number"`
		Timestamp int64 `json:"timestamp"`
	} `json:"raw_data"`
}

type tronTransaction struct {
	TxID    string `json:"txID"`
	RawData struct {
		Contract []tronContract `json:"contract"`
	} `json:"raw_data"`
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
}

type tronContract struct {
	Type      string `json:"type"`
	Parameter struct {
		Value tronContractValue `json:"value"`
	} `json:"parameter"`
}

type tronContractValue struct {
	Amount       int64  `json:"amount"`
	OwnerAddress string `json:"owner_address"`
	ToAddress    string `json:"to_address"`
}

type tronTransactionInfo struct {
	ID  string         `json:"id"`
	Log []tronEventLog `json:"log"`
}

type tronEventLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

// --- TRON address conversion utilities ---

// tronAddressToHex converts a TRON address (T-prefix Base58 or hex) to lowercase hex (41...)
func tronAddressToHex(addr string) string {
	if strings.HasPrefix(addr, "41") && len(addr) == 42 {
		return strings.ToLower(addr) // already hex
	}
	if len(addr) != 34 || addr[0] != 'T' {
		return strings.ToLower(addr) // pass through
	}
	decoded := base58DecodeTron(addr)
	if len(decoded) < 21 {
		return strings.ToLower(addr)
	}
	return strings.ToLower(hex.EncodeToString(decoded[:21]))
}

// tronAddressToHexRaw converts a TRON address to hex without the 41 prefix (for log matching)
func tronAddressToHexRaw(addr string) string {
	full := tronAddressToHex(addr)
	if strings.HasPrefix(full, "41") && len(full) == 42 {
		return full[2:] // strip 41 prefix
	}
	return full
}

// tronHexToBase58 converts a TRON hex address (41...) to Base58Check (T-prefix)
func tronHexToBase58(hexAddr string) string {
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	addrBytes, err := hex.DecodeString(hexAddr)
	if err != nil || len(addrBytes) != 21 {
		return hexAddr // pass through on error
	}
	// Double SHA-256 checksum
	h1 := sha256.Sum256(addrBytes)
	h2 := sha256.Sum256(h1[:])
	full := make([]byte, 25)
	copy(full, addrBytes)
	copy(full[21:], h2[:4])
	return base58EncodeTron(full)
}

// base58EncodeTron encodes bytes to Base58 (Bitcoin/TRON alphabet)
func base58EncodeTron(data []byte) string {
	bigInt := new(big.Int).SetBytes(data)
	bigZero := big.NewInt(0)
	bigBase := big.NewInt(58)
	mod := new(big.Int)

	var result []byte
	for bigInt.Cmp(bigZero) > 0 {
		bigInt.DivMod(bigInt, bigBase, mod)
		result = append(result, tronBase58Alpha[mod.Int64()])
	}
	// Leading zeros
	for _, b := range data {
		if b != 0 {
			break
		}
		result = append(result, tronBase58Alpha[0])
	}
	// Reverse
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return string(result)
}

// base58DecodeTron decodes a Base58 string to bytes
func base58DecodeTron(s string) []byte {
	bigInt := big.NewInt(0)
	bigBase := big.NewInt(58)

	for _, c := range s {
		idx := strings.IndexRune(tronBase58Alpha, c)
		if idx < 0 {
			return nil
		}
		bigInt.Mul(bigInt, bigBase)
		bigInt.Add(bigInt, big.NewInt(int64(idx)))
	}

	// Count leading '1's (zero bytes)
	leadingZeros := 0
	for _, c := range s {
		if c != rune(tronBase58Alpha[0]) {
			break
		}
		leadingZeros++
	}

	decoded := bigInt.Bytes()
	result := make([]byte, leadingZeros+len(decoded))
	copy(result[leadingZeros:], decoded)
	return result
}
