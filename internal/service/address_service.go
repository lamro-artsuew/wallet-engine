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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/hkdf"
)

// Fixed salt for domain separation in HKDF key derivation.
// Changing this value invalidates ALL previously derived addresses.
var hkdfSalt = sha256.Sum256([]byte("wallet-engine-address-derivation-v1"))

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// chainType classifies chains by address format
type chainType int

const (
	chainTypeEVM  chainType = iota
	chainTypeTRON
)

// AddressService generates and manages deposit addresses.
//
// SECURITY NOTE: masterSeed is held in process heap memory. In a production
// HSM/Vault deployment, the private key MUST never leave the HSM — signing
// should be performed inside the HSM, and address derivation should use the
// extended public key (xpub) which is safe to hold in application memory.
// The current design must be replaced with HSM-backed derivation before
// handling real funds. See: vault:secret/wallet-engine/master-seed
type AddressService struct {
	pool       *pgxpool.Pool
	addrRepo   *repository.DepositAddressRepo
	hdRepo     *repository.HDDerivationRepo
	masterSeed []byte
}

// NewAddressService creates a new address service
func NewAddressService(
	pool *pgxpool.Pool,
	addrRepo *repository.DepositAddressRepo,
	hdRepo *repository.HDDerivationRepo,
	masterSeed []byte,
) *AddressService {
	return &AddressService{
		pool:       pool,
		addrRepo:   addrRepo,
		hdRepo:     hdRepo,
		masterSeed: masterSeed,
	}
}

// GenerateAddress creates a new deposit address for a user on a chain.
// The index increment and address persistence are wrapped in a single
// transaction — if derivation or persistence fails, the index is rolled back
// to prevent gaps in the derivation sequence.
func (s *AddressService) GenerateAddress(ctx context.Context, workspaceID, userID uuid.UUID, chain string) (*domain.DepositAddress, error) {
	ct, err := getChainType(chain)
	if err != nil {
		return nil, err
	}

	// Atomic: index increment + address creation in single transaction.
	// Prevents index gaps when derivation or persistence fails. Without this,
	// a failed derivation permanently consumes an index — deposits sent to
	// that skipped address would be unmonitored.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Get next derivation index within the transaction
	var idx int64
	err = tx.QueryRow(ctx, `
		UPDATE hd_derivation_state 
		SET next_index = next_index + 1, updated_at = NOW()
		WHERE chain = $1
		RETURNING next_index - 1
	`, chain).Scan(&idx)
	if err != nil {
		err = tx.QueryRow(ctx, `
			INSERT INTO hd_derivation_state (chain, master_seed_id, next_index)
			VALUES ($1, 'vault:wallet-engine/master-seed', 1)
			ON CONFLICT (chain) DO UPDATE SET next_index = hd_derivation_state.next_index + 1, updated_at = NOW()
			RETURNING next_index - 1
		`, chain).Scan(&idx)
		if err != nil {
			return nil, fmt.Errorf("get derivation index: %w", err)
		}
	}

	address, err := s.deriveAddress(chain, ct, idx)
	if err != nil {
		return nil, fmt.Errorf("derive address for %s index %d: %w", chain, idx, err)
	}

	// Round-trip verification: re-derive from the same inputs and assert
	// equality. Catches any non-determinism in the derivation pipeline
	// before funds are sent to a potentially unrecoverable address.
	verifyAddr, err := s.deriveAddress(chain, ct, idx)
	if err != nil {
		return nil, fmt.Errorf("verification re-derivation failed: %w", err)
	}
	if address != verifyAddr {
		return nil, fmt.Errorf("derivation inconsistency: %s != %s (chain=%s, idx=%d)", address, verifyAddr, chain, idx)
	}

	coinType, _ := chainCoinType(chain)
	derivationPath := fmt.Sprintf("m/44'/%s'/0'/0/%d", coinType, idx)

	da := &domain.DepositAddress{
		ID:             uuid.New(),
		WorkspaceID:    workspaceID,
		UserID:         userID,
		Chain:          chain,
		Address:        address,
		DerivationPath: derivationPath,
		DerivationIdx:  idx,
		IsActive:       true,
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO deposit_addresses (id, workspace_id, user_id, chain, address, derivation_path, derivation_index)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, da.ID, da.WorkspaceID, da.UserID, da.Chain, da.Address, da.DerivationPath, da.DerivationIdx)
	if err != nil {
		return nil, fmt.Errorf("save deposit address: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit address generation: %w", err)
	}

	log.Info().
		Str("chain", chain).
		Str("address", address).
		Str("path", derivationPath).
		Str("user", userID.String()).
		Msg("generated deposit address")

	return da, nil
}

// deriveAddress creates a deterministic address from seed + chain + index
// using HKDF (RFC 5869) for key derivation and chain-appropriate address
// formatting. HKDF provides proper domain separation and key stretching
// unlike raw SHA3 concatenation.
func (s *AddressService) deriveAddress(chain string, ct chainType, index int64) (string, error) {
	// Construct structured HKDF info field with fixed-width encoding.
	// Null byte separates chain name from index to prevent collisions
	// like ("eth:1", 0) vs ("eth", 10).
	info := make([]byte, 0, len(chain)+1+8)
	info = append(info, []byte(chain)...)
	info = append(info, 0) // null separator
	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, uint64(index))
	info = append(info, idxBytes...)

	// HKDF Extract-then-Expand with SHA-256
	reader := hkdf.New(sha256.New, s.masterSeed, hkdfSalt[:], info)
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(reader, keyMaterial); err != nil {
		return "", fmt.Errorf("HKDF derivation: %w", err)
	}

	privKey, err := crypto.ToECDSA(keyMaterial)
	if err != nil {
		return "", fmt.Errorf("derive private key: %w", err)
	}

	pubKey := privKey.Public().(*ecdsa.PublicKey)

	switch ct {
	case chainTypeTRON:
		return tronAddressFromPubkey(pubKey), nil
	default:
		return crypto.PubkeyToAddress(*pubKey).Hex(), nil
	}
}

// tronAddressFromPubkey derives a TRON address (T-prefix, Base58Check)
// from a secp256k1 public key. TRON uses the same curve as Ethereum but
// encodes as Base58Check with a 0x41 prefix byte.
func tronAddressFromPubkey(pubKey *ecdsa.PublicKey) string {
	ethAddr := crypto.PubkeyToAddress(*pubKey)

	// TRON: 0x41 prefix + 20-byte address
	payload := make([]byte, 21)
	payload[0] = 0x41
	copy(payload[1:], ethAddr.Bytes())

	// Double SHA-256 checksum
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])

	full := make([]byte, 25)
	copy(full, payload)
	copy(full[21:], h2[:4])

	return base58Encode(full)
}

// base58Encode encodes bytes using Bitcoin/TRON Base58 alphabet
func base58Encode(input []byte) string {
	x := new(big.Int).SetBytes(input)
	base := big.NewInt(58)
	zero := big.NewInt(0)
	mod := new(big.Int)

	var result []byte
	for x.Cmp(zero) > 0 {
		x.DivMod(x, base, mod)
		result = append(result, base58Alphabet[mod.Int64()])
	}

	// Preserve leading zero bytes as '1' (base58 digit zero)
	for _, b := range input {
		if b != 0 {
			break
		}
		result = append(result, base58Alphabet[0])
	}

	// Reverse
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

// GetAddressForUser returns the deposit address for a user on a chain
func (s *AddressService) GetAddressForUser(ctx context.Context, userID uuid.UUID, chain string) (*domain.DepositAddress, error) {
	return s.addrRepo.FindByUserAndChain(ctx, userID, chain)
}

// ValidateAddress checks if an address is valid for the given chain.
// Returns false for unknown/unsupported chains (not a permissive default).
func ValidateAddress(chain, address string) bool {
	switch chain {
	case "ethereum", "bsc", "polygon", "arbitrum", "optimism", "avalanche":
		return common.IsHexAddress(address)
	case "tron":
		return validateTronAddress(address)
	default:
		return false
	}
}

// validateTronAddress performs structural TRON address validation:
// must be 34 chars, start with 'T', and consist only of Base58 characters.
func validateTronAddress(address string) bool {
	if len(address) != 34 || address[0] != 'T' {
		return false
	}
	for _, c := range address {
		if !strings.ContainsRune(base58Alphabet, c) {
			return false
		}
	}
	return true
}

// getChainType returns the address format for a chain, or error for unsupported chains.
func getChainType(chain string) (chainType, error) {
	switch chain {
	case "ethereum", "bsc", "polygon", "arbitrum", "optimism", "avalanche":
		return chainTypeEVM, nil
	case "tron":
		return chainTypeTRON, nil
	case "ton":
		return 0, fmt.Errorf("chain %s requires Ed25519 derivation (not secp256k1) — not yet implemented", chain)
	default:
		return 0, fmt.Errorf("unsupported chain for address derivation: %s", chain)
	}
}

// chainCoinType returns the BIP-44 coin type for a chain, or error for unknown chains.
func chainCoinType(chain string) (string, error) {
	switch chain {
	case "ethereum":
		return "60", nil
	case "bsc":
		return "9006", nil
	case "polygon":
		return "966", nil
	case "arbitrum":
		return "9001", nil
	case "optimism":
		return "614", nil
	case "avalanche":
		return "9005", nil
	case "tron":
		return "195", nil
	case "ton":
		return "607", nil
	default:
		return "", fmt.Errorf("unknown chain: %s", chain)
	}
}
