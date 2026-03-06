package service

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/sha3"
)

// AddressService generates and manages deposit addresses
type AddressService struct {
	addrRepo   *repository.DepositAddressRepo
	hdRepo     *repository.HDDerivationRepo
	masterSeed []byte // in production this comes from Vault
}

// NewAddressService creates a new address service
func NewAddressService(
	addrRepo *repository.DepositAddressRepo,
	hdRepo *repository.HDDerivationRepo,
	masterSeed []byte,
) *AddressService {
	return &AddressService{
		addrRepo:   addrRepo,
		hdRepo:     hdRepo,
		masterSeed: masterSeed,
	}
}

// GenerateAddress creates a new deposit address for a user on a chain
func (s *AddressService) GenerateAddress(ctx context.Context, workspaceID, userID uuid.UUID, chain string) (*domain.DepositAddress, error) {
	// Get next derivation index atomically
	idx, err := s.hdRepo.GetAndIncrementIndex(ctx, chain)
	if err != nil {
		return nil, fmt.Errorf("get derivation index: %w", err)
	}

	// Derive address using deterministic key derivation
	// In production: BIP-44 with Vault-stored master key via HSM
	// For now: HMAC-based deterministic derivation
	address, err := s.deriveAddress(chain, idx)
	if err != nil {
		return nil, fmt.Errorf("derive address: %w", err)
	}

	derivationPath := fmt.Sprintf("m/44'/%s'/0'/0/%d", chainCoinType(chain), idx)

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

	if err := s.addrRepo.Create(ctx, da); err != nil {
		return nil, fmt.Errorf("save deposit address: %w", err)
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
func (s *AddressService) deriveAddress(chain string, index int64) (string, error) {
	// Deterministic key material: HMAC-SHA3(seed, chain || index)
	h := sha3.New256()
	h.Write(s.masterSeed)
	h.Write([]byte(fmt.Sprintf("%s:%d", chain, index)))
	keyMaterial := h.Sum(nil)

	privKey, err := crypto.ToECDSA(keyMaterial)
	if err != nil {
		return "", fmt.Errorf("derive private key: %w", err)
	}

	pubKey := privKey.Public().(*ecdsa.PublicKey)
	address := crypto.PubkeyToAddress(*pubKey)
	return address.Hex(), nil
}

// GetAddressForUser returns the deposit address for a user on a chain
func (s *AddressService) GetAddressForUser(ctx context.Context, userID uuid.UUID, chain string) (*domain.DepositAddress, error) {
	return s.addrRepo.FindByUserAndChain(ctx, userID, chain)
}

// ValidateAddress checks if an address is valid for the given chain
func ValidateAddress(chain, address string) bool {
	switch chain {
	case "ethereum", "bsc", "polygon", "arbitrum", "optimism", "avalanche":
		return common.IsHexAddress(address)
	default:
		return len(address) > 0
	}
}

func chainCoinType(chain string) string {
	switch chain {
	case "ethereum":
		return "60"
	case "bsc":
		return "9006"
	case "polygon":
		return "966"
	case "arbitrum":
		return "9001"
	case "optimism":
		return "614"
	case "avalanche":
		return "9005"
	case "tron":
		return "195"
	case "ton":
		return "607"
	default:
		return "60"
	}
}
