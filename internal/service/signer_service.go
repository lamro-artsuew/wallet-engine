package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

var (
	signingOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_engine_signing_operations_total",
		Help: "Total signing operations by signer type and chain",
	}, []string{"signer_type", "chain", "result"})

	signingLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_engine_signing_duration_seconds",
		Help:    "Signing operation duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"signer_type", "chain"})
)

// Signer is the interface for all key management backends
type Signer interface {
	// Sign signs a raw transaction hash and returns the signature
	Sign(ctx context.Context, walletID uuid.UUID, chain string, txHash []byte) ([]byte, error)
	// GetPublicKey returns the public key for a wallet
	GetPublicKey(ctx context.Context, walletID uuid.UUID) ([]byte, error)
	// Type returns the signer type identifier
	Type() string
}

// LocalSigner uses encrypted keys stored in the database (development/small deployments)
type LocalSigner struct {
	walletRepo *repository.WalletRepo
	encKey     []byte
}

func (s *LocalSigner) Type() string { return "local" }

func (s *LocalSigner) Sign(ctx context.Context, walletID uuid.UUID, chain string, txHash []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		signingLatency.WithLabelValues(s.Type(), chain).Observe(time.Since(start).Seconds())
	}()

	wallet, err := s.walletRepo.FindByID(ctx, walletID)
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("wallet %s not found: %w", walletID, err)
	}
	if wallet.EncryptedKey == nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("wallet %s has no encrypted key", walletID)
	}

	// Decrypt the private key with AES-256-GCM
	privKeyBytes, err := decryptAESGCM(s.encKey, *wallet.EncryptedKey)
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("decrypt key: %w", err)
	}

	// Parse ECDSA private key from raw bytes
	privKey := new(ecdsa.PrivateKey)
	privKey.Curve = elliptic.P256()
	privKey.D = new(big.Int).SetBytes(privKeyBytes)
	privKey.PublicKey.X, privKey.PublicKey.Y = privKey.Curve.ScalarBaseMult(privKeyBytes)

	// Sign the transaction hash
	r, ss, err := ecdsa.Sign(strings.NewReader(hex.EncodeToString(txHash)), privKey, txHash)
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("ecdsa sign: %w", err)
	}

	// Encode as r || s (64 bytes for P256)
	sig := append(r.Bytes(), ss.Bytes()...)
	signingOpsTotal.WithLabelValues(s.Type(), chain, "success").Inc()

	log.Info().
		Str("wallet_id", walletID.String()).
		Str("chain", chain).
		Str("signer", s.Type()).
		Msg("transaction signed")

	return sig, nil
}

func (s *LocalSigner) GetPublicKey(ctx context.Context, walletID uuid.UUID) ([]byte, error) {
	wallet, err := s.walletRepo.FindByID(ctx, walletID)
	if err != nil {
		return nil, fmt.Errorf("wallet %s not found: %w", walletID, err)
	}
	if wallet.EncryptedKey == nil {
		return nil, fmt.Errorf("wallet %s has no encrypted key", walletID)
	}

	privKeyBytes, err := decryptAESGCM(s.encKey, *wallet.EncryptedKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt key: %w", err)
	}

	curve := elliptic.P256()
	x, y := curve.ScalarBaseMult(privKeyBytes)
	pubKey := elliptic.Marshal(curve, x, y)
	return pubKey, nil
}

// VaultSigner uses HashiCorp Vault Transit engine for signing
type VaultSigner struct {
	vaultAddr  string
	vaultToken string
	client     *http.Client
}

func (s *VaultSigner) Type() string { return "vault" }

func (s *VaultSigner) Sign(ctx context.Context, walletID uuid.UUID, chain string, txHash []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		signingLatency.WithLabelValues(s.Type(), chain).Observe(time.Since(start).Seconds())
	}()

	keyName := fmt.Sprintf("wallet-%s", walletID.String())
	url := fmt.Sprintf("%s/v1/transit/sign/%s", strings.TrimRight(s.vaultAddr, "/"), keyName)

	payload := map[string]interface{}{
		"input":     base64.StdEncoding.EncodeToString(txHash),
		"hash_algorithm": "sha2-256",
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("create vault request: %w", err)
	}
	req.Header.Set("X-Vault-Token", s.vaultToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("vault request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("vault returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data struct {
			Signature string `json:"signature"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("decode vault response: %w", err)
	}

	// Vault signature format: vault:v1:base64_signature
	parts := strings.SplitN(result.Data.Signature, ":", 3)
	if len(parts) != 3 {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("unexpected vault signature format: %s", result.Data.Signature)
	}

	sig, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
		return nil, fmt.Errorf("decode vault signature: %w", err)
	}

	signingOpsTotal.WithLabelValues(s.Type(), chain, "success").Inc()

	log.Info().
		Str("wallet_id", walletID.String()).
		Str("chain", chain).
		Str("signer", s.Type()).
		Msg("transaction signed via Vault")

	return sig, nil
}

func (s *VaultSigner) GetPublicKey(ctx context.Context, walletID uuid.UUID) ([]byte, error) {
	keyName := fmt.Sprintf("wallet-%s", walletID.String())
	url := fmt.Sprintf("%s/v1/transit/keys/%s", strings.TrimRight(s.vaultAddr, "/"), keyName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create vault request: %w", err)
	}
	req.Header.Set("X-Vault-Token", s.vaultToken)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vault returned %d: %s", resp.StatusCode, string(body))
	}

	// Parse the key from Vault response
	var result struct {
		Data struct {
			Keys map[string]struct {
				PublicKey string `json:"public_key"`
			} `json:"keys"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode vault response: %w", err)
	}

	// Get the latest key version
	for _, key := range result.Data.Keys {
		if key.PublicKey != "" {
			return base64.StdEncoding.DecodeString(key.PublicKey)
		}
	}

	return nil, fmt.Errorf("no public key found for wallet %s in Vault", walletID)
}

// MPCSigner is a stub for future MPC implementations (Fireblocks, Copper, etc.)
type MPCSigner struct {
	apiURL    string
	apiKey    string
	apiSecret string
}

func (s *MPCSigner) Type() string { return "mpc" }

func (s *MPCSigner) Sign(ctx context.Context, walletID uuid.UUID, chain string, txHash []byte) ([]byte, error) {
	signingOpsTotal.WithLabelValues(s.Type(), chain, "error").Inc()
	return nil, fmt.Errorf("MPC signing not yet configured — implement Fireblocks/Copper/BitGo integration")
}

func (s *MPCSigner) GetPublicKey(ctx context.Context, walletID uuid.UUID) ([]byte, error) {
	return nil, fmt.Errorf("MPC public key retrieval not yet configured")
}

// NewSigner creates the appropriate signer based on config
func NewSigner(cfg config.SignerConfig, walletRepo *repository.WalletRepo) Signer {
	switch cfg.Type {
	case "vault":
		log.Info().Str("vault_addr", cfg.VaultAddr).Msg("using Vault Transit signer")
		return &VaultSigner{
			vaultAddr:  cfg.VaultAddr,
			vaultToken: cfg.VaultToken,
			client:     &http.Client{Timeout: 10 * time.Second},
		}
	case "mpc":
		log.Info().Str("api_url", cfg.MPCURL).Msg("using MPC signer (stub)")
		return &MPCSigner{
			apiURL:    cfg.MPCURL,
			apiKey:    cfg.MPCKey,
			apiSecret: cfg.MPCSecret,
		}
	default:
		log.Info().Msg("using local AES-GCM signer")
		encKey, _ := hex.DecodeString(cfg.EncryptionKey)
		if len(encKey) != 32 {
			// Pad or hash to 32 bytes for AES-256
			padded := make([]byte, 32)
			copy(padded, encKey)
			encKey = padded
		}
		return &LocalSigner{
			walletRepo: walletRepo,
			encKey:     encKey,
		}
	}
}

// decryptAESGCM decrypts ciphertext using AES-256-GCM.
// Ciphertext format: nonce (12 bytes) || encrypted data || auth tag
func decryptAESGCM(key []byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}
