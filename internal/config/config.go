package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	Redis    RedisConfig    `yaml:"redis"`
	Redpanda RedpandaConfig `yaml:"redpanda"`
	Chains   []ChainConfig  `yaml:"chains"`
	Signer   SignerConfig   `yaml:"signer"`
	Features FeatureFlags   `yaml:"features"`
}

// SignerConfig configures the key management backend
type SignerConfig struct {
	Type          string `yaml:"type"`           // "local", "vault", "mpc"
	EncryptionKey string `yaml:"encryption_key"` // For local signer (AES-256 key, hex-encoded)
	VaultAddr     string `yaml:"vault_addr"`     // For vault signer
	VaultToken    string `yaml:"vault_token"`
	VaultMount    string `yaml:"vault_mount"`    // Vault mount path (default: "secret")
	VaultKeyName  string `yaml:"vault_key_name"` // Vault key name
	MPCURL        string `yaml:"mpc_url"`        // For MPC signer
	MPCKey        string `yaml:"mpc_key"`
	MPCSecret     string `yaml:"mpc_secret"`
	MPCTimeout    int    `yaml:"mpc_timeout_seconds"` // MPC signing timeout
}

type ServerConfig struct {
	Port         int           `yaml:"port"`
	MetricsPort  int           `yaml:"metrics_port"`
	ReadTimeout  time.Duration `yaml:"-"`
	WriteTimeout time.Duration `yaml:"-"`
}

type DatabaseConfig struct {
	URL              string        `yaml:"url"`
	MaxOpenConns     int           `yaml:"max_open_conns"`
	MaxIdleConns     int           `yaml:"max_idle_conns"`
	ConnMaxLifetime  time.Duration `yaml:"-"`
	StatementTimeout int          `yaml:"statement_timeout_seconds"` // Per-query timeout (0 = unlimited)
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type RedpandaConfig struct {
	Brokers []string `yaml:"brokers"`
	GroupID string   `yaml:"group_id"`
}

// FeatureFlags allows runtime toggling of subsystems
type FeatureFlags struct {
	DepositsEnabled       bool `yaml:"deposits_enabled"`
	WithdrawalsEnabled    bool `yaml:"withdrawals_enabled"`
	IndexerEnabled        bool `yaml:"indexer_enabled"`
	ReconciliationEnabled bool `yaml:"reconciliation_enabled"`
}

// TokenConfig holds per-token metadata including decimals
type TokenConfig struct {
	Address  string `yaml:"address"`
	Symbol   string `yaml:"symbol"`
	Decimals int    `yaml:"decimals"`
}

type ChainConfig struct {
	Name           string        `yaml:"name"`
	ChainID        int64         `yaml:"chain_id"`
	RPCURL         string        `yaml:"rpc_url"`
	RPCURLs        []string      `yaml:"rpc_urls"`         // Failover RPC endpoints
	WSURL          string        `yaml:"ws_url"`
	NativeSymbol   string        `yaml:"native_symbol"`
	NativeDecimals int           `yaml:"native_decimals"`
	BlockTime      int           `yaml:"block_time_seconds"`
	Confirmations  int           `yaml:"confirmations"`
	ReorgDepth     int           `yaml:"reorg_depth"`       // Max expected reorg depth
	FinalityType   string        `yaml:"finality_type"`     // "probabilistic", "instant", "l2-batch"
	StartBlock     int64         `yaml:"start_block"`
	Enabled        bool          `yaml:"enabled"`
	RPCTimeout     int           `yaml:"rpc_timeout_seconds"`
	RPCRetries     int           `yaml:"rpc_retries"`
	TrackedTokens  []TokenConfig `yaml:"tracked_tokens"`
}

// TrackedTokensMap returns the legacy map[string]string for backward compat
func (c *ChainConfig) TrackedTokensMap() map[string]string {
	m := make(map[string]string, len(c.TrackedTokens))
	for _, t := range c.TrackedTokens {
		m[t.Address] = t.Symbol
	}
	return m
}

func Load() *Config {
	cfg := &Config{
		Server: ServerConfig{
			Port:         envInt("PORT", 8080),
			MetricsPort:  envInt("METRICS_PORT", 9090),
			ReadTimeout:  time.Duration(envInt("READ_TIMEOUT_SECONDS", 30)) * time.Second,
			WriteTimeout: time.Duration(envInt("WRITE_TIMEOUT_SECONDS", 30)) * time.Second,
		},
		Database: DatabaseConfig{
			URL:              envStrFile("DATABASE_URL", "postgres://wallet_engine:wallet_engine@localhost:5432/wallet_engine?sslmode=disable"),
			MaxOpenConns:     envInt("DB_MAX_OPEN_CONNS", 50),
			MaxIdleConns:     envInt("DB_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime:  time.Duration(envInt("DB_CONN_MAX_LIFETIME_MINUTES", 30)) * time.Minute,
			StatementTimeout: envInt("DB_STATEMENT_TIMEOUT_SECONDS", 30),
		},
		Redis: RedisConfig{
			Addr:     envStr("REDIS_ADDR", "localhost:6379"),
			Password: envStrFile("REDIS_PASSWORD", ""),
			DB:       envInt("REDIS_DB", 0),
		},
		Redpanda: RedpandaConfig{
			Brokers: strings.Split(envStr("REDPANDA_BROKERS", "redpanda.blockchain.svc:9092"), ","),
			GroupID: envStr("REDPANDA_GROUP_ID", "wallet-engine"),
		},
		Chains: defaultChains(),
		Signer: SignerConfig{
			Type:          envStr("SIGNER_TYPE", "local"),
			EncryptionKey: envStrFile("SIGNER_ENCRYPTION_KEY", ""),
			VaultAddr:     envStr("VAULT_ADDR", ""),
			VaultToken:    envStrFile("VAULT_TOKEN", ""),
			VaultMount:    envStr("VAULT_MOUNT", "secret"),
			VaultKeyName:  envStr("VAULT_KEY_NAME", ""),
			MPCURL:        envStr("MPC_URL", ""),
			MPCKey:        envStrFile("MPC_KEY", ""),
			MPCSecret:     envStrFile("MPC_SECRET", ""),
			MPCTimeout:    envInt("MPC_TIMEOUT_SECONDS", 30),
		},
		Features: FeatureFlags{
			DepositsEnabled:       envBool("FEATURE_DEPOSITS_ENABLED", true),
			WithdrawalsEnabled:    envBool("FEATURE_WITHDRAWALS_ENABLED", true),
			IndexerEnabled:        envBool("FEATURE_INDEXER_ENABLED", true),
			ReconciliationEnabled: envBool("FEATURE_RECONCILIATION_ENABLED", true),
		},
	}

	// Try to load chains from external YAML file (overrides defaults if present)
	if chainsFile := envStr("CHAINS_CONFIG_FILE", ""); chainsFile != "" {
		if chains, err := loadChainsFromFile(chainsFile); err == nil {
			cfg.Chains = chains
		}
	} else {
		// Try default locations
		for _, path := range []string{"config/chains.yaml", "/app/config/chains.yaml"} {
			if chains, err := loadChainsFromFile(path); err == nil {
				cfg.Chains = chains
				break
			}
		}
	}

	return cfg
}

// Validate checks the config for correctness
func (c *Config) Validate() error {
	var errs []string

	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid server port: %d", c.Server.Port))
	}
	if c.Database.URL == "" {
		errs = append(errs, "database URL is required")
	}
	if c.Database.MaxOpenConns < 1 {
		errs = append(errs, "db max_open_conns must be >= 1")
	}

	chainIDs := make(map[int64]string)
	chainNames := make(map[string]bool)
	for i, ch := range c.Chains {
		if ch.Name == "" {
			errs = append(errs, fmt.Sprintf("chain[%d]: name is required", i))
			continue
		}
		if chainNames[ch.Name] {
			errs = append(errs, fmt.Sprintf("chain[%d]: duplicate name %q", i, ch.Name))
		}
		chainNames[ch.Name] = true

		if ch.ChainID <= 0 {
			errs = append(errs, fmt.Sprintf("chain %s: chain_id must be > 0", ch.Name))
		}
		if prev, dup := chainIDs[ch.ChainID]; dup {
			errs = append(errs, fmt.Sprintf("chain %s: duplicate chain_id %d (also %s)", ch.Name, ch.ChainID, prev))
		}
		chainIDs[ch.ChainID] = ch.Name

		if ch.RPCURL == "" && len(ch.RPCURLs) == 0 {
			errs = append(errs, fmt.Sprintf("chain %s: rpc_url or rpc_urls required", ch.Name))
		}
		if ch.Confirmations <= 0 && ch.Enabled {
			errs = append(errs, fmt.Sprintf("chain %s: confirmations must be > 0", ch.Name))
		}
		if ch.NativeSymbol == "" {
			errs = append(errs, fmt.Sprintf("chain %s: native_symbol is required", ch.Name))
		}
		for j, tok := range ch.TrackedTokens {
			if tok.Address == "" {
				errs = append(errs, fmt.Sprintf("chain %s token[%d]: address is required", ch.Name, j))
			}
			if tok.Symbol == "" {
				errs = append(errs, fmt.Sprintf("chain %s token[%d]: symbol is required", ch.Name, j))
			}
			if tok.Decimals < 0 || tok.Decimals > 77 {
				errs = append(errs, fmt.Sprintf("chain %s token %s: invalid decimals %d", ch.Name, tok.Symbol, tok.Decimals))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// loadChainsFromFile loads chain configs from a YAML file
func loadChainsFromFile(path string) ([]ChainConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Chains []ChainConfig `yaml:"chains"`
	}
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("parse chains config %s: %w", path, err)
	}
	// Apply env var overrides for RPC URLs (preserves existing behavior)
	for i := range wrapper.Chains {
		ch := &wrapper.Chains[i]
		envKey := strings.ToUpper(strings.ReplaceAll(ch.Name, "-", "_")) + "_RPC_URL"
		if v := os.Getenv(envKey); v != "" {
			ch.RPCURL = v
		}
	}
	return wrapper.Chains, nil
}

func defaultChains() []ChainConfig {
	return []ChainConfig{
		{
			Name: "ethereum", ChainID: 1,
			RPCURL: envStr("ETH_RPC_URL", "http://erigon-eth.blockchain.svc:8545"),
			NativeSymbol: "ETH", NativeDecimals: 18,
			BlockTime: 12, Confirmations: 12,
			ReorgDepth: 64, FinalityType: "probabilistic",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0xdAC17F958D2ee523a2206206994597C13D831ec7", Symbol: "USDT", Decimals: 6},
				{Address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", Symbol: "USDC", Decimals: 6},
			},
		},
		{
			Name: "bsc", ChainID: 56,
			RPCURL: envStr("BSC_RPC_URL", "http://erigon-bsc.blockchain.svc:8545"),
			NativeSymbol: "BNB", NativeDecimals: 18,
			BlockTime: 3, Confirmations: 15,
			ReorgDepth: 32, FinalityType: "probabilistic",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0x55d398326f99059fF775485246999027B3197955", Symbol: "USDT", Decimals: 18},
				{Address: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d", Symbol: "USDC", Decimals: 18},
			},
		},
		{
			Name: "polygon", ChainID: 137,
			RPCURL: envStr("POLYGON_RPC_URL", "http://erigon-polygon.blockchain.svc:8545"),
			NativeSymbol: "MATIC", NativeDecimals: 18,
			BlockTime: 2, Confirmations: 64,
			ReorgDepth: 128, FinalityType: "probabilistic",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", Symbol: "USDT", Decimals: 6},
				{Address: "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", Symbol: "USDC", Decimals: 6},
			},
		},
		{
			Name: "arbitrum", ChainID: 42161,
			RPCURL: envStr("ARBITRUM_RPC_URL", "http://arbitrum.blockchain.svc:8547"),
			NativeSymbol: "ETH", NativeDecimals: 18,
			BlockTime: 1, Confirmations: 10,
			ReorgDepth: 0, FinalityType: "l2-batch",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", Symbol: "USDT", Decimals: 6},
				{Address: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", Symbol: "USDC", Decimals: 6},
			},
		},
		{
			Name: "optimism", ChainID: 10,
			RPCURL: envStr("OPTIMISM_RPC_URL", "http://optimism.blockchain.svc:8545"),
			NativeSymbol: "ETH", NativeDecimals: 18,
			BlockTime: 2, Confirmations: 10,
			ReorgDepth: 0, FinalityType: "l2-batch",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58", Symbol: "USDT", Decimals: 6},
				{Address: "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", Symbol: "USDC", Decimals: 6},
			},
		},
		{
			Name: "avalanche", ChainID: 43114,
			RPCURL: envStr("AVALANCHE_RPC_URL", "http://avalanche.blockchain.svc:9650/ext/bc/C/rpc"),
			NativeSymbol: "AVAX", NativeDecimals: 18,
			BlockTime: 2, Confirmations: 12,
			ReorgDepth: 0, FinalityType: "instant",
			RPCTimeout: 30, RPCRetries: 3, Enabled: true,
			TrackedTokens: []TokenConfig{
				{Address: "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7", Symbol: "USDT", Decimals: 6},
				{Address: "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E", Symbol: "USDC", Decimals: 6},
			},
		},
	}
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envStrFile checks ENV_KEY first, then ENV_KEY_FILE (reads secret from file).
// This supports K8s secrets mounted as files.
func envStrFile(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	if filePath := os.Getenv(key + "_FILE"); filePath != "" {
		if data, err := os.ReadFile(filePath); err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
