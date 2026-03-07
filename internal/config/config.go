package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Redpanda RedpandaConfig
	Chains   []ChainConfig
	Signer   SignerConfig
}

// SignerConfig configures the key management backend
type SignerConfig struct {
	Type          string // "local", "vault", "mpc"
	EncryptionKey string // For local signer (AES-256 key, hex-encoded)
	VaultAddr     string // For vault signer
	VaultToken    string
	MPCURL        string // For MPC signer
	MPCKey        string
	MPCSecret     string
}

type ServerConfig struct {
	Port         int
	MetricsPort  int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type DatabaseConfig struct {
	URL             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type RedpandaConfig struct {
	Brokers []string
	GroupID string
}

type ChainConfig struct {
	Name           string `yaml:"name"`
	ChainID        int64  `yaml:"chain_id"`
	RPCURL         string `yaml:"rpc_url"`
	WSURL          string `yaml:"ws_url"`
	NativeSymbol   string `yaml:"native_symbol"`
	BlockTime      int    `yaml:"block_time_seconds"`
	Confirmations  int    `yaml:"confirmations"`
	StartBlock     int64  `yaml:"start_block"`
	Enabled        bool   `yaml:"enabled"`
	// ERC-20 tokens to track (address → symbol)
	TrackedTokens  map[string]string `yaml:"tracked_tokens"`
}

func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port:         envInt("PORT", 8080),
			MetricsPort:  envInt("METRICS_PORT", 9090),
			ReadTimeout:  time.Duration(envInt("READ_TIMEOUT_SECONDS", 30)) * time.Second,
			WriteTimeout: time.Duration(envInt("WRITE_TIMEOUT_SECONDS", 30)) * time.Second,
		},
		Database: DatabaseConfig{
			URL:             envStr("DATABASE_URL", "postgres://wallet_engine:wallet_engine@localhost:5432/wallet_engine?sslmode=disable"),
			MaxOpenConns:    envInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    envInt("DB_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: time.Duration(envInt("DB_CONN_MAX_LIFETIME_MINUTES", 30)) * time.Minute,
		},
		Redis: RedisConfig{
			Addr:     envStr("REDIS_ADDR", "localhost:6379"),
			Password: envStr("REDIS_PASSWORD", ""),
			DB:       envInt("REDIS_DB", 0),
		},
		Redpanda: RedpandaConfig{
			Brokers: strings.Split(envStr("REDPANDA_BROKERS", "redpanda.blockchain.svc:9092"), ","),
			GroupID: envStr("REDPANDA_GROUP_ID", "wallet-engine"),
		},
		Chains: defaultChains(),
		Signer: SignerConfig{
			Type:          envStr("SIGNER_TYPE", "local"),
			EncryptionKey: envStr("SIGNER_ENCRYPTION_KEY", ""),
			VaultAddr:     envStr("VAULT_ADDR", ""),
			VaultToken:    envStr("VAULT_TOKEN", ""),
			MPCURL:        envStr("MPC_URL", ""),
			MPCKey:        envStr("MPC_KEY", ""),
			MPCSecret:     envStr("MPC_SECRET", ""),
		},
	}
}

func defaultChains() []ChainConfig {
	return []ChainConfig{
		{
			Name: "ethereum", ChainID: 1,
			RPCURL: envStr("ETH_RPC_URL", "http://erigon-eth.blockchain.svc:8545"),
			NativeSymbol: "ETH", BlockTime: 12, Confirmations: 12, Enabled: true,
			TrackedTokens: map[string]string{
				"0xdAC17F958D2ee523a2206206994597C13D831ec7": "USDT",
				"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "USDC",
			},
		},
		{
			Name: "bsc", ChainID: 56,
			RPCURL: envStr("BSC_RPC_URL", "http://erigon-bsc.blockchain.svc:8545"),
			NativeSymbol: "BNB", BlockTime: 3, Confirmations: 15, Enabled: true,
			TrackedTokens: map[string]string{
				"0x55d398326f99059fF775485246999027B3197955": "USDT",
				"0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d": "USDC",
			},
		},
		{
			Name: "polygon", ChainID: 137,
			RPCURL: envStr("POLYGON_RPC_URL", "http://erigon-polygon.blockchain.svc:8545"),
			NativeSymbol: "MATIC", BlockTime: 2, Confirmations: 64, Enabled: true,
			TrackedTokens: map[string]string{
				"0xc2132D05D31c914a87C6611C10748AEb04B58e8F": "USDT",
				"0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359": "USDC",
			},
		},
		{
			Name: "arbitrum", ChainID: 42161,
			RPCURL: envStr("ARBITRUM_RPC_URL", "http://arbitrum.blockchain.svc:8547"),
			NativeSymbol: "ETH", BlockTime: 1, Confirmations: 10, Enabled: true,
			TrackedTokens: map[string]string{
				"0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": "USDT",
				"0xaf88d065e77c8cC2239327C5EDb3A432268e5831": "USDC",
			},
		},
		{
			Name: "optimism", ChainID: 10,
			RPCURL: envStr("OPTIMISM_RPC_URL", "http://optimism.blockchain.svc:8545"),
			NativeSymbol: "ETH", BlockTime: 2, Confirmations: 10, Enabled: true,
			TrackedTokens: map[string]string{
				"0x94b008aA00579c1307B0EF2c499aD98a8ce58e58": "USDT",
				"0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85": "USDC",
			},
		},
		{
			Name: "avalanche", ChainID: 43114,
			RPCURL: envStr("AVALANCHE_RPC_URL", "http://avalanche.blockchain.svc:9650/ext/bc/C/rpc"),
			NativeSymbol: "AVAX", BlockTime: 2, Confirmations: 12, Enabled: true,
			TrackedTokens: map[string]string{
				"0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7": "USDT",
				"0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E": "USDC",
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

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
