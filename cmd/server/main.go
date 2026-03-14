package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/messaging"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/config"
	handler "github.com/lamro-artsuew/wallet-engine/internal/port/http"
	"github.com/lamro-artsuew/wallet-engine/internal/service"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if os.Getenv("ENV") != "production" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	cfg := config.Load()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}

	log.Info().
		Int("port", cfg.Server.Port).
		Int("metrics_port", cfg.Server.MetricsPort).
		Int("chains", len(cfg.Chains)).
		Bool("deposits", cfg.Features.DepositsEnabled).
		Bool("withdrawals", cfg.Features.WithdrawalsEnabled).
		Bool("indexer", cfg.Features.IndexerEnabled).
		Msg("starting wallet-engine")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Database connection pool
	poolCfg, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid database URL")
	}
	poolCfg.MaxConns = int32(cfg.Database.MaxOpenConns)
	poolCfg.MinConns = int32(cfg.Database.MaxIdleConns)
	poolCfg.MaxConnLifetime = cfg.Database.ConnMaxLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal().Err(err).Msg("database ping failed")
	}
	log.Info().Msg("database connected")

	// Run migrations
	if err := runMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("migrations failed")
	}

	// Repositories
	depositRepo := repository.NewDepositRepo(pool)
	addrRepo := repository.NewDepositAddressRepo(pool)
	walletRepo := repository.NewWalletRepo(pool)
	indexRepo := repository.NewChainIndexRepo(pool)
	hdRepo := repository.NewHDDerivationRepo(pool)
	withdrawalRepo := repository.NewWithdrawalRepo(pool)
	ledgerRepo := repository.NewLedgerRepo(pool)
	rebalanceRepo := repository.NewRebalanceRepo(pool)
	blacklistRepo := repository.NewBlacklistRepo(pool)
	velocityRepo := repository.NewVelocityRepo(pool)
	fiatRepo := repository.NewFiatRepo(pool)

	// Redpanda producer (non-fatal if unavailable)
	var producer *messaging.RedpandaProducer
	producer, err = messaging.NewRedpandaProducer(cfg.Redpanda.Brokers)
	if err != nil {
		log.Warn().Err(err).Msg("redpanda producer unavailable, events will not be published")
	}

	// Master seed for HD derivation
	// Production: set MASTER_SEED_SOURCE=vault and configure VAULT_ADDR + VAULT_TOKEN
	// The seed is fetched from Vault KV at secret/wallet-engine/master-seed
	masterSeed := loadMasterSeed(cfg.Signer)
	addrSvc := service.NewAddressService(pool, addrRepo, hdRepo, masterSeed)

	// Ledger and withdrawal services
	ledgerSvc := service.NewLedgerService(ledgerRepo)
	velocitySvc := service.NewVelocityService(velocityRepo, withdrawalRepo)

	// Deposit indexer (create early — need EVM adapters for blacklist service)
	indexer := service.NewDepositIndexer(cfg.Chains, depositRepo, addrRepo, indexRepo, producer)

	// Blacklist service (needs EVM adapters from indexer — created before Start)
	blacklistSvc := service.NewBlacklistService(blacklistRepo, indexer.GetEVMAdapters())
	indexer.SetBlacklistService(blacklistSvc)

	// Withdrawal service (blacklist + velocity injected via interfaces, never nil)
	withdrawalSvc := service.NewWithdrawalService(withdrawalRepo, walletRepo, ledgerSvc, producer, blacklistSvc, velocitySvc)
	rebalanceSvc := service.NewRebalanceService(rebalanceRepo, walletRepo, ledgerSvc)

	// Signer (MPC/HSM/local key management)
	signer := service.NewSigner(cfg.Signer, walletRepo)

	// Start deposit indexer (gated by feature flag)
	if cfg.Features.IndexerEnabled {
		if err := indexer.Start(ctx); err != nil {
			log.Fatal().Err(err).Msg("failed to start deposit indexer")
		}
		defer indexer.Stop()
	} else {
		log.Warn().Msg("deposit indexer disabled by feature flag")
	}

	// --- New services: Sweep, Nonce, Reorg, Reconciliation, Gas ---

	sweepRepo := repository.NewSweepRepo(pool)
	nonceRepo := repository.NewNonceRepo(pool)
	reorgRepo := repository.NewReorgRepo(pool)
	reconRepo := repository.NewReconciliationRepo(pool)

	// Nonce manager (requires EVM adapters)
	nonceManager := service.NewNonceManager(nonceRepo, indexer.GetEVMAdapters())

	// Gas oracle
	maxGasPrice := new(big.Int).Mul(
		big.NewInt(cfg.GasOracle.MaxGasPriceGwei),
		big.NewInt(1_000_000_000), // gwei → wei
	)
	maxPrices := make(map[string]*big.Int)
	for _, ch := range cfg.Chains {
		if ch.Enabled {
			maxPrices[ch.Name] = new(big.Int).Set(maxGasPrice)
		}
	}
	gasOracle := service.NewGasOracle(indexer.GetEVMAdapters(), cfg.GasOracle.Multiplier, maxPrices)
	gasOracle.Start(ctx, time.Duration(cfg.GasOracle.SampleInterval)*time.Second)
	defer gasOracle.Stop()

	// Sweep worker (gated by feature flag — OFF by default until validated)
	if cfg.Features.SweepEnabled {
		sweepWorker := service.NewSweepWorker(
			depositRepo, sweepRepo, walletRepo, addrRepo,
			nonceManager, gasOracle, ledgerSvc,
			indexer.GetEVMAdapters(), masterSeed,
			time.Duration(cfg.Sweep.Interval)*time.Second,
			cfg.Sweep.BatchSize,
		)
		go sweepWorker.Start(ctx)
		defer sweepWorker.Stop()
		log.Info().Msg("sweep worker enabled")
	} else {
		log.Warn().Msg("sweep worker disabled by feature flag (FEATURE_SWEEP_ENABLED=false)")
	}

	// Reorg detector (gated by feature flag)
	var reorgDetector *service.ReorgDetector
	if cfg.Features.ReorgDetectionEnabled {
		reorgDetector = service.NewReorgDetector(reorgRepo, depositRepo, indexRepo, indexer.GetEVMAdapters(), cfg.Chains)
		go reorgDetector.Start(ctx)
		defer reorgDetector.Stop()
		log.Info().Msg("reorg detector enabled")
	}

	// Reconciliation service (gated by feature flag)
	if cfg.Features.ReconciliationEnabled {
		driftThreshold := big.NewInt(cfg.Reconciliation.DriftThresholdWei)
		reconInterval := time.Duration(cfg.Reconciliation.Interval) * time.Second
		reconSvc := service.NewReconciliationService(
			reconRepo, walletRepo, depositRepo, ledgerRepo,
			indexer.GetEVMAdapters(), driftThreshold, reconInterval,
		)
		go reconSvc.Start(ctx)
		defer reconSvc.Stop()
		log.Info().Msg("reconciliation service enabled")
	}

	// Suppress unused variable warnings
	_ = signer
	_ = reorgDetector
	_ = sweepRepo

	// Fiat bridge service
	fiatBridgeSvc := service.NewFiatBridgeService(fiatRepo, ledgerSvc)

	// HTTP server
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsMiddleware())
	r.Use(requestLogger())

	h := handler.NewHandler(depositRepo, addrRepo, walletRepo, indexer, addrSvc, withdrawalSvc, ledgerSvc, rebalanceSvc, signer, blacklistSvc, velocitySvc, fiatBridgeSvc, fiatRepo)
	h.RegisterRoutes(r)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Metrics server (separate port)
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.MetricsPort),
		Handler: promhttp.Handler(),
	}

	go func() {
		log.Info().Int("port", cfg.Server.MetricsPort).Msg("metrics server starting")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("metrics server failed")
		}
	}()

	go func() {
		log.Info().Int("port", cfg.Server.Port).Msg("API server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("API server failed")
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	indexer.Stop()
	srv.Shutdown(shutdownCtx)
	metricsSrv.Shutdown(shutdownCtx)
	if producer != nil {
		producer.Close()
	}

	log.Info().Msg("wallet-engine stopped")
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrationFiles := []string{
		"migrations/001_foundation.sql",
		"migrations/002_velocity_limits.sql",
		"migrations/003_blacklist.sql",
		"migrations/004_rebalance.sql",
		"migrations/005_fiat_bridge.sql",
		"migrations/006_ledger_hardening.sql",
		"migrations/007_sweep_nonce_reorg.sql",
	}
	altPrefix := "/app/"

	for _, file := range migrationFiles {
		migrationSQL, err := os.ReadFile(file)
		if err != nil {
			migrationSQL, err = os.ReadFile(altPrefix + file)
			if err != nil {
				log.Warn().Str("file", file).Msg("migration file not found, skipping")
				continue
			}
		}

		_, err = pool.Exec(ctx, string(migrationSQL))
		if err != nil {
			return fmt.Errorf("execute migration %s: %w", file, err)
		}
		log.Info().Str("file", file).Msg("migration applied")
	}
	return nil
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("latency", time.Since(start)).
			Msg("request")
	}
}

func corsMiddleware() gin.HandlerFunc {
	allowedOrigins := envStr("CORS_ALLOWED_ORIGINS", "*")
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if allowedOrigins == "*" {
			c.Header("Access-Control-Allow-Origin", "*")
		} else {
			for _, o := range strings.Split(allowedOrigins, ",") {
				if strings.TrimSpace(o) == origin {
					c.Header("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-API-Key")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "3600")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// loadMasterSeed loads the HD derivation master seed.
// In production, set MASTER_SEED_SOURCE=vault to fetch from HashiCorp Vault KV.
// Otherwise falls back to MASTER_SEED env var (development only).
func loadMasterSeed(signerCfg config.SignerConfig) []byte {
	source := envStr("MASTER_SEED_SOURCE", "env")

	switch source {
	case "vault":
		if signerCfg.VaultAddr == "" || signerCfg.VaultToken == "" {
			log.Fatal().Msg("MASTER_SEED_SOURCE=vault requires VAULT_ADDR and VAULT_TOKEN")
		}
		seed, err := fetchSeedFromVault(signerCfg.VaultAddr, signerCfg.VaultToken)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to fetch master seed from Vault")
		}
		log.Info().Msg("master seed loaded from Vault KV")
		return seed

	default:
		seed := envStr("MASTER_SEED", "DEVELOPMENT-SEED-DO-NOT-USE-IN-PRODUCTION")
		if seed == "DEVELOPMENT-SEED-DO-NOT-USE-IN-PRODUCTION" {
			log.Warn().Msg("⚠️ USING DEVELOPMENT SEED — set MASTER_SEED_SOURCE=vault for production")
		}
		return []byte(seed)
	}
}

// fetchSeedFromVault reads the master seed from Vault KV v2 at secret/data/wallet-engine/master-seed
func fetchSeedFromVault(vaultAddr, vaultToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/v1/secret/data/wallet-engine/master-seed", strings.TrimRight(vaultAddr, "/"))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", vaultToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("vault returned HTTP %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode vault response: %w", err)
	}

	seed, ok := result.Data.Data["seed"]
	if !ok || seed == "" {
		return nil, fmt.Errorf("seed field not found in Vault KV at secret/wallet-engine/master-seed")
	}

	return []byte(seed), nil
}
