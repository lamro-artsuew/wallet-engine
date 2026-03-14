package main

import (
	"context"
	"fmt"
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
	// In production: fetch from Vault at vault:secret/wallet-engine/master-seed
	masterSeed := []byte(envStr("MASTER_SEED", "DEVELOPMENT-SEED-DO-NOT-USE-IN-PRODUCTION"))
	addrSvc := service.NewAddressService(addrRepo, hdRepo, masterSeed)

	// Ledger and withdrawal services
	ledgerSvc := service.NewLedgerService(ledgerRepo)
	velocitySvc := service.NewVelocityService(velocityRepo, withdrawalRepo)
	withdrawalSvc := service.NewWithdrawalService(withdrawalRepo, walletRepo, ledgerSvc, producer, nil, velocitySvc)
	rebalanceSvc := service.NewRebalanceService(rebalanceRepo, walletRepo, ledgerSvc)

	// Signer (MPC/HSM/local key management)
	signer := service.NewSigner(cfg.Signer, walletRepo)

	// Deposit indexer (gated by feature flag)
	indexer := service.NewDepositIndexer(cfg.Chains, depositRepo, addrRepo, indexRepo, producer)
	if cfg.Features.IndexerEnabled {
		if err := indexer.Start(ctx); err != nil {
			log.Fatal().Err(err).Msg("failed to start deposit indexer")
		}
		defer indexer.Stop()
	} else {
		log.Warn().Msg("deposit indexer disabled by feature flag")
	}

	// Blacklist service (needs EVM adapters from indexer)
	blacklistSvc := service.NewBlacklistService(blacklistRepo, indexer.GetEVMAdapters())
	indexer.SetBlacklistService(blacklistSvc)
	withdrawalSvc.SetBlacklistService(blacklistSvc)

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
