package http

import (
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lamro-artsuew/wallet-engine/internal/adapter/repository"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
	"github.com/lamro-artsuew/wallet-engine/internal/service"
)

// Handler contains all HTTP endpoint handlers
type Handler struct {
	depositRepo    *repository.DepositRepo
	addrRepo       *repository.DepositAddressRepo
	walletRepo     *repository.WalletRepo
	indexer        *service.DepositIndexer
	addrSvc        *service.AddressService
	withdrawalSvc  *service.WithdrawalService
	ledgerSvc      *service.LedgerService
	rebalanceSvc   *service.RebalanceService
	signer         service.Signer
	blacklistSvc   *service.BlacklistService
	velocitySvc    *service.VelocityService
}

// NewHandler creates a new handler
func NewHandler(
	depositRepo *repository.DepositRepo,
	addrRepo *repository.DepositAddressRepo,
	walletRepo *repository.WalletRepo,
	indexer *service.DepositIndexer,
	addrSvc *service.AddressService,
	withdrawalSvc *service.WithdrawalService,
	ledgerSvc *service.LedgerService,
	rebalanceSvc *service.RebalanceService,
	signer service.Signer,
	blacklistSvc *service.BlacklistService,
	velocitySvc *service.VelocityService,
) *Handler {
	return &Handler{
		depositRepo:    depositRepo,
		addrRepo:       addrRepo,
		walletRepo:     walletRepo,
		indexer:        indexer,
		addrSvc:        addrSvc,
		withdrawalSvc:  withdrawalSvc,
		ledgerSvc:      ledgerSvc,
		rebalanceSvc:   rebalanceSvc,
		signer:         signer,
		blacklistSvc:   blacklistSvc,
		velocitySvc:    velocitySvc,
	}
}

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/", h.Root)

	api := r.Group("/api/v1")
	{
		api.GET("/health", h.Health)

		// Deposit addresses
		api.POST("/addresses", h.CreateAddress)
		api.GET("/addresses", h.ListAddresses)

		// Deposits
		api.GET("/deposits", h.ListDeposits)
		api.GET("/deposits/:id", h.GetDeposit)

		// Withdrawals
		api.POST("/withdrawals", h.CreateWithdrawal)
		api.GET("/withdrawals", h.ListWithdrawals)
		api.GET("/withdrawals/:id", h.GetWithdrawal)

		// Wallets
		api.GET("/wallets", h.ListWallets)

		// Chain status
		api.GET("/chains", h.ChainHealth)

		// Ledger
		api.GET("/ledger/accounts", h.ListLedgerAccounts)
		api.GET("/ledger/entries", h.ListLedgerEntries)
		api.GET("/ledger/balances/:code", h.GetAccountBalance)
		api.GET("/ledger/integrity", h.VerifyLedgerIntegrity)

		// Rebalance
		api.GET("/rebalance/policies", h.ListRebalancePolicies)
		api.PUT("/rebalance/policies/:chain/:token", h.UpdateRebalancePolicy)
		api.POST("/rebalance/evaluate/:chain/:token", h.EvaluateRebalance)
		api.POST("/rebalance/approve/:id", h.ApproveRebalance)
		api.GET("/rebalance/operations", h.ListRebalanceOperations)
		api.GET("/rebalance/balances", h.GetTierBalances)

		// Signing
		api.POST("/sign/:withdrawal_id", h.SignWithdrawal)

		// Blacklist & compliance
		api.GET("/blacklist", h.ListBlacklist)
		api.POST("/blacklist", h.AddToBlacklist)
		api.DELETE("/blacklist/:id", h.RemoveFromBlacklist)
		api.GET("/blacklist/check/:chain/:address", h.CheckBlacklist)
		api.GET("/stablecoins", h.ListStablecoins)

		// Velocity limits
		api.GET("/velocity-limits", h.ListVelocityLimits)
		api.POST("/velocity-limits", h.CreateVelocityLimit)
		api.PUT("/velocity-limits/:id", h.UpdateVelocityLimit)
		api.DELETE("/velocity-limits/:id", h.DeleteVelocityLimit)
	}
}

// Root returns API information
func (h *Handler) Root(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service":     "wallet-engine",
		"version":     "1.0.0",
		"description": "Exchange-grade custody microservice — hot/warm/cold wallet tiering, deposit indexing, withdrawal orchestration",
		"endpoints": gin.H{
			"health":               "GET  /api/v1/health",
			"addresses":            "POST /api/v1/addresses, GET /api/v1/addresses",
			"deposits":             "GET  /api/v1/deposits, GET /api/v1/deposits/:id",
			"withdrawals":          "POST /api/v1/withdrawals, GET /api/v1/withdrawals, GET /api/v1/withdrawals/:id",
			"wallets":              "GET  /api/v1/wallets",
			"chains":               "GET  /api/v1/chains",
			"ledger":               "GET  /api/v1/ledger/accounts, GET /api/v1/ledger/entries, GET /api/v1/ledger/balances/:code, GET /api/v1/ledger/integrity",
			"rebalance_policies":   "GET  /api/v1/rebalance/policies, PUT /api/v1/rebalance/policies/:chain/:token",
			"rebalance_evaluate":   "POST /api/v1/rebalance/evaluate/:chain/:token",
			"rebalance_approve":    "POST /api/v1/rebalance/approve/:id",
			"rebalance_operations": "GET  /api/v1/rebalance/operations",
			"rebalance_balances":   "GET  /api/v1/rebalance/balances",
			"sign":                 "POST /api/v1/sign/:withdrawal_id",
			"blacklist":            "GET /api/v1/blacklist, POST /api/v1/blacklist, DELETE /api/v1/blacklist/:id, GET /api/v1/blacklist/check/:chain/:address",
			"stablecoins":          "GET /api/v1/stablecoins",
			"velocity_limits":      "GET /api/v1/velocity-limits, POST /api/v1/velocity-limits, PUT /api/v1/velocity-limits/:id, DELETE /api/v1/velocity-limits/:id",
		},
	})
}

// Health returns service health
func (h *Handler) Health(c *gin.Context) {
	chains := h.indexer.GetChainHealth(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "wallet-engine",
		"chains":  chains,
	})
}

// CreateAddressRequest is the request body for creating a deposit address
type CreateAddressRequest struct {
	WorkspaceID string `json:"workspace_id" binding:"required"`
	UserID      string `json:"user_id" binding:"required"`
	Chain       string `json:"chain" binding:"required"`
}

// CreateAddress generates a new deposit address for a user/chain
func (h *Handler) CreateAddress(c *gin.Context) {
	var req CreateAddressRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	workspaceID, err := uuid.Parse(req.WorkspaceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workspace_id"})
		return
	}

	userID, err := uuid.Parse(req.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
		return
	}

	// Check if user already has an address for this chain
	existing, err := h.addrRepo.FindByUserAndChain(c.Request.Context(), userID, req.Chain)
	if err == nil && existing != nil {
		c.JSON(http.StatusOK, existing)
		return
	}

	addr, err := h.addrSvc.GenerateAddress(c.Request.Context(), workspaceID, userID, req.Chain)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, addr)
}

// ListAddresses returns deposit addresses filtered by query params
func (h *Handler) ListAddresses(c *gin.Context) {
	chain := c.Query("chain")
	userIDStr := c.Query("user_id")

	if userIDStr != "" {
		userID, err := uuid.Parse(userIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
			return
		}
		if chain != "" {
			addr, err := h.addrRepo.FindByUserAndChain(c.Request.Context(), userID, chain)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "address not found"})
				return
			}
			c.JSON(http.StatusOK, []*domain.DepositAddress{addr})
			return
		}
	}

	if chain != "" {
		addrs, err := h.addrRepo.FindByChain(c.Request.Context(), chain)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, addrs)
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{"error": "provide chain or user_id query parameter"})
}

// ListDeposits returns deposits with pagination and filters
func (h *Handler) ListDeposits(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	userIDStr := c.Query("user_id")
	workspaceIDStr := c.Query("workspace_id")

	if limit > 100 {
		limit = 100
	}

	if userIDStr != "" {
		userID, err := uuid.Parse(userIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
			return
		}
		deposits, err := h.depositRepo.FindByUser(c.Request.Context(), userID, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, deposits)
		return
	}

	if workspaceIDStr != "" {
		wsID, err := uuid.Parse(workspaceIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workspace_id"})
			return
		}
		deposits, err := h.depositRepo.FindByWorkspace(c.Request.Context(), wsID, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, deposits)
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{"error": "provide user_id or workspace_id query parameter"})
}

// GetDeposit returns a specific deposit by ID
func (h *Handler) GetDeposit(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid deposit id"})
		return
	}

	deposit, err := h.depositRepo.FindByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "deposit not found"})
		return
	}

	c.JSON(http.StatusOK, deposit)
}

// CreateWithdrawalRequest is the request body for creating a withdrawal
type CreateWithdrawalRequest struct {
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	WorkspaceID    string `json:"workspace_id" binding:"required"`
	UserID         string `json:"user_id" binding:"required"`
	Chain          string `json:"chain" binding:"required"`
	ToAddress      string `json:"to_address" binding:"required"`
	TokenAddress   string `json:"token_address"`
	TokenSymbol    string `json:"token_symbol" binding:"required"`
	Amount         string `json:"amount" binding:"required"`
	Decimals       int    `json:"decimals"`
}

// CreateWithdrawal initiates a new withdrawal
func (h *Handler) CreateWithdrawal(c *gin.Context) {
	var req CreateWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	workspaceID, err := uuid.Parse(req.WorkspaceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workspace_id"})
		return
	}

	userID, err := uuid.Parse(req.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
		return
	}

	amount := new(big.Int)
	if _, ok := amount.SetString(req.Amount, 10); !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}

	if amount.Sign() <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be positive"})
		return
	}

	decimals := req.Decimals
	if decimals == 0 {
		decimals = 18
	}

	withdrawal, err := h.withdrawalSvc.CreateWithdrawal(c.Request.Context(), service.CreateWithdrawalRequest{
		IdempotencyKey: req.IdempotencyKey,
		WorkspaceID:    workspaceID,
		UserID:         userID,
		Chain:          req.Chain,
		ToAddress:      req.ToAddress,
		TokenAddress:   req.TokenAddress,
		TokenSymbol:    req.TokenSymbol,
		Amount:         amount,
		Decimals:       decimals,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, withdrawal)
}

// ListWithdrawals returns withdrawals with pagination and filters
func (h *Handler) ListWithdrawals(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	userIDStr := c.Query("user_id")
	workspaceIDStr := c.Query("workspace_id")

	if limit > 100 {
		limit = 100
	}

	if userIDStr != "" {
		userID, err := uuid.Parse(userIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
			return
		}
		withdrawals, err := h.withdrawalSvc.ListByUser(c.Request.Context(), userID, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, withdrawals)
		return
	}

	if workspaceIDStr != "" {
		wsID, err := uuid.Parse(workspaceIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workspace_id"})
			return
		}
		withdrawals, err := h.withdrawalSvc.ListByWorkspace(c.Request.Context(), wsID, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, withdrawals)
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{"error": "provide user_id or workspace_id query parameter"})
}

// GetWithdrawal returns a specific withdrawal by ID
func (h *Handler) GetWithdrawal(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid withdrawal id"})
		return
	}

	withdrawal, err := h.withdrawalSvc.GetWithdrawal(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "withdrawal not found"})
		return
	}

	c.JSON(http.StatusOK, withdrawal)
}

// ListWallets returns all managed wallets
func (h *Handler) ListWallets(c *gin.Context) {
	wallets, err := h.walletRepo.FindAll(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, wallets)
}

// ChainHealth returns per-chain indexer health
func (h *Handler) ChainHealth(c *gin.Context) {
	chains := h.indexer.GetChainHealth(c.Request.Context())
	c.JSON(http.StatusOK, chains)
}

// ListLedgerAccounts returns all ledger accounts
func (h *Handler) ListLedgerAccounts(c *gin.Context) {
	accounts, err := h.ledgerSvc.ListAccounts(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accounts)
}

// ListLedgerEntries returns recent ledger entries
func (h *Handler) ListLedgerEntries(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit > 100 {
		limit = 100
	}

	entries, err := h.ledgerSvc.ListEntries(c.Request.Context(), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, entries)
}

// GetAccountBalance returns the balance for a ledger account
func (h *Handler) GetAccountBalance(c *gin.Context) {
	code := c.Param("code")
	balance, err := h.ledgerSvc.GetAccountBalance(c.Request.Context(), code)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": code, "balance": balance.String()})
}

// VerifyLedgerIntegrity verifies the hash chain integrity
func (h *Handler) VerifyLedgerIntegrity(c *gin.Context) {
	count, err := h.ledgerSvc.VerifyIntegrity(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"intact":  false,
			"error":   err.Error(),
			"entries": count,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"intact":  true,
		"entries": count,
	})
}

// ListRebalancePolicies returns all rebalance policies
func (h *Handler) ListRebalancePolicies(c *gin.Context) {
	policies, err := h.rebalanceSvc.ListPolicies(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, policies)
}

// UpdateRebalancePolicyRequest is the request body for updating a rebalance policy
type UpdateRebalancePolicyRequest struct {
	HotTargetPct       *float64 `json:"hot_target_pct"`
	HotMaxPct          *float64 `json:"hot_max_pct"`
	HotMinPct          *float64 `json:"hot_min_pct"`
	WarmTargetPct      *float64 `json:"warm_target_pct"`
	WarmMaxPct         *float64 `json:"warm_max_pct"`
	MinRebalanceAmount *string  `json:"min_rebalance_amount"`
	AutoRebalance      *bool    `json:"auto_rebalance"`
	IsActive           *bool    `json:"is_active"`
}

// UpdateRebalancePolicy updates a rebalance policy for a chain/token
func (h *Handler) UpdateRebalancePolicy(c *gin.Context) {
	chain := c.Param("chain")
	token := c.Param("token")

	var req UpdateRebalancePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	policy := &domain.RebalancePolicy{
		Chain:       chain,
		TokenSymbol: token,
		IsActive:    true,
	}

	if req.HotTargetPct != nil {
		policy.HotTargetPct = *req.HotTargetPct
	}
	if req.HotMaxPct != nil {
		policy.HotMaxPct = *req.HotMaxPct
	}
	if req.HotMinPct != nil {
		policy.HotMinPct = *req.HotMinPct
	}
	if req.WarmTargetPct != nil {
		policy.WarmTargetPct = *req.WarmTargetPct
	}
	if req.WarmMaxPct != nil {
		policy.WarmMaxPct = *req.WarmMaxPct
	}
	if req.AutoRebalance != nil {
		policy.AutoRebalance = *req.AutoRebalance
	}
	if req.IsActive != nil {
		policy.IsActive = *req.IsActive
	}

	policy.MinRebalanceAmount = big.NewInt(0)
	if req.MinRebalanceAmount != nil {
		if _, ok := policy.MinRebalanceAmount.SetString(*req.MinRebalanceAmount, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid min_rebalance_amount"})
			return
		}
	}

	if err := h.rebalanceSvc.UpdatePolicy(c.Request.Context(), policy); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// EvaluateRebalance checks if rebalancing is needed for a chain/token
func (h *Handler) EvaluateRebalance(c *gin.Context) {
	chain := c.Param("chain")
	token := c.Param("token")

	op, err := h.rebalanceSvc.EvaluateRebalance(c.Request.Context(), chain, token)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if op == nil {
		c.JSON(http.StatusOK, gin.H{"status": "balanced", "message": "no rebalance needed"})
		return
	}

	c.JSON(http.StatusCreated, op)
}

// ApproveRebalance approves a pending rebalance operation
func (h *Handler) ApproveRebalance(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid operation id"})
		return
	}

	approvedBy := c.DefaultQuery("approved_by", "api")

	if err := h.rebalanceSvc.ApproveRebalance(c.Request.Context(), id, approvedBy); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "approved"})
}

// ListRebalanceOperations returns rebalance operations with filters
func (h *Handler) ListRebalanceOperations(c *gin.Context) {
	chain := c.Query("chain")
	state := c.Query("state")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit > 100 {
		limit = 100
	}

	ops, err := h.rebalanceSvc.ListOperations(c.Request.Context(), chain, state, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ops)
}

// GetTierBalances returns balance breakdown per tier for all chains
func (h *Handler) GetTierBalances(c *gin.Context) {
	balances, err := h.rebalanceSvc.GetTierBalances(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, balances)
}

// SignWithdrawalRequest is the request body for signing a withdrawal
type SignWithdrawalRequest struct {
	TxHash string `json:"tx_hash" binding:"required"`
}

// SignWithdrawal signs a withdrawal and transitions it to SIGNED state
func (h *Handler) SignWithdrawal(c *gin.Context) {
	withdrawalID, err := uuid.Parse(c.Param("withdrawal_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid withdrawal_id"})
		return
	}

	var req SignWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Fetch the withdrawal
	withdrawal, err := h.withdrawalSvc.GetWithdrawal(c.Request.Context(), withdrawalID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "withdrawal not found"})
		return
	}

	// Verify it's in SIGNING state
	if withdrawal.State != domain.WithdrawalSigning {
		c.JSON(http.StatusConflict, gin.H{
			"error": "withdrawal must be in SIGNING state",
			"state": string(withdrawal.State),
		})
		return
	}

	// Decode tx hash
	txHashBytes := []byte(req.TxHash)

	// Sign using the configured signer
	sig, err := h.signer.Sign(c.Request.Context(), withdrawal.SourceWalletID, withdrawal.Chain, txHashBytes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("signing failed: %v", err)})
		return
	}

	// Transition to SIGNED
	if err := h.withdrawalSvc.TransitionState(c.Request.Context(), withdrawalID, domain.WithdrawalSigned, nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("state transition failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"withdrawal_id": withdrawalID.String(),
		"signature":     fmt.Sprintf("%x", sig),
		"signer_type":   h.signer.Type(),
		"state":         "SIGNED",
	})
}

// AddBlacklistRequest is the request body for adding a blacklist entry
type AddBlacklistRequest struct {
	Chain   string `json:"chain" binding:"required"`
	Address string `json:"address" binding:"required"`
	Source  string `json:"source" binding:"required"`
	Reason  string `json:"reason"`
}

// ListBlacklist returns paginated blacklist entries
func (h *Handler) ListBlacklist(c *gin.Context) {
	chain := c.Query("chain")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit > 100 {
		limit = 100
	}

	entries, err := h.blacklistSvc.ListBlacklisted(c.Request.Context(), chain, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []*domain.BlacklistedAddress{}
	}
	c.JSON(http.StatusOK, entries)
}

// AddToBlacklist adds an address to the blacklist
func (h *Handler) AddToBlacklist(c *gin.Context) {
	var req AddBlacklistRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	validSources := map[string]bool{
		"OFAC": true, "USDT_BLACKLIST": true, "USDC_BLACKLIST": true,
		"MANUAL": true, "CHAINALYSIS": true, "RISK_ENGINE": true,
	}
	if !validSources[req.Source] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source, must be one of: OFAC, USDT_BLACKLIST, USDC_BLACKLIST, MANUAL, CHAINALYSIS, RISK_ENGINE"})
		return
	}

	entry := &domain.BlacklistedAddress{
		Chain:   req.Chain,
		Address: req.Address,
		Source:  req.Source,
	}
	if req.Reason != "" {
		entry.Reason = &req.Reason
	}

	if err := h.blacklistSvc.AddToBlacklist(c.Request.Context(), entry); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, entry)
}

// RemoveFromBlacklist soft-deletes a blacklist entry
func (h *Handler) RemoveFromBlacklist(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid blacklist id"})
		return
	}

	if err := h.blacklistSvc.RemoveFromBlacklist(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "removed"})
}

// CheckBlacklist checks if an address is blacklisted
func (h *Handler) CheckBlacklist(c *gin.Context) {
	chain := c.Param("chain")
	address := c.Param("address")

	result, err := h.blacklistSvc.CheckAddress(c.Request.Context(), chain, address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// ListStablecoins returns all monitored stablecoin contracts
func (h *Handler) ListStablecoins(c *gin.Context) {
	contracts, err := h.blacklistSvc.ListStablecoins(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if contracts == nil {
		contracts = []*domain.StablecoinContract{}
	}
	c.JSON(http.StatusOK, contracts)
}

// CreateVelocityLimitRequest is the request body for creating a velocity limit
type CreateVelocityLimitRequest struct {
	Scope               string   `json:"scope" binding:"required"`
	ScopeID             *string  `json:"scope_id"`
	Chain               *string  `json:"chain"`
	TokenSymbol         *string  `json:"token_symbol"`
	MaxAmountPerTx      *string  `json:"max_amount_per_tx"`
	MaxAmountPerHour    *string  `json:"max_amount_per_hour"`
	MaxAmountPerDay     *string  `json:"max_amount_per_day"`
	MaxCountPerHour     *int     `json:"max_count_per_hour"`
	MaxCountPerDay      *int     `json:"max_count_per_day"`
	CooldownSeconds     *int     `json:"cooldown_seconds"`
	GeoAllowedCountries []string `json:"geo_allowed_countries"`
	GeoBlockedCountries []string `json:"geo_blocked_countries"`
	IsActive            *bool    `json:"is_active"`
}

// ListVelocityLimits returns all velocity limits
func (h *Handler) ListVelocityLimits(c *gin.Context) {
	limits, err := h.velocitySvc.ListLimits(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if limits == nil {
		limits = []*domain.VelocityLimit{}
	}
	c.JSON(http.StatusOK, limits)
}

// CreateVelocityLimit creates a new velocity limit
func (h *Handler) CreateVelocityLimit(c *gin.Context) {
	var req CreateVelocityLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	validScopes := map[string]bool{"GLOBAL": true, "WORKSPACE": true, "USER": true, "CHAIN": true}
	if !validScopes[req.Scope] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "scope must be one of: GLOBAL, WORKSPACE, USER, CHAIN"})
		return
	}

	vl := &domain.VelocityLimit{
		Scope:               req.Scope,
		ScopeID:             req.ScopeID,
		Chain:               req.Chain,
		TokenSymbol:         req.TokenSymbol,
		MaxCountPerHour:     req.MaxCountPerHour,
		MaxCountPerDay:      req.MaxCountPerDay,
		GeoAllowedCountries: req.GeoAllowedCountries,
		GeoBlockedCountries: req.GeoBlockedCountries,
		IsActive:            true,
	}

	if req.CooldownSeconds != nil {
		vl.CooldownSeconds = *req.CooldownSeconds
	}
	if req.IsActive != nil {
		vl.IsActive = *req.IsActive
	}
	if req.MaxAmountPerTx != nil {
		vl.MaxAmountPerTx = new(big.Int)
		if _, ok := vl.MaxAmountPerTx.SetString(*req.MaxAmountPerTx, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_tx"})
			return
		}
	}
	if req.MaxAmountPerHour != nil {
		vl.MaxAmountPerHour = new(big.Int)
		if _, ok := vl.MaxAmountPerHour.SetString(*req.MaxAmountPerHour, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_hour"})
			return
		}
	}
	if req.MaxAmountPerDay != nil {
		vl.MaxAmountPerDay = new(big.Int)
		if _, ok := vl.MaxAmountPerDay.SetString(*req.MaxAmountPerDay, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_day"})
			return
		}
	}

	if err := h.velocitySvc.CreateLimit(c.Request.Context(), vl); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, vl)
}

// UpdateVelocityLimit updates an existing velocity limit
func (h *Handler) UpdateVelocityLimit(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid velocity limit id"})
		return
	}

	var req CreateVelocityLimitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	validScopes := map[string]bool{"GLOBAL": true, "WORKSPACE": true, "USER": true, "CHAIN": true}
	if !validScopes[req.Scope] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "scope must be one of: GLOBAL, WORKSPACE, USER, CHAIN"})
		return
	}

	vl := &domain.VelocityLimit{
		ID:                  id,
		Scope:               req.Scope,
		ScopeID:             req.ScopeID,
		Chain:               req.Chain,
		TokenSymbol:         req.TokenSymbol,
		MaxCountPerHour:     req.MaxCountPerHour,
		MaxCountPerDay:      req.MaxCountPerDay,
		GeoAllowedCountries: req.GeoAllowedCountries,
		GeoBlockedCountries: req.GeoBlockedCountries,
		IsActive:            true,
	}

	if req.CooldownSeconds != nil {
		vl.CooldownSeconds = *req.CooldownSeconds
	}
	if req.IsActive != nil {
		vl.IsActive = *req.IsActive
	}
	if req.MaxAmountPerTx != nil {
		vl.MaxAmountPerTx = new(big.Int)
		if _, ok := vl.MaxAmountPerTx.SetString(*req.MaxAmountPerTx, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_tx"})
			return
		}
	}
	if req.MaxAmountPerHour != nil {
		vl.MaxAmountPerHour = new(big.Int)
		if _, ok := vl.MaxAmountPerHour.SetString(*req.MaxAmountPerHour, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_hour"})
			return
		}
	}
	if req.MaxAmountPerDay != nil {
		vl.MaxAmountPerDay = new(big.Int)
		if _, ok := vl.MaxAmountPerDay.SetString(*req.MaxAmountPerDay, 10); !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_amount_per_day"})
			return
		}
	}

	if err := h.velocitySvc.UpdateLimit(c.Request.Context(), vl); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// DeleteVelocityLimit soft-deletes a velocity limit
func (h *Handler) DeleteVelocityLimit(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid velocity limit id"})
		return
	}

	if err := h.velocitySvc.DeleteLimit(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "removed"})
}
