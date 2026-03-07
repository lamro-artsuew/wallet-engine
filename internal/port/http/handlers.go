package http

import (
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
	depositRepo   *repository.DepositRepo
	addrRepo      *repository.DepositAddressRepo
	walletRepo    *repository.WalletRepo
	indexer       *service.DepositIndexer
	addrSvc       *service.AddressService
	withdrawalSvc *service.WithdrawalService
	ledgerSvc     *service.LedgerService
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
) *Handler {
	return &Handler{
		depositRepo:   depositRepo,
		addrRepo:      addrRepo,
		walletRepo:    walletRepo,
		indexer:       indexer,
		addrSvc:       addrSvc,
		withdrawalSvc: withdrawalSvc,
		ledgerSvc:     ledgerSvc,
	}
}

// RegisterRoutes registers all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
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
	}
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
