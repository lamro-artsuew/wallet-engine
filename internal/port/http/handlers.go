package http

import (
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
	depositRepo *repository.DepositRepo
	addrRepo    *repository.DepositAddressRepo
	walletRepo  *repository.WalletRepo
	indexer     *service.DepositIndexer
	addrSvc     *service.AddressService
}

// NewHandler creates a new handler
func NewHandler(
	depositRepo *repository.DepositRepo,
	addrRepo *repository.DepositAddressRepo,
	walletRepo *repository.WalletRepo,
	indexer *service.DepositIndexer,
	addrSvc *service.AddressService,
) *Handler {
	return &Handler{
		depositRepo: depositRepo,
		addrRepo:    addrRepo,
		walletRepo:  walletRepo,
		indexer:     indexer,
		addrSvc:     addrSvc,
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

		// Wallets
		api.GET("/wallets", h.ListWallets)

		// Chain status
		api.GET("/chains", h.ChainHealth)
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
	// For now, return 501 — full implementation in Phase 2
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not yet implemented"})
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
