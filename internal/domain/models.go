package domain

import (
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// WalletTier defines the custody tier of a wallet
type WalletTier string

const (
	WalletTierHot  WalletTier = "HOT"
	WalletTierWarm WalletTier = "WARM"
	WalletTierCold WalletTier = "COLD"
)

// Wallet represents a managed wallet (hot, warm, or cold)
type Wallet struct {
	ID           uuid.UUID  `json:"id" db:"id"`
	Chain        string     `json:"chain" db:"chain"`
	Address      string     `json:"address" db:"address"`
	Tier         WalletTier `json:"tier" db:"tier"`
	Label        string     `json:"label" db:"label"`
	EncryptedKey *[]byte    `json:"-" db:"encrypted_key"`
	IsActive     bool       `json:"is_active" db:"is_active"`
	CreatedAt    time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at" db:"updated_at"`
}

// DepositAddress represents a per-user per-chain deposit address
type DepositAddress struct {
	ID             uuid.UUID `json:"id" db:"id"`
	WorkspaceID    uuid.UUID `json:"workspace_id" db:"workspace_id"`
	UserID         uuid.UUID `json:"user_id" db:"user_id"`
	Chain          string    `json:"chain" db:"chain"`
	Address        string    `json:"address" db:"address"`
	DerivationPath string    `json:"-" db:"derivation_path"`
	DerivationIdx  int64     `json:"-" db:"derivation_index"`
	IsActive       bool      `json:"is_active" db:"is_active"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
}

// DepositState represents the lifecycle of a deposit
type DepositState string

const (
	DepositDetected   DepositState = "DETECTED"
	DepositPending    DepositState = "PENDING"
	DepositConfirming DepositState = "CONFIRMING"
	DepositConfirmed  DepositState = "CONFIRMED"
	DepositSwept      DepositState = "SWEPT"
	DepositFailed     DepositState = "FAILED"
	DepositReorged    DepositState = "REORGED"
)

// Deposit represents an inbound transfer detected on-chain
type Deposit struct {
	ID               uuid.UUID    `json:"id" db:"id"`
	IdempotencyKey   string       `json:"idempotency_key" db:"idempotency_key"`
	Chain            string       `json:"chain" db:"chain"`
	TxHash           string       `json:"tx_hash" db:"tx_hash"`
	LogIndex         int          `json:"log_index" db:"log_index"`
	BlockNumber      int64        `json:"block_number" db:"block_number"`
	BlockHash        string       `json:"block_hash" db:"block_hash"`
	FromAddress      string       `json:"from_address" db:"from_address"`
	ToAddress        string       `json:"to_address" db:"to_address"`
	TokenAddress     string       `json:"token_address" db:"token_address"`
	TokenSymbol      string       `json:"token_symbol" db:"token_symbol"`
	Amount           *big.Int     `json:"amount" db:"amount"`
	Decimals         int          `json:"decimals" db:"decimals"`
	State            DepositState `json:"state" db:"state"`
	Confirmations    int          `json:"confirmations" db:"confirmations"`
	RequiredConfs    int          `json:"required_confirmations" db:"required_confirmations"`
	DepositAddressID uuid.UUID    `json:"deposit_address_id" db:"deposit_address_id"`
	WorkspaceID      uuid.UUID    `json:"workspace_id" db:"workspace_id"`
	UserID           uuid.UUID    `json:"user_id" db:"user_id"`
	IsFromBlacklisted bool         `json:"is_from_blacklisted" db:"is_from_blacklisted"`
	SweepTxHash      *string      `json:"sweep_tx_hash,omitempty" db:"sweep_tx_hash"`
	LedgerEntryID    *uuid.UUID   `json:"ledger_entry_id,omitempty" db:"ledger_entry_id"`
	DetectedAt       time.Time    `json:"detected_at" db:"detected_at"`
	ConfirmedAt      *time.Time   `json:"confirmed_at,omitempty" db:"confirmed_at"`
	SweptAt          *time.Time   `json:"swept_at,omitempty" db:"swept_at"`
	CreatedAt        time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at" db:"updated_at"`
}

// WithdrawalState represents the lifecycle of a withdrawal
type WithdrawalState string

const (
	WithdrawalInitiated         WithdrawalState = "INITIATED"
	WithdrawalRiskCheck         WithdrawalState = "RISK_CHECK"
	WithdrawalRiskApproved      WithdrawalState = "RISK_APPROVED"
	WithdrawalRiskRejected      WithdrawalState = "RISK_REJECTED"
	WithdrawalSigning           WithdrawalState = "SIGNING"
	WithdrawalSigned            WithdrawalState = "SIGNED"
	WithdrawalBroadcast         WithdrawalState = "BROADCAST"
	WithdrawalPendingConfirm    WithdrawalState = "PENDING_CONFIRMATION"
	WithdrawalConfirmed         WithdrawalState = "CONFIRMED"
	WithdrawalFailed            WithdrawalState = "FAILED"
	WithdrawalExpired           WithdrawalState = "EXPIRED"
	WithdrawalManualReview      WithdrawalState = "MANUAL_REVIEW"
)

// Withdrawal represents an outbound transfer from the platform
type Withdrawal struct {
	ID              uuid.UUID       `json:"id" db:"id"`
	IdempotencyKey  string          `json:"idempotency_key" db:"idempotency_key"`
	WorkspaceID     uuid.UUID       `json:"workspace_id" db:"workspace_id"`
	UserID          uuid.UUID       `json:"user_id" db:"user_id"`
	Chain           string          `json:"chain" db:"chain"`
	ToAddress       string          `json:"to_address" db:"to_address"`
	TokenAddress    string          `json:"token_address" db:"token_address"`
	TokenSymbol     string          `json:"token_symbol" db:"token_symbol"`
	Amount          *big.Int        `json:"amount" db:"amount"`
	Decimals        int             `json:"decimals" db:"decimals"`
	State           WithdrawalState `json:"state" db:"state"`
	TxHash          *string         `json:"tx_hash,omitempty" db:"tx_hash"`
	Nonce           *int64          `json:"nonce,omitempty" db:"nonce"`
	GasPrice        *big.Int        `json:"gas_price,omitempty" db:"gas_price"`
	GasUsed         *int64          `json:"gas_used,omitempty" db:"gas_used"`
	Fee             *big.Int        `json:"fee,omitempty" db:"fee"`
	RiskScore       *int            `json:"risk_score,omitempty" db:"risk_score"`
	RiskLevel       *string         `json:"risk_level,omitempty" db:"risk_level"`
	SourceWalletID  uuid.UUID       `json:"source_wallet_id" db:"source_wallet_id"`
	LedgerEntryID   *uuid.UUID      `json:"ledger_entry_id,omitempty" db:"ledger_entry_id"`
	ErrorMessage    *string         `json:"error_message,omitempty" db:"error_message"`
	Confirmations   int             `json:"confirmations" db:"confirmations"`
	RequiredConfs   int             `json:"required_confirmations" db:"required_confirmations"`
	CreatedAt       time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at" db:"updated_at"`
	BroadcastAt     *time.Time      `json:"broadcast_at,omitempty" db:"broadcast_at"`
	ConfirmedAt     *time.Time      `json:"confirmed_at,omitempty" db:"confirmed_at"`
}

// LedgerAccountType defines account types in the double-entry ledger
type LedgerAccountType string

const (
	AccountAsset     LedgerAccountType = "ASSET"
	AccountLiability LedgerAccountType = "LIABILITY"
	AccountRevenue   LedgerAccountType = "REVENUE"
	AccountExpense   LedgerAccountType = "EXPENSE"
)

// LedgerAccount represents an account in the internal double-entry ledger
type LedgerAccount struct {
	ID        uuid.UUID         `json:"id" db:"id"`
	Code      string            `json:"code" db:"code"`
	Name      string            `json:"name" db:"name"`
	Type      LedgerAccountType `json:"type" db:"type"`
	Chain     *string           `json:"chain,omitempty" db:"chain"`
	Currency  string            `json:"currency" db:"currency"`
	ParentID  *uuid.UUID        `json:"parent_id,omitempty" db:"parent_id"`
	IsActive  bool              `json:"is_active" db:"is_active"`
	CreatedAt time.Time         `json:"created_at" db:"created_at"`
}

// LedgerEntryState represents the posting state
type LedgerEntryState string

const (
	EntryPending  LedgerEntryState = "PENDING"
	EntryPosted   LedgerEntryState = "POSTED"
	EntryReversed LedgerEntryState = "REVERSED"
)

// LedgerEntry represents a journal entry (group of lines that must balance)
type LedgerEntry struct {
	ID              uuid.UUID        `json:"id" db:"id"`
	IdempotencyKey  string           `json:"idempotency_key" db:"idempotency_key"`
	EntryType       string           `json:"entry_type" db:"entry_type"`
	Description     string           `json:"description" db:"description"`
	State           LedgerEntryState `json:"state" db:"state"`
	ReferenceType   *string          `json:"reference_type,omitempty" db:"reference_type"`
	ReferenceID     *uuid.UUID       `json:"reference_id,omitempty" db:"reference_id"`
	PreviousHash    string           `json:"previous_hash" db:"previous_hash"`
	EntryHash       string           `json:"entry_hash" db:"entry_hash"`
	CreatedAt       time.Time        `json:"created_at" db:"created_at"`
	Lines           []LedgerLine     `json:"lines,omitempty"`
}

// LedgerLine represents a single debit or credit in a journal entry
type LedgerLine struct {
	ID        uuid.UUID `json:"id" db:"id"`
	EntryID   uuid.UUID `json:"entry_id" db:"entry_id"`
	AccountID uuid.UUID `json:"account_id" db:"account_id"`
	Amount    *big.Int  `json:"amount" db:"amount"`
	Currency  string    `json:"currency" db:"currency"`
	IsDebit   bool      `json:"is_debit" db:"is_debit"`
	Sequence  int       `json:"sequence" db:"sequence"`
}

// IntegrityResult is the structured output of a ledger hash chain verification
type IntegrityResult struct {
	TotalChecked   int        `json:"total_checked"`
	Valid          bool       `json:"valid"`
	FirstFailureID *uuid.UUID `json:"first_failure_id,omitempty"`
	FailureReason  string     `json:"failure_reason,omitempty"`
}

// ChainIndexState tracks the last processed block per chain
type ChainIndexState struct {
	Chain           string    `json:"chain" db:"chain"`
	LastBlockNumber int64     `json:"last_block_number" db:"last_block_number"`
	LastBlockHash   string    `json:"last_block_hash" db:"last_block_hash"`
	UpdatedAt       time.Time `json:"updated_at" db:"updated_at"`
}

// SweepRecord tracks sweep transactions from deposit addresses to hot wallet
type SweepRecord struct {
	ID              uuid.UUID `json:"id" db:"id"`
	Chain           string    `json:"chain" db:"chain"`
	FromAddress     string    `json:"from_address" db:"from_address"`
	ToAddress       string    `json:"to_address" db:"to_address"`
	TokenAddress    string    `json:"token_address" db:"token_address"`
	Amount          *big.Int  `json:"amount" db:"amount"`
	TxHash          string    `json:"tx_hash" db:"tx_hash"`
	GasCost         *big.Int  `json:"gas_cost" db:"gas_cost"`
	State           string    `json:"state" db:"state"`
	DepositID       uuid.UUID `json:"deposit_id" db:"deposit_id"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
	ConfirmedAt     *time.Time `json:"confirmed_at,omitempty" db:"confirmed_at"`
}

// BlacklistedAddress represents a known bad address
type BlacklistedAddress struct {
	ID         uuid.UUID `json:"id" db:"id"`
	Chain      string    `json:"chain" db:"chain"`
	Address    string    `json:"address" db:"address"`
	Source     string    `json:"source" db:"source"`
	Reason     *string   `json:"reason,omitempty" db:"reason"`
	DetectedAt time.Time `json:"detected_at" db:"detected_at"`
	IsActive   bool      `json:"is_active" db:"is_active"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

// StablecoinContract represents a monitored stablecoin
type StablecoinContract struct {
	ID              uuid.UUID `json:"id" db:"id"`
	Chain           string    `json:"chain" db:"chain"`
	ContractAddress string    `json:"contract_address" db:"contract_address"`
	Symbol          string    `json:"symbol" db:"symbol"`
	Issuer          string    `json:"issuer" db:"issuer"`
	HasBlacklist    bool      `json:"has_blacklist" db:"has_blacklist"`
	HasFreeze       bool      `json:"has_freeze" db:"has_freeze"`
	BlacklistMethod *string   `json:"blacklist_method,omitempty" db:"blacklist_method"`
	FreezeMethod    *string   `json:"freeze_method,omitempty" db:"freeze_method"`
	Decimals        int       `json:"decimals" db:"decimals"`
	IsActive        bool      `json:"is_active" db:"is_active"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
}

// FreezeEvent represents a freeze/blacklist event on-chain
type FreezeEvent struct {
	ID              uuid.UUID `json:"id" db:"id"`
	Chain           string    `json:"chain" db:"chain"`
	Address         string    `json:"address" db:"address"`
	ContractAddress string    `json:"contract_address" db:"contract_address"`
	EventType       string    `json:"event_type" db:"event_type"`
	TxHash          *string   `json:"tx_hash,omitempty" db:"tx_hash"`
	BlockNumber     *int64    `json:"block_number,omitempty" db:"block_number"`
	DetectedAt      time.Time `json:"detected_at" db:"detected_at"`
}

// BlacklistCheckResult contains the result of a blacklist check
type BlacklistCheckResult struct {
	IsBlacklisted bool            `json:"is_blacklisted"`
	Sources       []string        `json:"sources"`
	Reasons       []string        `json:"reasons"`
	FreezeStatus  map[string]bool `json:"freeze_status"`
	CheckedAt     time.Time       `json:"checked_at"`
}

// RebalancePolicy defines thresholds for automatic tier rebalancing
type RebalancePolicy struct {
	ID                 uuid.UUID `json:"id" db:"id"`
	Chain              string    `json:"chain" db:"chain"`
	TokenSymbol        string    `json:"token_symbol" db:"token_symbol"`
	HotTargetPct       float64   `json:"hot_target_pct" db:"hot_target_pct"`
	HotMaxPct          float64   `json:"hot_max_pct" db:"hot_max_pct"`
	HotMinPct          float64   `json:"hot_min_pct" db:"hot_min_pct"`
	WarmTargetPct      float64   `json:"warm_target_pct" db:"warm_target_pct"`
	WarmMaxPct         float64   `json:"warm_max_pct" db:"warm_max_pct"`
	MinRebalanceAmount *big.Int  `json:"min_rebalance_amount" db:"min_rebalance_amount"`
	AutoRebalance      bool      `json:"auto_rebalance" db:"auto_rebalance"`
	IsActive           bool      `json:"is_active" db:"is_active"`
	CreatedAt          time.Time `json:"created_at" db:"created_at"`
	UpdatedAt          time.Time `json:"updated_at" db:"updated_at"`
}

// RebalanceState represents the lifecycle of a rebalance operation
type RebalanceState string

const (
	RebalancePending   RebalanceState = "PENDING"
	RebalanceApproved  RebalanceState = "APPROVED"
	RebalanceSigning   RebalanceState = "SIGNING"
	RebalanceBroadcast RebalanceState = "BROADCAST"
	RebalanceConfirmed RebalanceState = "CONFIRMED"
	RebalanceFailed    RebalanceState = "FAILED"
	RebalanceRejected  RebalanceState = "REJECTED"
)

// RebalanceOperation represents a fund movement between wallet tiers
type RebalanceOperation struct {
	ID               uuid.UUID      `json:"id" db:"id"`
	Chain            string         `json:"chain" db:"chain"`
	TokenSymbol      string         `json:"token_symbol" db:"token_symbol"`
	FromTier         WalletTier     `json:"from_tier" db:"from_tier"`
	ToTier           WalletTier     `json:"to_tier" db:"to_tier"`
	FromWalletID     uuid.UUID      `json:"from_wallet_id" db:"from_wallet_id"`
	ToWalletID       uuid.UUID      `json:"to_wallet_id" db:"to_wallet_id"`
	Amount           *big.Int       `json:"amount" db:"amount"`
	TxHash           *string        `json:"tx_hash,omitempty" db:"tx_hash"`
	State            RebalanceState `json:"state" db:"state"`
	RequiresApproval bool           `json:"requires_approval" db:"requires_approval"`
	ApprovedBy       *string        `json:"approved_by,omitempty" db:"approved_by"`
	LedgerEntryID    *uuid.UUID     `json:"ledger_entry_id,omitempty" db:"ledger_entry_id"`
	ErrorMessage     *string        `json:"error_message,omitempty" db:"error_message"`
	CreatedAt        time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at" db:"updated_at"`
}

// TierBalance represents balance per tier for a chain/token
type TierBalance struct {
	Chain        string   `json:"chain"`
	TokenSymbol  string   `json:"token_symbol"`
	HotBalance   *big.Int `json:"hot_balance"`
	WarmBalance  *big.Int `json:"warm_balance"`
	ColdBalance  *big.Int `json:"cold_balance"`
	TotalBalance *big.Int `json:"total_balance"`
	HotPct       float64  `json:"hot_pct"`
	WarmPct      float64  `json:"warm_pct"`
	ColdPct      float64  `json:"cold_pct"`
}

// FiatAccount represents a fiat bank/EMI account
type FiatAccount struct {
	ID                uuid.UUID       `json:"id"`
	WorkspaceID       uuid.UUID       `json:"workspace_id"`
	Currency          string          `json:"currency"`
	Provider          string          `json:"provider"`
	ProviderAccountID *string         `json:"provider_account_id,omitempty"`
	AccountType       string          `json:"account_type"`
	Balance           decimal.Decimal `json:"balance"`
	IsActive          bool            `json:"is_active"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// FiatTransactionType enumerates fiat transaction types
type FiatTransactionType string

const (
	FiatDeposit    FiatTransactionType = "DEPOSIT"
	FiatWithdrawal FiatTransactionType = "WITHDRAWAL"
	FiatConversion FiatTransactionType = "CONVERSION"
	FiatFee        FiatTransactionType = "FEE"
	FiatAdjustment FiatTransactionType = "ADJUSTMENT"
)

// FiatTransactionState enumerates fiat transaction states
type FiatTransactionState string

const (
	FiatStatePending    FiatTransactionState = "PENDING"
	FiatStateProcessing FiatTransactionState = "PROCESSING"
	FiatStateCompleted  FiatTransactionState = "COMPLETED"
	FiatStateFailed     FiatTransactionState = "FAILED"
	FiatStateReversed   FiatTransactionState = "REVERSED"
)

// FiatTransaction represents a fiat money movement
type FiatTransaction struct {
	ID            uuid.UUID              `json:"id"`
	WorkspaceID   uuid.UUID              `json:"workspace_id"`
	FiatAccountID uuid.UUID              `json:"fiat_account_id"`
	Type          FiatTransactionType    `json:"type"`
	Currency      string                 `json:"currency"`
	Amount        decimal.Decimal        `json:"amount"`
	Reference     *string                `json:"reference,omitempty"`
	Counterparty  *string                `json:"counterparty,omitempty"`
	State         FiatTransactionState   `json:"state"`
	LedgerEntryID *uuid.UUID             `json:"ledger_entry_id,omitempty"`
	Metadata      map[string]interface{} `json:"metadata"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// ConversionRate represents a currency conversion rate
type ConversionRate struct {
	ID           uuid.UUID       `json:"id"`
	FromCurrency string          `json:"from_currency"`
	ToCurrency   string          `json:"to_currency"`
	Rate         decimal.Decimal `json:"rate"`
	Source       string          `json:"source"`
	ValidFrom    time.Time       `json:"valid_from"`
	ValidUntil   *time.Time      `json:"valid_until,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

// ConversionRequest is a request to convert between fiat and crypto
type ConversionRequest struct {
	WorkspaceID  uuid.UUID       `json:"workspace_id"`
	FromCurrency string          `json:"from_currency"`
	ToCurrency   string          `json:"to_currency"`
	Amount       decimal.Decimal `json:"amount"`
	Direction    string          `json:"direction"`
}

// VelocityLimit defines withdrawal rate limits
type VelocityLimit struct {
	ID                  uuid.UUID `json:"id" db:"id"`
	Scope               string    `json:"scope" db:"scope"` // GLOBAL, WORKSPACE, USER, CHAIN
	ScopeID             *string   `json:"scope_id,omitempty" db:"scope_id"`
	Chain               *string   `json:"chain,omitempty" db:"chain"`
	TokenSymbol         *string   `json:"token_symbol,omitempty" db:"token_symbol"`
	MaxAmountPerTx      *big.Int  `json:"max_amount_per_tx,omitempty" db:"max_amount_per_tx"`
	MaxAmountPerHour    *big.Int  `json:"max_amount_per_hour,omitempty" db:"max_amount_per_hour"`
	MaxAmountPerDay     *big.Int  `json:"max_amount_per_day,omitempty" db:"max_amount_per_day"`
	MaxCountPerHour     *int      `json:"max_count_per_hour,omitempty" db:"max_count_per_hour"`
	MaxCountPerDay      *int      `json:"max_count_per_day,omitempty" db:"max_count_per_day"`
	CooldownSeconds     int       `json:"cooldown_seconds" db:"cooldown_seconds"`
	GeoAllowedCountries []string  `json:"geo_allowed_countries,omitempty" db:"geo_allowed_countries"`
	GeoBlockedCountries []string  `json:"geo_blocked_countries,omitempty" db:"geo_blocked_countries"`
	IsActive            bool      `json:"is_active" db:"is_active"`
	CreatedAt           time.Time `json:"created_at" db:"created_at"`
	UpdatedAt           time.Time `json:"updated_at" db:"updated_at"`
}

// WithdrawalAttempt logs each withdrawal attempt for velocity tracking
type WithdrawalAttempt struct {
	ID                  uuid.UUID `json:"id" db:"id"`
	WithdrawalID        uuid.UUID `json:"withdrawal_id" db:"withdrawal_id"`
	WorkspaceID         uuid.UUID `json:"workspace_id" db:"workspace_id"`
	UserID              uuid.UUID `json:"user_id" db:"user_id"`
	Chain               string    `json:"chain" db:"chain"`
	TokenSymbol         string    `json:"token_symbol" db:"token_symbol"`
	Amount              *big.Int  `json:"amount" db:"amount"`
	SourceIP            *string   `json:"source_ip,omitempty" db:"source_ip"`
	CountryCode         *string   `json:"country_code,omitempty" db:"country_code"`
	VelocityCheckPassed bool      `json:"velocity_check_passed" db:"velocity_check_passed"`
	RejectionReason     *string   `json:"rejection_reason,omitempty" db:"rejection_reason"`
	CreatedAt           time.Time `json:"created_at" db:"created_at"`
}

// VelocityCheckResult contains the result of a velocity check
type VelocityCheckResult struct {
	Allowed         bool     `json:"allowed"`
	RejectionReason string   `json:"rejection_reason,omitempty"`
	ApplicableRules []string `json:"applicable_rules"`
}
