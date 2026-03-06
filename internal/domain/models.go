package domain

import (
	"math/big"
	"time"

	"github.com/google/uuid"
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
