package repository

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lamro-artsuew/wallet-engine/internal/domain"
)

// VelocityRepo handles velocity limit and withdrawal attempt persistence
type VelocityRepo struct {
	pool *pgxpool.Pool
}

func NewVelocityRepo(pool *pgxpool.Pool) *VelocityRepo {
	return &VelocityRepo{pool: pool}
}

// FindLimits returns all applicable velocity limits ordered by most specific first.
// It matches GLOBAL, plus WORKSPACE/USER/CHAIN scopes where scope_id matches,
// and filters by chain/token (NULL = all).
func (r *VelocityRepo) FindLimits(ctx context.Context, workspaceID uuid.UUID, userID uuid.UUID, chain string, token string) ([]*domain.VelocityLimit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, scope, scope_id, chain, token_symbol,
			max_amount_per_tx, max_amount_per_hour, max_amount_per_day,
			max_count_per_hour, max_count_per_day, cooldown_seconds,
			geo_allowed_countries, geo_blocked_countries,
			is_active, created_at, updated_at
		FROM velocity_limits
		WHERE is_active = TRUE
		  AND (
			(scope = 'GLOBAL')
			OR (scope = 'WORKSPACE' AND scope_id = $1)
			OR (scope = 'USER' AND scope_id = $2)
			OR (scope = 'CHAIN' AND scope_id = $3)
		  )
		  AND (chain IS NULL OR chain = $3)
		  AND (token_symbol IS NULL OR token_symbol = $4)
		ORDER BY
			CASE scope
				WHEN 'USER' THEN 1
				WHEN 'WORKSPACE' THEN 2
				WHEN 'CHAIN' THEN 3
				WHEN 'GLOBAL' THEN 4
			END
	`, workspaceID.String(), userID.String(), chain, token)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVelocityLimits(rows)
}

// FindAllLimits returns all velocity limits (for admin listing)
func (r *VelocityRepo) FindAllLimits(ctx context.Context) ([]*domain.VelocityLimit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, scope, scope_id, chain, token_symbol,
			max_amount_per_tx, max_amount_per_hour, max_amount_per_day,
			max_count_per_hour, max_count_per_day, cooldown_seconds,
			geo_allowed_countries, geo_blocked_countries,
			is_active, created_at, updated_at
		FROM velocity_limits
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVelocityLimits(rows)
}

// FindLimitByID returns a single velocity limit
func (r *VelocityRepo) FindLimitByID(ctx context.Context, id uuid.UUID) (*domain.VelocityLimit, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, scope, scope_id, chain, token_symbol,
			max_amount_per_tx, max_amount_per_hour, max_amount_per_day,
			max_count_per_hour, max_count_per_day, cooldown_seconds,
			geo_allowed_countries, geo_blocked_countries,
			is_active, created_at, updated_at
		FROM velocity_limits WHERE id = $1
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	limits, err := scanVelocityLimits(rows)
	if err != nil {
		return nil, err
	}
	if len(limits) == 0 {
		return nil, fmt.Errorf("velocity limit %s not found", id)
	}
	return limits[0], nil
}

// CreateLimit inserts a new velocity limit
func (r *VelocityRepo) CreateLimit(ctx context.Context, vl *domain.VelocityLimit) error {
	var maxTx, maxHour, maxDay *string
	if vl.MaxAmountPerTx != nil {
		s := vl.MaxAmountPerTx.String()
		maxTx = &s
	}
	if vl.MaxAmountPerHour != nil {
		s := vl.MaxAmountPerHour.String()
		maxHour = &s
	}
	if vl.MaxAmountPerDay != nil {
		s := vl.MaxAmountPerDay.String()
		maxDay = &s
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO velocity_limits (
			id, scope, scope_id, chain, token_symbol,
			max_amount_per_tx, max_amount_per_hour, max_amount_per_day,
			max_count_per_hour, max_count_per_day, cooldown_seconds,
			geo_allowed_countries, geo_blocked_countries, is_active
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`,
		vl.ID, vl.Scope, vl.ScopeID, vl.Chain, vl.TokenSymbol,
		maxTx, maxHour, maxDay,
		vl.MaxCountPerHour, vl.MaxCountPerDay, vl.CooldownSeconds,
		vl.GeoAllowedCountries, vl.GeoBlockedCountries, vl.IsActive,
	)
	return err
}

// UpdateLimit updates an existing velocity limit
func (r *VelocityRepo) UpdateLimit(ctx context.Context, vl *domain.VelocityLimit) error {
	var maxTx, maxHour, maxDay *string
	if vl.MaxAmountPerTx != nil {
		s := vl.MaxAmountPerTx.String()
		maxTx = &s
	}
	if vl.MaxAmountPerHour != nil {
		s := vl.MaxAmountPerHour.String()
		maxHour = &s
	}
	if vl.MaxAmountPerDay != nil {
		s := vl.MaxAmountPerDay.String()
		maxDay = &s
	}

	tag, err := r.pool.Exec(ctx, `
		UPDATE velocity_limits SET
			scope = $1, scope_id = $2, chain = $3, token_symbol = $4,
			max_amount_per_tx = $5, max_amount_per_hour = $6, max_amount_per_day = $7,
			max_count_per_hour = $8, max_count_per_day = $9, cooldown_seconds = $10,
			geo_allowed_countries = $11, geo_blocked_countries = $12,
			is_active = $13, updated_at = NOW()
		WHERE id = $14
	`,
		vl.Scope, vl.ScopeID, vl.Chain, vl.TokenSymbol,
		maxTx, maxHour, maxDay,
		vl.MaxCountPerHour, vl.MaxCountPerDay, vl.CooldownSeconds,
		vl.GeoAllowedCountries, vl.GeoBlockedCountries,
		vl.IsActive, vl.ID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("velocity limit %s not found", vl.ID)
	}
	return nil
}

// DeleteLimit soft-deletes a velocity limit
func (r *VelocityRepo) DeleteLimit(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE velocity_limits SET is_active = FALSE, updated_at = NOW() WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("velocity limit %s not found", id)
	}
	return nil
}

// LogAttempt inserts a withdrawal attempt record
func (r *VelocityRepo) LogAttempt(ctx context.Context, a *domain.WithdrawalAttempt) error {
	var withdrawalID *uuid.UUID
	if a.WithdrawalID != uuid.Nil {
		withdrawalID = &a.WithdrawalID
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO withdrawal_attempts (
			id, withdrawal_id, workspace_id, user_id, chain, token_symbol,
			amount, source_ip, country_code, velocity_check_passed, rejection_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		a.ID, withdrawalID, a.WorkspaceID, a.UserID, a.Chain, a.TokenSymbol,
		a.Amount.String(), a.SourceIP, a.CountryCode,
		a.VelocityCheckPassed, a.RejectionReason,
	)
	return err
}

// CountWithdrawalsInWindow counts successful withdrawal attempts for a user/chain since a given time
func (r *VelocityRepo) CountWithdrawalsInWindow(ctx context.Context, userID uuid.UUID, chain string, since time.Time) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM withdrawal_attempts
		WHERE user_id = $1 AND chain = $2 AND created_at >= $3 AND velocity_check_passed = TRUE
	`, userID, chain, since).Scan(&count)
	return count, err
}

// SumWithdrawalsInWindow sums withdrawal amounts for a user/chain/token since a given time
func (r *VelocityRepo) SumWithdrawalsInWindow(ctx context.Context, userID uuid.UUID, chain string, token string, since time.Time) (*big.Int, error) {
	var sumStr *string
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)::TEXT FROM withdrawal_attempts
		WHERE user_id = $1 AND chain = $2 AND token_symbol = $3
		  AND created_at >= $4 AND velocity_check_passed = TRUE
	`, userID, chain, token, since).Scan(&sumStr)
	if err != nil {
		return nil, err
	}
	result := new(big.Int)
	if sumStr != nil {
		result.SetString(*sumStr, 10)
	}
	return result, nil
}

// LastWithdrawalTime returns the most recent successful withdrawal time for a user/chain
func (r *VelocityRepo) LastWithdrawalTime(ctx context.Context, userID uuid.UUID, chain string) (*time.Time, error) {
	var t *time.Time
	err := r.pool.QueryRow(ctx, `
		SELECT MAX(created_at) FROM withdrawal_attempts
		WHERE user_id = $1 AND chain = $2 AND velocity_check_passed = TRUE
	`, userID, chain).Scan(&t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func scanVelocityLimits(rows pgx.Rows) ([]*domain.VelocityLimit, error) {
	var limits []*domain.VelocityLimit
	for rows.Next() {
		vl := &domain.VelocityLimit{}
		var maxTxStr, maxHourStr, maxDayStr *string
		err := rows.Scan(
			&vl.ID, &vl.Scope, &vl.ScopeID, &vl.Chain, &vl.TokenSymbol,
			&maxTxStr, &maxHourStr, &maxDayStr,
			&vl.MaxCountPerHour, &vl.MaxCountPerDay, &vl.CooldownSeconds,
			&vl.GeoAllowedCountries, &vl.GeoBlockedCountries,
			&vl.IsActive, &vl.CreatedAt, &vl.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		if maxTxStr != nil {
			vl.MaxAmountPerTx = new(big.Int)
			vl.MaxAmountPerTx.SetString(*maxTxStr, 10)
		}
		if maxHourStr != nil {
			vl.MaxAmountPerHour = new(big.Int)
			vl.MaxAmountPerHour.SetString(*maxHourStr, 10)
		}
		if maxDayStr != nil {
			vl.MaxAmountPerDay = new(big.Int)
			vl.MaxAmountPerDay.SetString(*maxDayStr, 10)
		}
		limits = append(limits, vl)
	}
	return limits, rows.Err()
}
