package repository

import (
	"fmt"
	"math/big"
)

// parseBigInt parses a numeric string into a *big.Int, returning an error if malformed.
// Used across all repos to prevent silent-zero from corrupted DB values.
func parseBigInt(s string) (*big.Int, error) {
	n := new(big.Int)
	if _, ok := n.SetString(s, 10); !ok {
		return nil, fmt.Errorf("invalid integer value: %q", s)
	}
	return n, nil
}

// parseOptionalBigInt parses a nullable string into an optional *big.Int.
// Returns nil target if input is nil. Returns error if present but malformed.
func parseOptionalBigInt(s *string, target **big.Int) error {
	if s == nil {
		*target = nil
		return nil
	}
	v, err := parseBigInt(*s)
	if err != nil {
		return err
	}
	*target = v
	return nil
}
