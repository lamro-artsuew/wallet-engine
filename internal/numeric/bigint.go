package numeric

import (
	"fmt"
	"math/big"
)

// ParseBigInt parses a numeric string into a *big.Int, returning an error if malformed.
// Used across repos and services to prevent silent-zero from corrupted DB values.
//
// Callers should wrap errors with field context:
//
//	amount, err := numeric.ParseBigInt(amountStr)
//	if err != nil { return nil, fmt.Errorf("amount: %w", err) }
func ParseBigInt(s string) (*big.Int, error) {
	n := new(big.Int)
	if _, ok := n.SetString(s, 10); !ok {
		return nil, fmt.Errorf("parseBigInt: cannot parse %q as integer", s)
	}
	return n, nil
}

// ParseOptionalBigInt parses a nullable string into an optional *big.Int.
// Returns (nil, nil) if input is nil. Returns error if present but malformed.
func ParseOptionalBigInt(s *string) (*big.Int, error) {
	if s == nil {
		return nil, nil
	}
	return ParseBigInt(*s)
}
