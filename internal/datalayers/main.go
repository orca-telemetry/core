// Package datalayers provides a factory function for generating a
// datalayer client. Current supported datalayers are:
// - PostgreSQL
package datalayers

import (
	"context"
	"fmt"
	"log/slog"

	psql "github.com/orc-analytics/orca/internal/datalayers/postgresql"
	types "github.com/orc-analytics/orca/internal/types"
)

// Platform resprents a database storage platform (e.g. PostgreSQL)
type Platform string

const (
	// PostgreSQL is the postgresql platform
	PostgreSQL Platform = "postgresql"
)

// check if the platform is supported
func (p Platform) isValid() bool {
	switch p {
	case PostgreSQL:
		return true
	default:
		return false
	}
}

// NewDatalayerClient generates a new datalayer client of the specificed type.
func NewDatalayerClient(
	ctx context.Context,
	platform Platform,
	connStr string,
) (types.Datalayer, error) {
	if !platform.isValid() {
		return nil, fmt.Errorf("unsupported platform: %s", platform)
	}

	switch platform {
	case PostgreSQL:
		return psql.NewClient(ctx, connStr)
	default:
		slog.Error(
			"attempted to access unsuported platform",
			"platform",
			platform,
		)
		return nil, fmt.Errorf("platform not implemented: %s", platform)
	}
}
