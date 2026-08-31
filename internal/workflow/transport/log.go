// Package transport logging. Kept behind a helper so the package can log
// without leaking slog into every adapter.
package transport

import (
	"log/slog"
)

func logf(msg string, args ...any) {
	slog.Warn(msg, args...)
}
