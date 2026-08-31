package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// NewLogger builds a Logrus logger with the given textual level
// (e.g. "info", "debug", "warn", "error").
func NewLogger(level string) (*logrus.Logger, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("app: invalid log level %q: %w", level, err)
	}
	logger := logrus.New()
	logger.SetLevel(lvl)
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	return logger, nil
}
