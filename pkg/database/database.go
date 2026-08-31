// Package database opens and configures the PostgreSQL connection pool used
// by GORM. It contains no domain logic; it only turns Options into a ready
// *gorm.DB with the requested pool settings.
package database

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Options controls the PostgreSQL connection pool.
type Options struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	LogLevel        gormlogger.LogLevel
}

// DefaultOptions returns sane pool defaults.
func DefaultOptions() Options {
	return Options{
		MaxOpenConns:    25,
		MaxIdleConns:    25,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		LogLevel:        gormlogger.Warn,
	}
}

// New opens a PostgreSQL connection pool and verifies it with a ping.
func New(opts Options) (*gorm.DB, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("database: empty dsn")
	}

	db, err := gorm.Open(postgres.New(postgres.Config{DSN: opts.DSN}), &gorm.Config{
		Logger: gormlogger.Default.LogMode(opts.LogLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("database: open: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("database: pool: %w", err)
	}
	sqlDB.SetMaxOpenConns(opts.MaxOpenConns)
	sqlDB.SetMaxIdleConns(opts.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(opts.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("database: ping: %w", err)
	}
	return db, nil
}
