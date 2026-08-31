package database_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/pkg/database"
)

func TestNewRejectsEmptyDSN(t *testing.T) {
	_, err := database.New(database.Options{})
	if err == nil {
		t.Fatal("New() error = nil, want error for empty DSN")
	}
	if !strings.Contains(err.Error(), "dsn") {
		t.Errorf("error = %q, want mention of dsn", err)
	}
}

func TestNewRejectsUnreachableDatabase(t *testing.T) {
	_, err := database.New(database.Options{DSN: "host=127.0.0.1 user=x password=x dbname=x port=1 sslmode=disable"})
	if err == nil {
		t.Fatal("New() error = nil, want error for unreachable database")
	}
}

func TestNewConnectsAndAppliesPoolOptions(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set; skipping live database test")
	}

	opts := database.DefaultOptions()
	opts.DSN = dsn
	opts.MaxOpenConns = 7
	opts.MaxIdleConns = 4
	opts.ConnMaxLifetime = 90 * time.Second
	opts.ConnMaxIdleTime = 30 * time.Second

	db, err := database.New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}
	defer func() { _ = sqlDB.Close() }()

	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if got := sqlDB.Stats().MaxOpenConnections; got != opts.MaxOpenConns {
		t.Errorf("MaxOpenConnections = %d, want %d", got, opts.MaxOpenConns)
	}
}
