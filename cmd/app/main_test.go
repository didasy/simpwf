package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/pkg/configuration"
	"github.com/simpwf/workflow-engine/pkg/database"
	"github.com/sirupsen/logrus"
)

func TestNewLogger(t *testing.T) {
	logger, err := NewLogger("debug")
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	if logger.GetLevel() != logrus.DebugLevel {
		t.Errorf("level = %v, want debug", logger.GetLevel())
	}
	if logger.Formatter == nil {
		t.Error("formatter not set")
	}
}

func TestNewLoggerRejectsInvalidLevel(t *testing.T) {
	if _, err := NewLogger("loud"); err == nil {
		t.Fatal("NewLogger() error = nil, want error for invalid level")
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
}

func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("server %s did not start within %s", addr, timeout)
}

// bootstrapSchema creates the test schema. The production app never
// migrates; Atlas owns migrations, and the test database needs the tables
// before run() seeds the system user.
func bootstrapSchema(t *testing.T, dsn string) {
	t.Helper()
	opts := database.DefaultOptions()
	opts.DSN = dsn
	db, err := database.New(opts)
	if err != nil {
		t.Fatalf("database.New() error = %v", err)
	}
	defer func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	}()
	if err := db.AutoMigrate(
		&repository.UserModel{},
		&repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{},
		&repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{},
		&repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{},
		&repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{},
		&repository.StatusUpdateOutboxModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
}

func TestRunShutsDownGracefully(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_DSN_APP")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_DSN")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set; skipping live database test")
	}
	bootstrapSchema(t, dsn)

	host := freePort(t)
	cfg := &configuration.Config{
		Infra: configuration.Infra{
			HTTP:       configuration.HTTP{Host: host},
			PostgreSQL: configuration.PostgreSQL{DSN: dsn},
		},
		Worker: configuration.Worker{
			Pool:             configuration.WorkerPool{Size: 4},
			ExpiryDuration:   time.Minute,
			MaxBlockingTasks: 4,
		},
	}
	logger, err := NewLogger("info")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg, logger) }()

	waitForServer(t, host, 3*time.Second)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() error = %v, want nil on graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	if _, err := net.DialTimeout("tcp", host, 500*time.Millisecond); err == nil {
		t.Error("server still listening after graceful shutdown")
	}
}

// TestCompositionNilBrokerDependencies mirrors the app wiring with brokers
// disabled: the executor set stays HTTP-only, and broker poller nodes fail
// with a missing-transport error instead of crashing startup.
func TestCompositionNilBrokerDependencies(t *testing.T) {
	execLimits := executor.Limits{HTTPAllowlist: []string{"127.0.0.1"}, MaxOutputBytes: 1 << 20, MaxRedirects: 5}
	reg := executor.NewExecutors(execLimits, nil, executor.Dependencies{})
	poller, ok := reg[model.NodeTypePoller]
	if !ok {
		t.Fatal("poller executor not registered")
	}
	redisCfg := &model.PollerRedisConfig{Method: "GET", Key: "k", RequestTimeout: time.Second, MaxAttempts: 1, Until: "return true;"}
	_, err := poller.Execute(context.Background(), executor.Request{
		Node:    &model.NodeContent{Type: model.NodeTypePoller, PollerRedis: redisCfg, PredicateTimeout: time.Second},
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("redis poller error = %v, want missing transport", err)
	}
	rabbitCfg := &model.PollerRabbitMQConfig{Queue: "q", MaxWaitTime: time.Second, Until: "return true;"}
	_, err = poller.Execute(context.Background(), executor.Request{
		Node:    &model.NodeContent{Type: model.NodeTypePoller, PollerRabbitMQ: rabbitCfg, PredicateTimeout: time.Second},
		Context: map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "rabbitmq") {
		t.Fatalf("rabbitmq poller error = %v, want missing transport", err)
	}
}
