package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/testdb"
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

// TestExecutorLimitsSharesBulkhead pins that the composition root threads
// one bulkhead built from the configured caps into the executor limits, so
// executors, pollers, publishers, and custom nodes share it process-wide.
func TestExecutorLimitsSharesBulkhead(t *testing.T) {
	cfg := &configuration.Config{}
	cfg.Engine.HTTPAllowlist = []string{"example.com"}
	cfg.Engine.ExecAllowlist = []string{"/bin/echo"}
	cfg.Engine.MaxOutputBytes = 1024
	cfg.Engine.HTTPMaxInFlight = 3
	cfg.Engine.HTTPMaxInFlightPerHost = 2

	limits := executorLimits(cfg)
	if limits.MaxRedirects != 5 {
		t.Errorf("MaxRedirects = %d, want default 5", limits.MaxRedirects)
	}
	if limits.HTTPBulkhead == nil {
		t.Fatal("HTTPBulkhead is nil, want the shared instance")
	}
	if limits.HTTPMaxInFlight != 3 || limits.HTTPMaxInFlightPerHost != 2 {
		t.Errorf("caps = %d/%d, want 3/2 from config",
			limits.HTTPMaxInFlight, limits.HTTPMaxInFlightPerHost)
	}

	// The bulkhead honors the configured global cap: 3 distinct-host
	// slots admit, the 4th sheds.
	ctx := context.Background()
	var releases []func()
	for i := 0; i < 3; i++ {
		release, err := limits.HTTPBulkhead.Acquire(ctx, fmt.Sprintf("h%d.example", i))
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := limits.HTTPBulkhead.Acquire(short, "other.example"); !errors.Is(err, executor.ErrHTTPOverloaded) {
		t.Fatalf("over-cap acquire err = %v, want ErrHTTPOverloaded", err)
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
		&repository.SecretModel{},
		&repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{},
		&repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{},
		&repository.CronScheduleModel{},
		&repository.ScheduleFireModel{},
		&repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{},
		&repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{},
		&repository.StatusUpdateOutboxModel{},
		&repository.ParallelExecutionModel{},
		&repository.ParallelBranchModel{},
	); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
}

func TestRunShutsDownGracefully(t *testing.T) {
	dsn := testdb.RequireDSN(t, "app")
	bootstrapSchema(t, dsn)

	host := freePort(t)
	cfg := &configuration.Config{
		Infra: configuration.Infra{
			HTTP: configuration.HTTP{Host: host},
			PostgreSQL: configuration.PostgreSQL{
				DSN:             dsn,
				MaxOpenConns:    25,
				MaxIdleConns:    25,
				ConnMaxLifetime: 5 * time.Minute,
				ConnMaxIdleTime: 5 * time.Minute,
			},
		},
		Worker: configuration.Worker{
			Pool: configuration.WorkerPool{Size: 4},
		},
		Scheduler: configuration.Scheduler{Enabled: true},
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

// TestDatabaseOptions pins the config→pool mapping: explicit config values
// reach database.New unchanged, and a Load-defaulted config never yields
// zero pool fields (the original bug class: a bare database.Options{DSN}
// reaching New from production).
func TestDatabaseOptions(t *testing.T) {
	explicit := &configuration.Config{
		Infra: configuration.Infra{
			PostgreSQL: configuration.PostgreSQL{
				DSN:             "explicit-dsn",
				MaxOpenConns:    10,
				MaxIdleConns:    4,
				ConnMaxLifetime: 10 * time.Minute,
				ConnMaxIdleTime: 2 * time.Minute,
			},
		},
	}
	opts := databaseOptions(explicit)
	if opts.DSN != "explicit-dsn" || opts.MaxOpenConns != 10 || opts.MaxIdleConns != 4 ||
		opts.ConnMaxLifetime != 10*time.Minute || opts.ConnMaxIdleTime != 2*time.Minute {
		t.Fatalf("databaseOptions() = %+v, want explicit config values verbatim", opts)
	}

	t.Setenv("SIMPWF_INFRA_POSTGRESQL_DSN", "default-dsn")
	loaded, err := configuration.Load(configuration.WithConfigFile(filepath.Join(t.TempDir(), "does-not-exist.yaml")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	def := databaseOptions(loaded)
	if def.DSN != "default-dsn" {
		t.Errorf("DSN = %q, want default-dsn", def.DSN)
	}
	if def.MaxOpenConns == 0 || def.MaxIdleConns == 0 || def.ConnMaxLifetime == 0 || def.ConnMaxIdleTime == 0 {
		t.Errorf("databaseOptions() from defaults = %+v, want all pool fields non-zero", def)
	}
	if def.MaxOpenConns != 25 {
		t.Errorf("MaxOpenConns = %d, want 25", def.MaxOpenConns)
	}
}

// TestDatabasePoolCeiling proves the production pool is bounded: options
// built by databaseOptions from a default config open a pool whose
// MaxOpenConnections is 25. No load needed — the ceiling is a setting
// assertion, deterministic and fast.
func TestDatabasePoolCeiling(t *testing.T) {
	dsn := testdb.RequireDSN(t, "app")

	t.Setenv("SIMPWF_INFRA_POSTGRESQL_DSN", dsn)
	cfg, err := configuration.Load(configuration.WithConfigFile(filepath.Join(t.TempDir(), "does-not-exist.yaml")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	db, err := database.New(databaseOptions(cfg))
	if err != nil {
		t.Fatalf("database.New() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}
	defer func() { _ = sqlDB.Close() }()
	if got := sqlDB.Stats().MaxOpenConnections; got != 25 {
		t.Errorf("Stats().MaxOpenConnections = %d, want 25", got)
	}
}

// TestStatusDispatcherOptions pins the config→options mapping: explicit
// worker/engine values and resolved limits reach the status dispatcher
// options verbatim, and Load defaults yield 20 / 200ms / 30s / 10 (the
// original bug: run() built the status dispatcher with empty options, so
// every config value was silently dropped).
func TestStatusDispatcherOptions(t *testing.T) {
	explicit := &configuration.Config{}
	explicit.Worker.StatusPool.Size = 7
	explicit.Engine.PollInterval = 111 * time.Millisecond
	limits := model.DefaultLimits()
	limits.LeaseDuration = 45 * time.Second
	limits.ClaimBatchSize = 7
	if opts := statusDispatcherOptions(explicit, limits); opts.PoolSize != 7 ||
		opts.PollInterval != 111*time.Millisecond || opts.Lease != 45*time.Second || opts.BatchSize != 7 {
		t.Fatalf("statusDispatcherOptions() = %+v, want explicit values verbatim", opts)
	}

	t.Setenv("SIMPWF_INFRA_POSTGRESQL_DSN", "default-dsn")
	loaded, err := configuration.Load(configuration.WithConfigFile(filepath.Join(t.TempDir(), "does-not-exist.yaml")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if opts := statusDispatcherOptions(loaded, model.DefaultLimits()); opts.PoolSize != 20 ||
		opts.PollInterval != 200*time.Millisecond || opts.Lease != 30*time.Second || opts.BatchSize != 10 {
		t.Errorf("statusDispatcherOptions() from defaults = %+v, want 20/200ms/30s/10", opts)
	}
}

// TestEngineDispatcherOptions pins the config→options mapping: the engine
// dispatcher gets the poll/heartbeat intervals from config, the claim knobs
// from the resolved limits, and the pool size from worker config (the
// original bug: run() omitted PollInterval and Heartbeat, so both ran on
// hardcoded fallbacks no matter the config).
func TestEngineDispatcherOptions(t *testing.T) {
	explicit := &configuration.Config{}
	explicit.Worker.Pool.Size = 11
	explicit.Engine.PollInterval = 111 * time.Millisecond
	explicit.Engine.HeartbeatInterval = 7 * time.Second
	limits := model.DefaultLimits()
	limits.LeaseDuration = 45 * time.Second
	limits.ClaimBatchSize = 7
	if opts := engineDispatcherOptions(explicit, limits); opts.PoolSize != 11 ||
		opts.PollInterval != 111*time.Millisecond || opts.Heartbeat != 7*time.Second ||
		opts.Lease != 45*time.Second || opts.BatchSize != 7 {
		t.Fatalf("engineDispatcherOptions() = %+v, want explicit values verbatim", opts)
	}

	t.Setenv("SIMPWF_INFRA_POSTGRESQL_DSN", "default-dsn")
	loaded, err := configuration.Load(configuration.WithConfigFile(filepath.Join(t.TempDir(), "does-not-exist.yaml")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if opts := engineDispatcherOptions(loaded, model.DefaultLimits()); opts.PoolSize != 1000 ||
		opts.PollInterval != 200*time.Millisecond || opts.Heartbeat != 5*time.Second ||
		opts.Lease != 30*time.Second || opts.BatchSize != 10 {
		t.Errorf("engineDispatcherOptions() from defaults = %+v, want 1000/200ms/5s/30s/10", opts)
	}
}

// TestCheckAuthWiringRefusesUnattributableOIDC: with OIDC on, a missing
// identity resolver or audit actor is refused at boot. The resolver gap
// strands human principals; the actor gap silently attributes the service
// principal and engine background effects to nobody. Neither reports itself
// at request time.
func TestCheckAuthWiringRefusesUnattributableOIDC(t *testing.T) {
	svc := &stubAuthService{}
	const actor = "00000000-0000-7000-8000-000000000001"

	if err := checkAuthWiring(nil, nil, ""); err != nil {
		t.Errorf("OIDC off with no resolver: error = %v, want nil", err)
	}
	if err := checkAuthWiring(nil, svc, actor); err != nil {
		t.Errorf("OIDC off with a resolver: error = %v, want nil", err)
	}
	if err := checkAuthWiring(stubWiringVerifier{}, svc, actor); err != nil {
		t.Errorf("OIDC on, resolver and actor present: error = %v, want nil", err)
	}
	if err := checkAuthWiring(stubWiringVerifier{}, nil, actor); err == nil {
		t.Error("OIDC on without a resolver: error = nil, want a refusal")
	}
	for _, actor := range []string{"", "   "} {
		if err := checkAuthWiring(stubWiringVerifier{}, svc, actor); err == nil {
			t.Errorf("OIDC on with actor %q: error = nil, want a refusal", actor)
		}
	}
}

// stubWiringVerifier stands in for a configured OIDC verifier; the wiring
// guard only ever asks whether one is present.
type stubWiringVerifier struct{}

func (stubWiringVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, nil
}

func (stubWiringVerifier) Config() auth.AuthConfig {
	return auth.AuthConfig{Enabled: true, Issuer: "https://issuer.test"}
}

// stubAuthService stands in for the wired auth service.
type stubAuthService struct{}

func (stubAuthService) ResolvePrincipal(_ context.Context, p auth.Principal) (auth.Principal, error) {
	return p, nil
}
func (stubAuthService) SeedRoles(context.Context) error { return nil }
func (stubAuthService) ListRoles(context.Context) ([]model.Role, map[string][]string, error) {
	return nil, nil, nil
}
func (stubAuthService) GetRole(context.Context, string) (model.Role, []string, error) {
	return model.Role{}, nil, nil
}
func (stubAuthService) Me(context.Context, auth.Principal) (model.User, error) {
	return model.User{}, nil
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
