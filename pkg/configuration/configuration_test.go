package configuration_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/pkg/configuration"
)

const testDSN = "host=localhost user=gorm password=gorm dbname=gorm port=9921 sslmode=disable"

// missingPath returns a config file path that never exists (env-only mode).
func missingPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "does-not-exist.yaml")
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func setenv(t *testing.T, key, value string) {
	t.Helper()
	t.Setenv(key, value)
}

func TestLoadEnvOnly(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_INFRA_HTTP_HOST", "localhost:9999")
	setenv(t, "SIMPWF_INFRA_HTTP_SWAGGER_ENABLED", "false")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.HTTP.Host != "localhost:9999" {
		t.Errorf("host = %q, want localhost:9999", cfg.Infra.HTTP.Host)
	}
	if cfg.Infra.PostgreSQL.DSN != testDSN {
		t.Errorf("dsn = %q, want %q", cfg.Infra.PostgreSQL.DSN, testDSN)
	}
	if cfg.Infra.HTTP.SwaggerEnabled {
		t.Error("swagger_enabled = true, want false from environment")
	}
}

func TestLoadConfigFileOverridesEnv(t *testing.T) {
	path := writeConfig(t, `
infra:
  http:
    host: "file-host:1234"
    swagger_enabled: false
  postgresql:
    dsn: "file-dsn"
`)
	setenv(t, "SIMPWF_INFRA_HTTP_HOST", "env-host:5678")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.HTTP.Host != "file-host:1234" {
		t.Errorf("host = %q, want file-host:1234 (file must take priority over env)", cfg.Infra.HTTP.Host)
	}
	if cfg.Infra.PostgreSQL.DSN != "file-dsn" {
		t.Errorf("dsn = %q, want file-dsn", cfg.Infra.PostgreSQL.DSN)
	}
	if cfg.Infra.HTTP.SwaggerEnabled {
		t.Error("swagger_enabled = true, want false from config file")
	}
}

func TestLoadDefaultsApplied(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.HTTP.Host != "localhost:8080" {
		t.Errorf("default host = %q, want localhost:8080", cfg.Infra.HTTP.Host)
	}
	if !cfg.Infra.HTTP.SwaggerEnabled {
		t.Error("default swagger_enabled = false, want true")
	}
	if cfg.Worker.Pool.Size != 1000 {
		t.Errorf("default pool size = %d, want 1000", cfg.Worker.Pool.Size)
	}
	if cfg.Worker.ExpiryDuration != 5*time.Minute {
		t.Errorf("default expiry = %v, want 5m", cfg.Worker.ExpiryDuration)
	}
}

func TestLoadDurationFromEnv(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_WORKER_EXPIRY_DURATION", "10s")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Worker.ExpiryDuration != 10*time.Second {
		t.Errorf("expiry = %v, want 10s", cfg.Worker.ExpiryDuration)
	}
}

func TestLoadRejectsMissingDSN(t *testing.T) {
	_, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err == nil {
		t.Fatal("Load() error = nil, want validation error for missing dsn")
	}
	if !strings.Contains(err.Error(), "dsn") {
		t.Errorf("error = %q, want mention of dsn", err)
	}
}

func TestLoadRejectsInvalidPoolSize(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_WORKER_POOL_SIZE", "0")

	_, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err == nil {
		t.Fatal("Load() error = nil, want validation error for pool size 0")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_WORKER_EXPIRY_DURATION", "not-a-duration")

	_, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err == nil {
		t.Fatal("Load() error = nil, want error for invalid duration")
	}
}

func TestLoadRejectsInvalidConfigFile(t *testing.T) {
	path := writeConfig(t, "::not: [valid yaml")

	_, err := configuration.Load(configuration.WithConfigFile(path))
	if err == nil {
		t.Fatal("Load() error = nil, want error for invalid yaml")
	}
}

func TestLoadEngineDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Engine.DefaultNodeTimeout != 30*time.Second {
		t.Errorf("default_node_timeout = %v, want 30s", cfg.Engine.DefaultNodeTimeout)
	}
	if cfg.Engine.MaxNodeTimeout != 5*time.Minute {
		t.Errorf("max_node_timeout = %v, want 5m", cfg.Engine.MaxNodeTimeout)
	}
	if cfg.Engine.ConditionTimeout != 5*time.Second {
		t.Errorf("condition_timeout = %v, want 5s", cfg.Engine.ConditionTimeout)
	}
	if cfg.System.UserID == "" || cfg.System.Name != "system" || cfg.System.Email == "" {
		t.Errorf("system defaults mismatch: %+v", cfg.System)
	}
}

func TestLoadEngineSettingsFromEnv(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_ENGINE_MAX_PER_NODE_EXECUTIONS", "50")
	setenv(t, "SIMPWF_ENGINE_MAX_TOTAL_EXECUTIONS", "500")
	setenv(t, "SIMPWF_ENGINE_LEASE_DURATION", "45s")
	setenv(t, "SIMPWF_ENGINE_CLAIM_BATCH_SIZE", "7")
	setenv(t, "SIMPWF_ENGINE_MAX_OUTPUT_BYTES", "2048")
	setenv(t, "SIMPWF_ENGINE_MAX_REDIRECTS", "3")
	setenv(t, "SIMPWF_ENGINE_HTTP_ALLOWLIST", "*")
	setenv(t, "SIMPWF_ENGINE_EXEC_ALLOWLIST", "echo")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Engine.MaxPerNodeExecutions != 50 {
		t.Errorf("max_per_node_executions = %d, want 50", cfg.Engine.MaxPerNodeExecutions)
	}
	if cfg.Engine.MaxTotalExecutions != 500 {
		t.Errorf("max_total_executions = %d, want 500", cfg.Engine.MaxTotalExecutions)
	}
	if cfg.Engine.LeaseDuration != 45*time.Second {
		t.Errorf("lease_duration = %v, want 45s", cfg.Engine.LeaseDuration)
	}
	if cfg.Engine.ClaimBatchSize != 7 {
		t.Errorf("claim_batch_size = %d, want 7", cfg.Engine.ClaimBatchSize)
	}
	if cfg.Engine.MaxOutputBytes != 2048 {
		t.Errorf("max_output_bytes = %d, want 2048", cfg.Engine.MaxOutputBytes)
	}
	if cfg.Engine.MaxRedirects != 3 {
		t.Errorf("max_redirects = %d, want 3", cfg.Engine.MaxRedirects)
	}
	if len(cfg.Engine.HTTPAllowlist) != 1 || cfg.Engine.HTTPAllowlist[0] != "*" {
		t.Errorf("http_allowlist = %q, want [\"*\"]", cfg.Engine.HTTPAllowlist)
	}
	if len(cfg.Engine.ExecAllowlist) != 1 || cfg.Engine.ExecAllowlist[0] != "echo" {
		t.Errorf("exec_allowlist = %q, want [\"echo\"]", cfg.Engine.ExecAllowlist)
	}
}

func TestLoadEngineAllowlistsFromEnvCommaSeparated(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_ENGINE_HTTP_ALLOWLIST", "api.example.com,jsonplaceholder.typicode.com")
	setenv(t, "SIMPWF_ENGINE_EXEC_ALLOWLIST", "echo,ls")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !slices.Equal(cfg.Engine.HTTPAllowlist, []string{"api.example.com", "jsonplaceholder.typicode.com"}) {
		t.Errorf("http_allowlist = %q, want [api.example.com jsonplaceholder.typicode.com]", cfg.Engine.HTTPAllowlist)
	}
	if !slices.Equal(cfg.Engine.ExecAllowlist, []string{"echo", "ls"}) {
		t.Errorf("exec_allowlist = %q, want [echo ls]", cfg.Engine.ExecAllowlist)
	}
}

func TestLoadRejectsMaxBelowDefault(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_ENGINE_DEFAULT_NODE_TIMEOUT", "10m")
	setenv(t, "SIMPWF_ENGINE_MAX_NODE_TIMEOUT", "5m")
	if _, err := configuration.Load(configuration.WithConfigFile(missingPath(t))); err == nil {
		t.Fatal("Load() error = nil, want error when max < default")
	}
}

func TestLoadBrokerDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.Redis.DSN != "" {
		t.Errorf("redis dsn = %q, want empty (optional)", cfg.Infra.Redis.DSN)
	}
	if cfg.Infra.RabbitMQ.DSN != "" {
		t.Errorf("rabbitmq dsn = %q, want empty (optional)", cfg.Infra.RabbitMQ.DSN)
	}
	if cfg.Infra.RabbitMQ.InputQueue != "simpwf.input" {
		t.Errorf("input_queue = %q, want simpwf.input", cfg.Infra.RabbitMQ.InputQueue)
	}
	if cfg.Infra.RabbitMQ.OutputQueue != "simpwf.output" {
		t.Errorf("output_queue = %q, want simpwf.output", cfg.Infra.RabbitMQ.OutputQueue)
	}
	if cfg.Infra.RabbitMQ.StatusQueue != "simpwf.status" {
		t.Errorf("status_queue = %q, want simpwf.status", cfg.Infra.RabbitMQ.StatusQueue)
	}
}

func TestLoadBrokersFromEnv(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_INFRA_REDIS_DSN", "redis://localhost:6379/0")
	setenv(t, "SIMPWF_INFRA_RABBITMQ_DSN", "amqp://simpwf:simpwf@localhost:5672/")
	setenv(t, "SIMPWF_INFRA_RABBITMQ_INPUT_QUEUE", "workflow.input")
	setenv(t, "SIMPWF_INFRA_RABBITMQ_OUTPUT_QUEUE", "workflow.output")
	setenv(t, "SIMPWF_INFRA_RABBITMQ_STATUS_QUEUE", "workflow.status")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.Redis.DSN != "redis://localhost:6379/0" {
		t.Errorf("redis dsn = %q, want redis://localhost:6379/0", cfg.Infra.Redis.DSN)
	}
	if cfg.Infra.RabbitMQ.DSN != "amqp://simpwf:simpwf@localhost:5672/" {
		t.Errorf("rabbitmq dsn = %q, want amqp://...", cfg.Infra.RabbitMQ.DSN)
	}
	if cfg.Infra.RabbitMQ.InputQueue != "workflow.input" || cfg.Infra.RabbitMQ.OutputQueue != "workflow.output" || cfg.Infra.RabbitMQ.StatusQueue != "workflow.status" {
		t.Errorf("rabbit queues = %+v, want explicit values", cfg.Infra.RabbitMQ)
	}
}

func TestLoadBrokersFromConfigFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
  redis:
    dsn: "redis://file-redis:6379/0"
  rabbitmq:
    dsn: "amqp://file-rabbit:5672/"
`)
	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.Redis.DSN != "redis://file-redis:6379/0" {
		t.Errorf("redis dsn = %q, want file value", cfg.Infra.Redis.DSN)
	}
	if cfg.Infra.RabbitMQ.DSN != "amqp://file-rabbit:5672/" {
		t.Errorf("rabbitmq dsn = %q, want file value", cfg.Infra.RabbitMQ.DSN)
	}
	// Queue defaults still apply when the file omits them.
	if cfg.Infra.RabbitMQ.InputQueue != "simpwf.input" {
		t.Errorf("input_queue = %q, want default simpwf.input", cfg.Infra.RabbitMQ.InputQueue)
	}
}

func TestLoadRejectsRabbitDSNWithoutQueues(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
  rabbitmq:
    dsn: "amqp://localhost:5672/"
    input_queue: ""
`)
	_, err := configuration.Load(configuration.WithConfigFile(path))
	if err == nil {
		t.Fatal("Load() error = nil, want error for rabbit dsn with empty input_queue")
	}
	if !strings.Contains(err.Error(), "input_queue") {
		t.Errorf("error = %q, want mention of input_queue", err)
	}
}
