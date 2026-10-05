package configuration_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/pkg/configuration"
	"github.com/simpwf/workflow-engine/pkg/ids"
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

func TestLoadEnvOverridesConfigFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  http:
    host: "file-host:1234"
    swagger_enabled: false
  postgresql:
    dsn: "file-dsn"
worker:
  pool:
    size: 4
`)
	setenv(t, "SIMPWF_INFRA_HTTP_HOST", "env-host:5678")
	setenv(t, "SIMPWF_WORKER_POOL_SIZE", "7")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.HTTP.Host != "env-host:5678" {
		t.Errorf("host = %q, want env-host:5678 (env must take priority over file)", cfg.Infra.HTTP.Host)
	}
	if cfg.Worker.Pool.Size != 7 {
		t.Errorf("worker.pool.size = %d, want 7 from environment", cfg.Worker.Pool.Size)
	}
	if cfg.Infra.PostgreSQL.DSN != "file-dsn" {
		t.Errorf("dsn = %q, want file-dsn (file backs keys absent from env)", cfg.Infra.PostgreSQL.DSN)
	}
	if cfg.Infra.HTTP.SwaggerEnabled {
		t.Error("swagger_enabled = true, want false from config file")
	}
}

// Keys the file omits still resolve from the environment when a file is
// present. Under the old file-XOR-env switch these fell back to defaults;
// the merge reads them from env.
func TestLoadEnvSuppliesKeysMissingFromFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
`)
	setenv(t, "SIMPWF_WORKER_POOL_SIZE", "7")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Worker.Pool.Size != 7 {
		t.Errorf("worker.pool.size = %d, want 7 from environment", cfg.Worker.Pool.Size)
	}
}

// The SIMPWF_API_TOKEN alias keeps working with a file present: the
// explicit binding wins over the file value.
func TestLoadAPITokenAliasOverridesFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
auth:
  enabled: true
  api_token: "file-token"
`)
	setenv(t, "SIMPWF_API_TOKEN", "env-token")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Auth.APIToken != "env-token" {
		t.Errorf("auth.api_token = %q, want env-token from SIMPWF_API_TOKEN", cfg.Auth.APIToken)
	}
}

// A set-but-empty variable is treated as unset (Viper's default), so the
// file value stands. This pins the behavior operators get when a compose
// file interpolates an unset variable to "".
func TestLoadEmptyEnvFallsBackToFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  http:
    host: "file-host:1234"
  postgresql:
    dsn: "file-dsn"
`)
	setenv(t, "SIMPWF_INFRA_HTTP_HOST", "")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Infra.HTTP.Host != "file-host:1234" {
		t.Errorf("host = %q, want file-host:1234 (empty env must fall back to file)", cfg.Infra.HTTP.Host)
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
}

func TestLoadDispatcherTuningDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Worker.StatusPool.Size != 20 {
		t.Errorf("worker.status_pool.size = %d, want 20", cfg.Worker.StatusPool.Size)
	}
	if cfg.Engine.PollInterval != 200*time.Millisecond {
		t.Errorf("engine.poll_interval = %v, want 200ms", cfg.Engine.PollInterval)
	}
	if cfg.Engine.HeartbeatInterval != 5*time.Second {
		t.Errorf("engine.heartbeat_interval = %v, want 5s", cfg.Engine.HeartbeatInterval)
	}
}

func TestLoadDispatcherTuningFromEnv(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_WORKER_STATUS_POOL_SIZE", "7")
	setenv(t, "SIMPWF_ENGINE_POLL_INTERVAL", "50ms")
	setenv(t, "SIMPWF_ENGINE_HEARTBEAT_INTERVAL", "9s")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Worker.StatusPool.Size != 7 {
		t.Errorf("worker.status_pool.size = %d, want 7", cfg.Worker.StatusPool.Size)
	}
	if cfg.Engine.PollInterval != 50*time.Millisecond {
		t.Errorf("engine.poll_interval = %v, want 50ms", cfg.Engine.PollInterval)
	}
	if cfg.Engine.HeartbeatInterval != 9*time.Second {
		t.Errorf("engine.heartbeat_interval = %v, want 9s", cfg.Engine.HeartbeatInterval)
	}
}

func TestLoadRejectsNonPositiveDispatcherTuning(t *testing.T) {
	cases := []struct {
		env    string
		name   string
		values []string
	}{
		{"SIMPWF_WORKER_STATUS_POOL_SIZE", "worker.status_pool.size", []string{"0", "-2"}},
		{"SIMPWF_ENGINE_POLL_INTERVAL", "engine.poll_interval", []string{"0", "-5s"}},
		{"SIMPWF_ENGINE_HEARTBEAT_INTERVAL", "engine.heartbeat_interval", []string{"0", "-5s"}},
	}
	for _, tc := range cases {
		for _, value := range tc.values {
			t.Run(tc.env+"="+value, func(t *testing.T) {
				setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
				setenv(t, tc.env, value)
				if _, err := configuration.Load(configuration.WithConfigFile(missingPath(t))); err == nil {
					t.Errorf("%s = %q: error = nil, want a rejection", tc.env, value)
				} else if !strings.Contains(err.Error(), tc.name) {
					t.Errorf("%s = %q: error = %q, want mention of %s", tc.env, value, err, tc.name)
				}
			})
		}
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
	setenv(t, "SIMPWF_ENGINE_POLL_INTERVAL", "not-a-duration")

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

func TestLoadLeanContextEnvDefaultOverridesConfigFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  lean_context_default: false
`)
	setenv(t, "SIMPWF_ENGINE_LEAN_CONTEXT_DEFAULT", "true")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Engine.LeanContextDefault {
		t.Error("lean_context_default = false, want true from environment")
	}
	if cfg.Engine.LeanAnchorEvery != 20 {
		t.Errorf("lean_anchor_every = %d, want 20", cfg.Engine.LeanAnchorEvery)
	}
	if cfg.Engine.LeanReplayMax != 500 {
		t.Errorf("lean_replay_max = %d, want 500", cfg.Engine.LeanReplayMax)
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

func TestLoadEnvDenyOverridesFromEnvCommaSeparated(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_ENGINE_ENV_DENY_EXTRA", "SIMPWF_OPENROUTER*,SIMPWF_FOO")
	setenv(t, "SIMPWF_ENGINE_ENV_ALLOW_EXCEPTIONS", "SIMPWF_OPENROUTER_MODEL")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !slices.Equal(cfg.Engine.EnvDenyExtra, []string{"SIMPWF_OPENROUTER*", "SIMPWF_FOO"}) {
		t.Errorf("env_deny_extra = %q", cfg.Engine.EnvDenyExtra)
	}
	if !slices.Equal(cfg.Engine.EnvAllowExceptions, []string{"SIMPWF_OPENROUTER_MODEL"}) {
		t.Errorf("env_allow_exceptions = %q", cfg.Engine.EnvAllowExceptions)
	}
}

func TestLoadRejectsInvalidEnvDenyGlob(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_ENGINE_ENV_DENY_EXTRA", "SIMPWF_[")
	if _, err := configuration.Load(configuration.WithConfigFile(missingPath(t))); err == nil {
		t.Fatal("Load() error = nil, want error for malformed glob")
	}
	setenv(t, "SIMPWF_ENGINE_ENV_DENY_EXTRA", "")
	setenv(t, "SIMPWF_ENGINE_ENV_ALLOW_EXCEPTIONS", "SIMPWF_*[")
	if _, err := configuration.Load(configuration.WithConfigFile(missingPath(t))); err == nil {
		t.Fatal("Load() error = nil, want error for malformed allow glob")
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

// The system user is the audit actor the service bypasses are recorded as,
// so a value that is not a canonical uuid has to fail the boot rather than
// the first foreign key.
//
// An empty value is not in this table: Viper substitutes the default before
// validate runs, so a blank user id in the environment is the shipped
// default rather than a misconfiguration. It is TestShippedConfigUsesAUUID
// that pins the fallback.
func TestLoadRejectsNonUUIDSystemUserID(t *testing.T) {
	for _, id := range []string{"system", "not-a-uuid", "  ", "00000000-0000-7000-8000-00000000000", "00000000000070008000000000000001"} {
		setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
		setenv(t, "SIMPWF_SYSTEM_USER_ID", id)
		_, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
		if err == nil {
			t.Errorf("system.user_id = %q: error = nil, want a rejection", id)
			continue
		}
		if !strings.Contains(err.Error(), "system.user_id") {
			t.Errorf("system.user_id = %q: error = %q, want mention of system.user_id", id, err)
		}
	}
}

// A valid system user id still loads.
func TestLoadAcceptsUUIDSystemUserID(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_SYSTEM_USER_ID", "00000000-0000-7000-8000-000000000001")
	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.System.UserID != "00000000-0000-7000-8000-000000000001" {
		t.Errorf("system.user_id = %q, want the configured uuid", cfg.System.UserID)
	}
}

// The validation is new, so the default the code falls back to has to
// satisfy it: a deployment that never sets system.user_id must still boot.
func TestShippedConfigUsesAUUIDSystemUserByDefault(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_SYSTEM_USER_ID", "")
	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !ids.Valid(cfg.System.UserID) {
		t.Errorf("default system.user_id = %q, want a canonical uuid", cfg.System.UserID)
	}
}

// The shipped config.yaml and the e2e config both name a system user, so
// the new validation must not reject either. config.e2e-oidc.yaml is started
// by `task e2e-oidc`, which exports the API token the same way.
func TestShippedConfigsPassSystemUserValidation(t *testing.T) {
	setenv(t, "SIMPWF_API_TOKEN", "e2e-service-token")
	for _, name := range []string{"config.yaml", "config.e2e-oidc.yaml"} {
		path := filepath.Join("..", "..", name)
		if _, err := os.Stat(path); err != nil {
			t.Skipf("%s not available: %v", name, err)
		}
		if _, err := configuration.Load(configuration.WithConfigFile(path)); err != nil {
			t.Errorf("Load(%s) error = %v", name, err)
		}
	}
}

// The shipped config.yaml documents clock_skew above the cache_ttl it belongs
// to. A comment describing a key that is not there reads as if the tolerance
// is set, so the file is loaded here to keep the key and its comment together.
func TestShippedConfigParsesClockSkew(t *testing.T) {
	path := filepath.Join("..", "..", "config.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("shipped config not available: %v", err)
	}
	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load(%s) error = %v", path, err)
	}
	if cfg.Auth.OIDC.ClockSkew != time.Minute {
		t.Errorf("auth.oidc.clock_skew = %v, want 1m", cfg.Auth.OIDC.ClockSkew)
	}
}

// The role catalog is a map, which cannot ride Viper's AutomaticEnv, so it
// is exposed through an explicit post-unmarshal JSON override.
func TestLoadRolePermissionsFromEnvJSON(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_AUTH_ROLE_PERMISSIONS", `{"admin":["definitions:read","roles:read"],"finance":["instances:read"]}`)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.Auth.RolePermissions["admin"]; !slices.Equal(got, []string{"definitions:read", "roles:read"}) {
		t.Errorf("admin = %v, want [definitions:read roles:read]", got)
	}
	if got := cfg.Auth.RolePermissions["finance"]; !slices.Equal(got, []string{"instances:read"}) {
		t.Errorf("finance = %v, want [instances:read]", got)
	}
}

func TestLoadRolePermissionsEnvOverridesFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
auth:
  role_permissions:
    admin: ["definitions:read"]
`)
	setenv(t, "SIMPWF_AUTH_ROLE_PERMISSIONS", `{"finance":["instances:read"]}`)

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Auth.RolePermissions) != 1 || cfg.Auth.RolePermissions["finance"] == nil {
		t.Errorf("RolePermissions = %v, want only env value {finance:...}", cfg.Auth.RolePermissions)
	}
}

func TestLoadRolePermissionsInvalidJSONFails(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_AUTH_ROLE_PERMISSIONS", `not-json`)

	_, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err == nil {
		t.Fatal("Load() error = nil, want fail-fast on invalid JSON")
	}
	if !strings.Contains(err.Error(), "SIMPWF_AUTH_ROLE_PERMISSIONS") {
		t.Errorf("error = %q, want it to name SIMPWF_AUTH_ROLE_PERMISSIONS", err.Error())
	}
}

func TestLoadRolePermissionsBlankEnvUsesFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
auth:
  role_permissions:
    admin: ["definitions:read"]
`)
	setenv(t, "SIMPWF_AUTH_ROLE_PERMISSIONS", "   ")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !slices.Equal(cfg.Auth.RolePermissions["admin"], []string{"definitions:read"}) {
		t.Errorf("admin = %v, want file value [definitions:read]", cfg.Auth.RolePermissions["admin"])
	}
}

func TestLoadSchedulerDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Scheduler.Enabled {
		t.Error("scheduler.enabled = false, want true by default")
	}
	if cfg.Scheduler.RefreshInterval != 30*time.Second {
		t.Errorf("scheduler.refresh_interval = %v, want 30s", cfg.Scheduler.RefreshInterval)
	}
}

func TestLoadSchedulerFromEnv(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_SCHEDULER_ENABLED", "false")
	setenv(t, "SIMPWF_SCHEDULER_REFRESH_INTERVAL", "10s")

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Scheduler.Enabled {
		t.Error("scheduler.enabled = true, want false from environment")
	}
	if cfg.Scheduler.RefreshInterval != 10*time.Second {
		t.Errorf("scheduler.refresh_interval = %v, want 10s", cfg.Scheduler.RefreshInterval)
	}
}

func TestLoadRejectsInvalidSchedulerRefreshInterval(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)
	setenv(t, "SIMPWF_SCHEDULER_REFRESH_INTERVAL", "0s")

	if _, err := configuration.Load(configuration.WithConfigFile(missingPath(t))); err == nil {
		t.Fatal("Load() error = nil, want validation error for refresh_interval 0")
	}
}

func TestLoadConsumerRetryDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	r := cfg.Engine.ConsumerRetry
	if r.MaxAttempts != 3 || r.InitialBackoff != time.Second || r.MaxBackoff != 30*time.Second || r.MinHealthyRun != 30*time.Second {
		t.Errorf("consumer_retry = %+v, want 3/1s/30s/30s", r)
	}
}

func TestLoadConsumerRetryFromFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  consumer_retry:
    max_attempts: 5
    initial_backoff: 2s
    max_backoff: 1m
    min_healthy_run: 45s
`)
	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	r := cfg.Engine.ConsumerRetry
	if r.MaxAttempts != 5 || r.InitialBackoff != 2*time.Second || r.MaxBackoff != time.Minute || r.MinHealthyRun != 45*time.Second {
		t.Errorf("consumer_retry = %+v, want 5/2s/1m/45s", r)
	}
}

func TestLoadConsumerRetryEnvOverridesFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  consumer_retry:
    max_attempts: 5
    initial_backoff: 2s
    max_backoff: 1m
    min_healthy_run: 45s
`)
	setenv(t, "SIMPWF_ENGINE_CONSUMER_RETRY_MAX_ATTEMPTS", "7")
	setenv(t, "SIMPWF_ENGINE_CONSUMER_RETRY_INITIAL_BACKOFF", "500ms")
	setenv(t, "SIMPWF_ENGINE_CONSUMER_RETRY_MAX_BACKOFF", "10s")
	setenv(t, "SIMPWF_ENGINE_CONSUMER_RETRY_MIN_HEALTHY_RUN", "20s")

	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	r := cfg.Engine.ConsumerRetry
	if r.MaxAttempts != 7 || r.InitialBackoff != 500*time.Millisecond || r.MaxBackoff != 10*time.Second || r.MinHealthyRun != 20*time.Second {
		t.Errorf("consumer_retry = %+v, want 7/500ms/10s/20s from environment", r)
	}
}

func TestLoadRejectsInvalidConsumerRetry(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"max_attempts", "max_attempts: 0"},
		{"initial_backoff", "initial_backoff: 0s"},
		{"max_backoff", "initial_backoff: 5s\n    max_backoff: 1s"},
		{"min_healthy_run", "min_healthy_run: 0s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  consumer_retry:
    `+tc.value+`
`)
			_, err := configuration.Load(configuration.WithConfigFile(path))
			if err == nil {
				t.Fatalf("Load() error = nil, want rejection for %s", tc.name)
			}
			if !strings.Contains(err.Error(), "configuration: ") || !strings.Contains(err.Error(), "engine.consumer_retry."+tc.name) {
				t.Errorf("error = %q, want a configuration: engine.consumer_retry.%s error", err, tc.name)
			}
		})
	}
}
