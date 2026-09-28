// Package configuration loads service configuration from a YAML config file
// or environment variables, with config-file values taking priority.
//
// Precedence (highest first): config file > environment variables > defaults.
// When no config file is given or the given path does not exist, environment
// variables prefixed with SIMPWF_ are used (dots in keys become underscores).
package configuration

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/simpwf/workflow-engine/pkg/ids"
	"github.com/spf13/viper"
)

const (
	envPrefix         = "SIMPWF"
	defaultConfigFile = "config.yaml"
)

// Config is the root configuration for the service.
type Config struct {
	Infra  Infra  `mapstructure:"infra"`
	Worker Worker `mapstructure:"worker"`
	Engine Engine `mapstructure:"engine"`
	System System `mapstructure:"system"`
	Auth   Auth   `mapstructure:"auth"`
}

// Infra groups infrastructure endpoints.
type Infra struct {
	HTTP       HTTP       `mapstructure:"http"`
	PostgreSQL PostgreSQL `mapstructure:"postgresql"`
	Redis      Redis      `mapstructure:"redis"`
	RabbitMQ   RabbitMQ   `mapstructure:"rabbitmq"`
}

// HTTP holds HTTP server settings.
type HTTP struct {
	Host           string `mapstructure:"host"`
	SwaggerEnabled bool   `mapstructure:"swagger_enabled"`
}

// PostgreSQL holds the DSN for the application database.
type PostgreSQL struct {
	DSN string `mapstructure:"dsn"`
}

// Redis holds the optional Redis connection. An empty DSN disables the
// Redis input/output/status transports; a configured but unreachable DSN
// fails startup.
type Redis struct {
	DSN string `mapstructure:"dsn"`
}

// RabbitMQ holds the optional RabbitMQ connection and the three durable
// queues used for input, output, and status messages. An empty DSN disables
// the RabbitMQ transports; a configured but unreachable DSN fails startup.
type RabbitMQ struct {
	DSN         string `mapstructure:"dsn"`
	InputQueue  string `mapstructure:"input_queue"`
	OutputQueue string `mapstructure:"output_queue"`
	StatusQueue string `mapstructure:"status_queue"`
}

// Worker holds the ants worker pool configuration.
type Worker struct {
	Pool             WorkerPool    `mapstructure:"pool"`
	ExpiryDuration   time.Duration `mapstructure:"expiry_duration"`
	MaxBlockingTasks int           `mapstructure:"max_blocking_tasks"`
	PreAlloc         bool          `mapstructure:"pre_alloc"`
	NonBlocking      bool          `mapstructure:"non_blocking"`
	DisablePurge     bool          `mapstructure:"disable_purge"`
}

// WorkerPool holds the pool size.
type WorkerPool struct {
	Size int `mapstructure:"size"`
}

// Engine holds the workflow engine limits.
type Engine struct {
	DefaultNodeTimeout   time.Duration `mapstructure:"default_node_timeout"`
	MaxNodeTimeout       time.Duration `mapstructure:"max_node_timeout"`
	ConditionTimeout     time.Duration `mapstructure:"condition_timeout"`
	MaxPerNodeExecutions int           `mapstructure:"max_per_node_executions"`
	MaxTotalExecutions   int           `mapstructure:"max_total_executions"`
	LeaseDuration        time.Duration `mapstructure:"lease_duration"`
	ClaimBatchSize       int           `mapstructure:"claim_batch_size"`
	MaxOutputBytes       int           `mapstructure:"max_output_bytes"`
	MaxRedirects         int           `mapstructure:"max_redirects"`
	HTTPAllowlist        []string      `mapstructure:"http_allowlist"`
	ExecAllowlist        []string      `mapstructure:"exec_allowlist"`
	LeanContextDefault   bool          `mapstructure:"lean_context_default"`
	LeanAnchorEvery      int           `mapstructure:"lean_anchor_every"`
	LeanReplayMax        int           `mapstructure:"lean_replay_max"`
}

// System holds the configured audit actor (no auth yet).
type System struct {
	UserID string `mapstructure:"user_id"`
	Name   string `mapstructure:"name"`
	Email  string `mapstructure:"email"`
}

// Auth holds the authentication settings: the optional API token service
// principal and the optional OIDC resource-server settings.
type Auth struct {
	Enabled  bool   `mapstructure:"enabled"`
	APIToken string `mapstructure:"api_token"`
	// OIDC enables bearer-token authentication against any OIDC provider.
	// The engine keeps no sessions and no callback: the frontend runs
	// code+PKCE against the provider and calls the API with the JWT.
	OIDC OIDC `mapstructure:"oidc"`
	// RolePermissions is the static role catalog: role name to the
	// resource-action permissions it grants. At startup it upserts the
	// role and permission rows and prunes the permission rows of these
	// roles that are no longer granted; no role row is ever deleted.
	RolePermissions map[string][]string `mapstructure:"role_permissions"`
}

// OIDC holds the OIDC resource-server settings.
type OIDC struct {
	Enabled bool `mapstructure:"enabled"`
	// Issuer is the provider base URL used for discovery and for the
	// iss check.
	Issuer string `mapstructure:"issuer"`
	// ClientID is the expected aud value.
	ClientID string `mapstructure:"client_id"`
	// Audience overrides the expected aud value when the provider issues
	// an API audience distinct from the client id.
	Audience string `mapstructure:"audience"`
	// RolesClaim is the claim carrying the caller's roles. Default "roles".
	// A dotted path reads a nested claim, e.g. "realm_access.roles".
	RolesClaim string `mapstructure:"roles_claim"`
	// UsernameClaim overrides the claim used as the display name.
	// Default "name", falling back to "preferred_username" then email.
	// A dotted path reads a nested claim, e.g. "profile.display".
	UsernameClaim string `mapstructure:"username_claim"`
	// ClockSkew tolerates a small issuer/applicant clock difference on a
	// token's exp: a token that expired less than ClockSkew ago is retried
	// against a widened window. It is not applied to nbf, so a token issued
	// ahead of this clock is still rejected. Default 1m.
	ClockSkew time.Duration `mapstructure:"clock_skew"`
	// CacheTTL is how long a fetched discovery document and JWKS stay
	// fresh before they are refetched. Default 5m.
	CacheTTL time.Duration `mapstructure:"cache_ttl"`
}

// Option customizes Load behavior.
type Option func(*loader)

type loader struct {
	configFile string
}

// WithConfigFile points Load at an explicit config file. An empty path means
// the default "config.yaml" in the working directory.
func WithConfigFile(path string) Option {
	return func(l *loader) { l.configFile = path }
}

// Load builds a validated Config.
func Load(opts ...Option) (*Config, error) {
	l := loader{}
	for _, opt := range opts {
		opt(&l)
	}

	path := l.configFile
	if path == "" {
		path = defaultConfigFile
	}

	v := viper.New()
	setDefaults(v)
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	// auth.api_token is exposed as SIMPWF_API_TOKEN (not
	// SIMPWF_AUTH_API_TOKEN) to match the requested env contract.
	_ = v.BindEnv("auth.api_token", "SIMPWF_API_TOKEN")
	// auth.enabled is bound explicitly because SIMPWF_AUTH_ENABLED is the
	// switch compose and the e2e harness use to turn authentication on
	// without rewriting config.yaml.
	_ = v.BindEnv("auth.enabled", "SIMPWF_AUTH_ENABLED")
	// auth.oidc.* is exposed as SIMPWF_AUTH_OIDC_* (the default mapping),
	// which is bound explicitly so the keys resolve without a config file.
	for _, key := range []string{
		"enabled", "issuer", "client_id", "audience",
		"roles_claim", "username_claim", "clock_skew", "cache_ttl",
	} {
		_ = v.BindEnv("auth.oidc."+key, "SIMPWF_AUTH_OIDC_"+strings.ToUpper(key))
	}
	// Lean settings are operator toggles and must override config.yaml.
	_ = v.BindEnv("engine.lean_context_default", "SIMPWF_ENGINE_LEAN_CONTEXT_DEFAULT")
	_ = v.BindEnv("engine.lean_anchor_every", "SIMPWF_ENGINE_LEAN_ANCHOR_EVERY")
	_ = v.BindEnv("engine.lean_replay_max", "SIMPWF_ENGINE_LEAN_REPLAY_MAX")

	fileRead := false
	if _, err := os.Stat(path); err == nil {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("configuration: read config file %s: %w", path, err)
		}
		fileRead = true
	}
	if !fileRead {
		v.AutomaticEnv()
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	))); err != nil {
		return nil, fmt.Errorf("configuration: unmarshal: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	// Register every env-addressable key so AutomaticEnv can find it; an empty
	// default keeps validation meaningful.
	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.api_token", "")
	v.SetDefault("auth.role_permissions", map[string][]string{})
	// OIDC stays inert until enabled; the defaults keep every key
	// env-addressable so validation stays meaningful.
	v.SetDefault("auth.oidc.enabled", false)
	v.SetDefault("auth.oidc.issuer", "")
	v.SetDefault("auth.oidc.client_id", "")
	v.SetDefault("auth.oidc.audience", "")
	v.SetDefault("auth.oidc.roles_claim", "roles")
	v.SetDefault("auth.oidc.username_claim", "name")
	v.SetDefault("auth.oidc.clock_skew", "1m")
	v.SetDefault("auth.oidc.cache_ttl", "5m")
	v.SetDefault("infra.postgresql.dsn", "")
	v.SetDefault("infra.http.host", "localhost:8080")
	v.SetDefault("infra.http.swagger_enabled", true)
	// Optional brokers: empty DSN disables the transport. Queue names carry
	// safe defaults so an enabled RabbitMQ always has valid destinations.
	v.SetDefault("infra.redis.dsn", "")
	v.SetDefault("infra.rabbitmq.dsn", "")
	v.SetDefault("infra.rabbitmq.input_queue", "simpwf.input")
	v.SetDefault("infra.rabbitmq.output_queue", "simpwf.output")
	v.SetDefault("infra.rabbitmq.status_queue", "simpwf.status")
	v.SetDefault("worker.pool.size", 1000)
	v.SetDefault("worker.expiry_duration", "5m")
	v.SetDefault("worker.max_blocking_tasks", 16)
	v.SetDefault("worker.pre_alloc", false)
	v.SetDefault("worker.non_blocking", false)
	v.SetDefault("worker.disable_purge", false)
	v.SetDefault("engine.default_node_timeout", "30s")
	v.SetDefault("engine.max_node_timeout", "5m")
	v.SetDefault("engine.condition_timeout", "5s")
	// Zero/empty defaults register the keys so AutomaticEnv can address them;
	// optional limits keep their downstream fallback behavior.
	v.SetDefault("engine.max_per_node_executions", 0)
	v.SetDefault("engine.max_total_executions", 0)
	v.SetDefault("engine.lease_duration", "0s")
	v.SetDefault("engine.claim_batch_size", 0)
	v.SetDefault("engine.max_output_bytes", 0)
	v.SetDefault("engine.max_redirects", 0)
	v.SetDefault("engine.http_allowlist", []string{})
	v.SetDefault("engine.exec_allowlist", []string{})
	v.SetDefault("engine.lean_context_default", false)
	v.SetDefault("engine.lean_anchor_every", 20)
	v.SetDefault("engine.lean_replay_max", 500)
	v.SetDefault("system.user_id", "00000000-0000-7000-8000-000000000001")
	v.SetDefault("system.name", "system")
	v.SetDefault("system.email", "system@localhost")
}

func (c *Config) validate() error {
	if strings.TrimSpace(c.Infra.PostgreSQL.DSN) == "" {
		return errors.New("configuration: infra.postgresql.dsn is required")
	}
	if c.Worker.Pool.Size <= 0 {
		return errors.New("configuration: worker.pool.size must be > 0")
	}
	if c.Engine.DefaultNodeTimeout <= 0 {
		return errors.New("configuration: engine.default_node_timeout must be > 0")
	}
	if c.Engine.MaxNodeTimeout < c.Engine.DefaultNodeTimeout {
		return errors.New("configuration: engine.max_node_timeout must be >= engine.default_node_timeout")
	}
	if c.Engine.ConditionTimeout <= 0 {
		return errors.New("configuration: engine.condition_timeout must be > 0")
	}
	if c.Engine.LeanAnchorEvery <= 0 {
		return errors.New("configuration: engine.lean_anchor_every must be > 0")
	}
	if c.Engine.LeanReplayMax <= 0 {
		return errors.New("configuration: engine.lean_replay_max must be > 0")
	}
	if strings.TrimSpace(c.Infra.RabbitMQ.DSN) != "" {
		if strings.TrimSpace(c.Infra.RabbitMQ.InputQueue) == "" {
			return errors.New("configuration: infra.rabbitmq.input_queue is required when infra.rabbitmq.dsn is set")
		}
		if strings.TrimSpace(c.Infra.RabbitMQ.OutputQueue) == "" {
			return errors.New("configuration: infra.rabbitmq.output_queue is required when infra.rabbitmq.dsn is set")
		}
		if strings.TrimSpace(c.Infra.RabbitMQ.StatusQueue) == "" {
			return errors.New("configuration: infra.rabbitmq.status_queue is required when infra.rabbitmq.dsn is set")
		}
	}
	if c.Auth.Enabled && strings.TrimSpace(c.Auth.APIToken) == "" {
		return errors.New("configuration: auth.api_token is required when auth.enabled is true")
	}
	if c.Auth.OIDC.Enabled {
		if strings.TrimSpace(c.Auth.OIDC.Issuer) == "" {
			return errors.New("configuration: auth.oidc.issuer is required when auth.oidc.enabled is true")
		}
		if strings.TrimSpace(c.Auth.OIDC.ClientID) == "" {
			return errors.New("configuration: auth.oidc.client_id is required when auth.oidc.enabled is true")
		}
		if c.Auth.OIDC.ClockSkew < 0 {
			return errors.New("configuration: auth.oidc.clock_skew must be >= 0")
		}
		if c.Auth.OIDC.CacheTTL <= 0 {
			return errors.New("configuration: auth.oidc.cache_ttl must be > 0")
		}
	}
	for role := range c.Auth.RolePermissions {
		if strings.TrimSpace(role) == "" {
			return errors.New("configuration: auth.role_permissions keys must be non-empty")
		}
	}
	// The system user is the audit actor the API-token and broker bypasses
	// are recorded as, so every created_by/updated_by written on their behalf
	// points at this uuid. A value that is blank or is not a canonical uuid
	// fails the foreign key at the first write instead, by which point the
	// deployment has already served traffic attributed to nobody.
	if !ids.Valid(c.System.UserID) {
		return fmt.Errorf("configuration: system.user_id %q must be a canonical uuid", c.System.UserID)
	}
	return nil
}
