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
}

// System holds the configured audit actor (no auth yet).
type System struct {
	UserID string `mapstructure:"user_id"`
	Name   string `mapstructure:"name"`
	Email  string `mapstructure:"email"`
}

// Auth holds the optional API token authentication settings.
type Auth struct {
	Enabled  bool   `mapstructure:"enabled"`
	APIToken string `mapstructure:"api_token"`
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

	fileRead := false
	if _, err := os.Stat(path); err == nil {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("configuration: read config file %s: %w", path, err)
		}
		fileRead = true
	}

	// Config file takes priority over environment variables: env vars are
	// consulted only when no config file was read.
	if !fileRead {
		v.SetEnvPrefix(envPrefix)
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		// auth.api_token is exposed as SIMPWF_API_TOKEN (not
		// SIMPWF_AUTH_API_TOKEN) to match the requested env contract.
		_ = v.BindEnv("auth.api_token", "SIMPWF_API_TOKEN")
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
	return nil
}
