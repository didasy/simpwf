// Package s3fetch is the bundled example custom node: it pipes a URL into
// S3-compatible storage (pipe) or presigns an existing key (presign).
// S3 connection fields are templated from the instance context, where
// Create snapshots SIMPWF_* process env under the reserved env root, so
// configs use shapes like {{ env.SIMPWF_S3_ENDPOINT }}.
//
// The node returns a small output object {bucket,key,url,expires_at[,size]}
// and never file bytes.
package s3fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
	"github.com/simpwf/workflow-engine/pkg/customnode"
)

// Operations supported by the s3fetch node.
const (
	OperationPipe    = "pipe"
	OperationPresign = "presign"
)

// DefaultExpirySeconds is the presigned URL lifetime when expiry_seconds
// is omitted. MaxExpirySeconds is the S3 presign ceiling (7 days).
const (
	DefaultExpirySeconds = 3600
	MaxExpirySeconds     = 604800
)

// configSchema describes the s3fetch config object. It is documentation
// for frontend form rendering: ValidateConfig stays authoritative. Values
// may carry {{ path }} templates, which are unresolved at parse time, so
// templated strings are typed as plain strings and the string shapes stay
// loose. operation drives the if/then rule mirroring the validator.
var configSchema = json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "s3fetch config",
  "description": "Pipes a URL into S3-compatible storage (pipe) or presigns an existing key (presign).",
  "type": "object",
  "properties": {
    "operation": {
      "type": "string",
      "enum": ["pipe", "presign"],
      "description": "pipe uploads source_url under key; presign only presigns an existing key."
    },
    "endpoint": { "type": "string", "minLength": 1, "description": "Bare host:port (an http(s):// prefix is tolerated). Templated, e.g. {{ env.SIMPWF_S3_ENDPOINT }}." },
    "region": { "type": "string", "default": "us-east-1" },
    "bucket": { "type": "string", "minLength": 1, "description": "Templated, e.g. {{ env.SIMPWF_S3_BUCKET }}." },
    "key": { "type": "string", "minLength": 1, "description": "Templated object key." },
    "source_url": { "type": "string", "minLength": 1, "description": "Templated URL downloaded and uploaded. Required for pipe, forbidden for presign." },
    "expiry_seconds": { "type": "integer", "minimum": 1, "maximum": 604800, "default": 3600, "description": "Presigned URL lifetime; the S3 ceiling is 7 days." },
    "use_ssl": { "type": "boolean", "default": true },
    "access_key": { "type": "string", "minLength": 1, "description": "Templated, e.g. {{ env.SIMPWF_S3_ACCESS_KEY }}." },
    "secret_key": { "type": "string", "minLength": 1, "description": "Templated, e.g. {{ env.SIMPWF_S3_SECRET_KEY }}." }
  },
  "required": ["operation", "endpoint", "bucket", "key", "access_key", "secret_key"],
  "allOf": [
    {
      "if": { "type": "object", "required": ["operation"], "properties": { "operation": { "const": "pipe" } } },
      "then": { "required": ["source_url"] }
    },
    {
      "if": { "type": "object", "required": ["operation"], "properties": { "operation": { "const": "presign" } } },
      "then": { "not": { "required": ["source_url"] } }
    }
  ]
}`)

func init() {
	customnode.MustRegister(customnode.Definition{
		Type:     "s3fetch",
		Validate: ValidateConfig,
		Schema:   configSchema,
		New: func(d customnode.Deps) (executor.Executor, error) {
			return &Executor{http: d.HTTP, newClient: newMinioClient, maxBytes: d.Limits.MaxOutputBytes}, nil
		},
	})
}

// Config is the validated form of the s3fetch node config object.
type Config struct {
	Operation string `json:"operation"`                // pipe | presign
	Endpoint  string `json:"endpoint"`                 // bare host:port, templated
	Region    string `json:"region"`                   // default us-east-1
	Bucket    string `json:"bucket"`                   // templated
	Key       string `json:"key"`                      // templated
	SourceURL string `json:"source_url,omitempty"`     // pipe only, templated
	Expiry    int    `json:"expiry_seconds,omitempty"` // default 3600, clamp 1..604800
	UseSSL    *bool  `json:"use_ssl,omitempty"`        // default true
	AccessKey string `json:"access_key"`               // templated, e.g. {{ env.SIMPWF_S3_ACCESS_KEY }}
	SecretKey string `json:"secret_key"`
}

// ValidateConfig owns the s3fetch schema. It checks raw presence only:
// templates are unresolved at parse, so values containing {{ }} shapes
// (e.g. {{ env.SIMPWF_S3_ACCESS_KEY }}) are accepted here and resolved
// at runtime against the instance context.
func ValidateConfig(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, fmt.Errorf("config is required")
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("config must be an object: %w", err)
	}
	if c.Operation != OperationPipe && c.Operation != OperationPresign {
		return nil, fmt.Errorf("config.operation must be pipe or presign")
	}
	if strings.TrimSpace(c.Endpoint) == "" {
		return nil, fmt.Errorf("config.endpoint is required")
	}
	if strings.TrimSpace(c.Bucket) == "" {
		return nil, fmt.Errorf("config.bucket is required")
	}
	if strings.TrimSpace(c.Key) == "" {
		return nil, fmt.Errorf("config.key is required")
	}
	if strings.TrimSpace(c.AccessKey) == "" {
		return nil, fmt.Errorf("config.access_key is required")
	}
	if strings.TrimSpace(c.SecretKey) == "" {
		return nil, fmt.Errorf("config.secret_key is required")
	}
	if c.Operation == OperationPipe {
		if strings.TrimSpace(c.SourceURL) == "" {
			return nil, fmt.Errorf("config.source_url is required for operation pipe")
		}
	} else if strings.TrimSpace(c.SourceURL) != "" {
		return nil, fmt.Errorf("config.source_url is forbidden for operation presign")
	}
	if c.Expiry < 0 || c.Expiry > MaxExpirySeconds {
		return nil, fmt.Errorf("config.expiry_seconds must be 1..%d", MaxExpirySeconds)
	}
	c.Endpoint = stripScheme(c.Endpoint)
	if c.Expiry == 0 {
		c.Expiry = DefaultExpirySeconds
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	return c, nil
}

// stripScheme tolerates http(s):// prefixes on endpoint; S3 clients want
// the bare host:port.
func stripScheme(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return endpoint
}

// httpDo is the subset of HTTPExecutor the s3fetch node needs. It is an
// interface (not *executor.HTTPExecutor) so unit tests can fake it.
type httpDo interface {
	Do(ctx context.Context, method, target string, headers map[string]string, body []byte, timeout time.Duration) ([]byte, int, http.Header, error)
}

// S3Client is the subset of the minio client the node needs, so unit tests
// can fake S3 without network. Exported for the external test package.
type S3Client interface {
	Put(bucket, key string, data []byte) error
	Stat(bucket, key string) error
	Presigned(bucket, key string, expiry time.Duration) (string, error)
}

// minioClient adapts *minio.Client to S3Client.
type minioClient struct {
	c *minio.Client
}

func (m *minioClient) Put(bucket, key string, data []byte) error {
	_, err := m.c.PutObject(context.Background(), bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	return err
}

func (m *minioClient) Stat(bucket, key string) error {
	_, err := m.c.StatObject(context.Background(), bucket, key, minio.StatObjectOptions{})
	return err
}

func (m *minioClient) Presigned(bucket, key string, expiry time.Duration) (string, error) {
	u, err := m.c.PresignedGetObject(context.Background(), bucket, key, expiry, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// newClientFunc builds an S3Client for the rendered connection fields.
type newClientFunc func(endpoint, accessKey, secretKey, region string, useSSL bool) (S3Client, error)

func newMinioClient(endpoint, accessKey, secretKey, region string, useSSL bool) (S3Client, error) {
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
		Region: region,
	})
	if err != nil {
		return nil, err
	}
	return &minioClient{c: c}, nil
}

// Executor pipes a URL into S3 (pipe) or presigns an existing key
// (presign), using the shared HTTP client for downloads so the allowlist
// and output-cap policies apply unchanged.
type Executor struct {
	http      httpDo
	newClient newClientFunc
	maxBytes  int
}

// NewForTest builds an Executor with fakes. Tests only.
func NewForTest(h httpDo, newClient newClientFunc, maxBytes int) *Executor {
	return &Executor{http: h, newClient: newClient, maxBytes: maxBytes}
}

// ExecuteForTest runs Execute with fakes. Tests only; it keeps the http
// iface unexported while letting the external test package drive the
// executor with an S3Client fake.
func ExecuteForTest(ctx context.Context, req executor.Request, h httpDo, s S3Client) (*executor.Result, error) {
	return (&Executor{http: h, newClient: func(string, string, string, string, bool) (S3Client, error) {
		return s, nil
	}}).Execute(ctx, req)
}

// renderField renders one templated string field against the request
// context and requires a non-blank string result.
func renderField(tpl string, ctx map[string]any) (string, error) {
	v, err := contextpath.RenderTemplate(tpl, ctx)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("s3fetch: rendered field is blank (want non-empty string)")
	}
	return s, nil
}

// partial returns the bucket/key fragment kept on status-style failures
// when on_failure is set, so routeFailure has output to store.
func partial(bucket, key string) map[string]any {
	return map[string]any{"bucket": bucket, "key": key}
}

// fail wraps err as a NodeError, attaching partial output when on_failure
// is set (routeFailure path); otherwise the error stands alone.
func fail(req executor.Request, reason string, err error, bucket, key string) (*executor.Result, error) {
	nodeErr := &executor.NodeError{Node: req.Node, Reason: reason, Err: err}
	if req.Node != nil && req.Node.OnFailure != nil {
		return &executor.Result{Output: partial(bucket, key)}, nodeErr
	}
	return nil, nodeErr
}

// Execute renders the config against req.Context, downloads (pipe) via the
// shared HTTP client, uploads/presigns via minio, and returns a small
// output object — never file bytes.
func (e *Executor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
	if req.Node == nil {
		return nil, fmt.Errorf("s3fetch: request has no node")
	}
	cfg, ok := req.Node.Custom.(Config)
	if !ok {
		return nil, fmt.Errorf("s3fetch: node custom config has unexpected type %T", req.Node.Custom)
	}
	tplCtx := req.Context
	if tplCtx == nil {
		tplCtx = map[string]any{}
	}
	endpoint, err := renderField(cfg.Endpoint, tplCtx)
	if err != nil {
		return nil, &executor.NodeError{Node: req.Node, Reason: "s3fetch", Err: fmt.Errorf("endpoint: %w", err)}
	}
	bucket, err := renderField(cfg.Bucket, tplCtx)
	if err != nil {
		return nil, &executor.NodeError{Node: req.Node, Reason: "s3fetch", Err: fmt.Errorf("bucket: %w", err)}
	}
	key, err := renderField(cfg.Key, tplCtx)
	if err != nil {
		return nil, &executor.NodeError{Node: req.Node, Reason: "s3fetch", Err: fmt.Errorf("key: %w", err)}
	}
	accessKey, err := renderField(cfg.AccessKey, tplCtx)
	if err != nil {
		return nil, &executor.NodeError{Node: req.Node, Reason: "s3fetch", Err: fmt.Errorf("access_key: %w", err)}
	}
	secretKey, err := renderField(cfg.SecretKey, tplCtx)
	if err != nil {
		return nil, &executor.NodeError{Node: req.Node, Reason: "s3fetch", Err: fmt.Errorf("secret_key: %w", err)}
	}
	useSSL := true
	if cfg.UseSSL != nil {
		useSSL = *cfg.UseSSL
	}
	newClient := e.newClient
	if newClient == nil {
		newClient = newMinioClient
	}
	client, err := newClient(stripScheme(endpoint), accessKey, secretKey, cfg.Region, useSSL)
	if err != nil {
		return fail(req, "s3fetch", fmt.Errorf("s3 client: %w", err), bucket, key)
	}
	expiry := time.Duration(cfg.Expiry) * time.Second
	if expiry <= 0 {
		expiry = time.Duration(DefaultExpirySeconds) * time.Second
	}

	if cfg.Operation == OperationPresign {
		if err := client.Stat(bucket, key); err != nil {
			return fail(req, "s3fetch", fmt.Errorf("stat: %w", err), bucket, key)
		}
		presigned, err := client.Presigned(bucket, key, expiry)
		if err != nil {
			return fail(req, "s3fetch", fmt.Errorf("presign: %w", err), bucket, key)
		}
		return &executor.Result{Output: map[string]any{
			"bucket": bucket, "key": key, "url": presigned,
			"expires_at": time.Now().UTC().Add(expiry).Format(time.RFC3339),
		}}, nil
	}

	sourceURL, err := renderField(cfg.SourceURL, tplCtx)
	if err != nil {
		return fail(req, "s3fetch", fmt.Errorf("source_url: %w", err), bucket, key)
	}
	body, status, _, err := e.http.Do(ctx, "GET", sourceURL, nil, nil, req.Node.Timeout)
	if err != nil {
		return fail(req, "s3fetch", err, bucket, key)
	}
	if status >= 300 {
		return fail(req, "s3fetch-status", fmt.Errorf("download failed with status %d", status), bucket, key)
	}
	if e.maxBytes > 0 && len(body) > e.maxBytes {
		return fail(req, "s3fetch", fmt.Errorf("download exceeds output cap (%d > %d bytes)", len(body), e.maxBytes), bucket, key)
	}
	if err := client.Put(bucket, key, body); err != nil {
		return fail(req, "s3fetch", fmt.Errorf("upload: %w", err), bucket, key)
	}
	presigned, err := client.Presigned(bucket, key, expiry)
	if err != nil {
		return fail(req, "s3fetch", fmt.Errorf("presign: %w", err), bucket, key)
	}
	return &executor.Result{Output: map[string]any{
		"bucket": bucket, "key": key, "url": presigned,
		"expires_at": time.Now().UTC().Add(expiry).Format(time.RFC3339),
		"size":       int64(len(body)),
	}}, nil
}
