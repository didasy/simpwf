package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/contextpath"
)

// HTTPExecutor performs outbound HTTP calls under the configured target
// allowlist. Every request (including redirects) is validated against the
// allowlist and DNS, so scripts cannot exfiltrate context data. Every call
// is admitted through the shared bulkhead, so outbound concurrency stays
// bounded process-wide.
type HTTPExecutor struct {
	allowlist    []string
	maxRedirects int
	maxBody      int
	bulkhead     *HTTPBulkhead
}

type httpConfig struct {
	target  string
	method  string
	headers map[string]string
	body    []byte
}

// NewHTTPExecutor builds a standalone outbound HTTP client enforcing the
// executor security policy (scheme, allowlist, DNS, redirect revalidation).
// A nil limits.HTTPBulkhead builds a private default bulkhead from the
// configured caps, so existing constructions stay bounded with zero churn.
func NewHTTPExecutor(limits Limits) *HTTPExecutor {
	bulkhead := limits.HTTPBulkhead
	if bulkhead == nil {
		bulkhead = NewHTTPBulkhead(limits.HTTPMaxInFlight, limits.HTTPMaxInFlightPerHost)
	}
	return &HTTPExecutor{
		allowlist:    limits.HTTPAllowlist,
		maxRedirects: limits.MaxRedirects,
		maxBody:      limits.MaxOutputBytes,
		bulkhead:     bulkhead,
	}
}

func (e *HTTPExecutor) Execute(ctx context.Context, req Request) (*Result, error) {
	cfg, err := e.build(ctx, req)
	if err != nil {
		return nil, &NodeError{Node: req.Node, Reason: "http", Err: err}
	}
	headers := make(map[string]string, len(cfg.headers)+1)
	if req.IdempotencyKey != "" {
		headers["Idempotency-Key"] = req.IdempotencyKey
	}
	for k, v := range cfg.headers {
		headers[k] = v
	}
	body, status, respHeaders, err := e.Do(ctx, cfg.method, cfg.target, headers, cfg.body, nodeTimeout(req.Node))
	if err != nil {
		if errors.Is(err, ErrHTTPOverloaded) {
			return nil, &NodeError{Node: req.Node, Reason: "http-overload", Err: err}
		}
		return nil, &NodeError{Node: req.Node, Reason: "http", Err: err}
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		parsed = string(body)
	}
	outHeaders := make(map[string][]string, len(respHeaders))
	for k, v := range respHeaders {
		outHeaders[k] = v
	}
	res := &Result{
		Output: &HTTPResult{Status: status, Headers: outHeaders, Body: parsed},
	}
	if req.Node != nil && req.Node.OnFailure != nil && status >= 300 {
		return res, &NodeError{
			Node:   req.Node,
			Reason: "http-status",
			Err:    fmt.Errorf("http request failed with status %d", status),
		}
	}
	return res, nil
}

// maxHTTPRetries bounds transparent retries of 429/503 on GET/HEAD.
const maxHTTPRetries = 2

// Do performs an outbound HTTP request under the executor's security policy
// and returns the raw response body, status code, and headers. A non-nil
// body defaults the Content-Type to application/json when no header sets
// one. The timeout covers the whole request lifecycle including redirects,
// bulkhead admission, and any Retry-After backoff.
//
// Admission sheds load: when no bulkhead slot frees before the timeout, Do
// fails with ErrHTTPOverloaded instead of hanging. A 429/503 on GET/HEAD
// retries up to maxHTTPRetries times with the Retry-After delay honored
// (500ms/1s exponential when the header is absent or invalid); permits are
// released while backing off, and when the delay exceeds the remaining
// budget the last response is returned as-is. Other methods never retry
// transparently. Each caller keeps its own upstream call and response.
func (e *HTTPExecutor) Do(ctx context.Context, method, target string, headers map[string]string, body []byte, timeout time.Duration) ([]byte, int, http.Header, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, 0, nil, err
	}
	if err := e.validateTarget(parsed); err != nil {
		return nil, 0, nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := &http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= e.maxRedirects {
				return fmt.Errorf("too many redirects (max %d)", e.maxRedirects)
			}
			if err := e.validateTarget(r.URL); err != nil {
				return err
			}
			return nil
		},
	}

	release, err := e.bulkhead.Acquire(reqCtx, parsed.Host)
	if err != nil {
		return nil, 0, nil, err
	}

	upper := strings.ToUpper(strings.TrimSpace(method))
	retryable := upper == http.MethodGet || upper == http.MethodHead
	for attempt := 0; ; attempt++ {
		out, status, respHeaders, err := e.roundTrip(reqCtx, client, method, target, headers, body)
		if err != nil {
			release()
			return nil, 0, nil, err
		}
		if !retryable || (status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable) || attempt >= maxHTTPRetries {
			release()
			return out, status, respHeaders, nil
		}
		backoff := retryBackoff(respHeaders.Get("Retry-After"), attempt)
		release()
		if !sleepForRetry(reqCtx, backoff) {
			return out, status, respHeaders, nil
		}
		release, err = e.bulkhead.Acquire(reqCtx, parsed.Host)
		if err != nil {
			// The budget expired while re-admitting. The upstream 429/503
			// already in hand routes better than a shed error, so keep it.
			return out, status, respHeaders, nil
		}
	}
}

// roundTrip issues one HTTP request. The caller must hold a bulkhead slot.
func (e *HTTPExecutor) roundTrip(ctx context.Context, client *http.Client, method, target string, headers map[string]string, body []byte) ([]byte, int, http.Header, error) {
	httpReq, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, 0, nil, err
	}
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}
	if body != nil && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	out, err := io.ReadAll(cappedReader(resp.Body, e.maxBody))
	if err != nil {
		return nil, 0, nil, err
	}
	return out, resp.StatusCode, resp.Header, nil
}

// retryBackoff returns the Retry-After delay when the header parses, else
// 500ms/1s exponential by zero-based attempt.
func retryBackoff(header string, attempt int) time.Duration {
	if d, ok := parseRetryAfter(header); ok {
		return d
	}
	return 500 * time.Millisecond << attempt
}

// sleepForRetry sleeps d unless ctx expires first or d exceeds the remaining
// budget. It reports whether the caller should retry.
func sleepForRetry(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Now().Add(d).After(deadline) {
			return false
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (e *HTTPExecutor) build(ctx context.Context, req Request) (*httpConfig, error) {
	cfg := req.Node.HTTP
	return e.buildConfig(ctx, cfg.URL, cfg.Method, cfg.Headers, cfg.Body, req.Context)
}

// buildConfig renders and validates one outbound HTTP call configuration
// from the templated url/method/headers/body against the context snapshot.
// It is shared by external_call nodes and HTTP pollers so allowlist, DNS,
// redirect, body-cap, and templating policy stays in one place.
func (e *HTTPExecutor) buildConfig(ctx context.Context, target, method string, headers map[string]string, body json.RawMessage, data map[string]any) (*httpConfig, error) {
	rendered, err := contextpath.RenderTemplate(target, data)
	if err != nil {
		return nil, fmt.Errorf("render url: %w", err)
	}
	targetStr, ok := rendered.(string)
	if !ok {
		return nil, fmt.Errorf("url rendered to %T, want string", rendered)
	}
	parsed, err := url.Parse(targetStr)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("url scheme %q not allowed", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("url has no host")
	}

	methodRendered, err := contextpath.RenderTemplate(method, data)
	if err != nil {
		return nil, fmt.Errorf("render method: %w", err)
	}
	methodStr, ok := methodRendered.(string)
	if !ok {
		return nil, fmt.Errorf("method rendered to %T, want string", methodRendered)
	}
	methodStr = strings.ToUpper(strings.TrimSpace(methodStr))
	if methodStr == "" {
		return nil, errors.New("method is empty after rendering")
	}

	renderedHeaders := make(map[string]string, len(headers))
	hasContentType := false
	for k, v := range headers {
		if strings.EqualFold(k, "Content-Type") {
			if hasContentType {
				return nil, errors.New("duplicate Content-Type header")
			}
			hasContentType = true
		}
		rendered, err := contextpath.RenderTemplate(v, data)
		if err != nil {
			return nil, fmt.Errorf("render header %q: %w", k, err)
		}
		renderedHeaders[k] = fmt.Sprintf("%v", rendered)
	}

	var bodyBytes []byte
	if len(body) > 0 {
		rendered, err := contextpath.RenderJSON(body, data)
		if err != nil {
			return nil, fmt.Errorf("render body: %w", err)
		}
		bodyBytes = rendered
	}
	bodyBytes, err = encodeHTTPBody(bodyBytes, renderedHeaders)
	if err != nil {
		return nil, fmt.Errorf("encode body: %w", err)
	}
	return &httpConfig{target: targetStr, method: methodStr, headers: renderedHeaders, body: bodyBytes}, nil
}

func encodeHTTPBody(body []byte, headers map[string]string) ([]byte, error) {
	contentTypeKey, contentType := headerValue(headers, "Content-Type")
	if contentType == "" {
		return body, nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		if !isFormMediaType(contentType) {
			return body, nil
		}
		return nil, fmt.Errorf("parse Content-Type: %w", err)
	}

	switch strings.ToLower(mediaType) {
	case "application/x-www-form-urlencoded":
		fields, err := formFields(body)
		if err != nil {
			return nil, err
		}
		values := make(url.Values, len(fields))
		for _, field := range fields {
			values.Add(field.name, field.value)
		}
		return []byte(values.Encode()), nil
	case "multipart/form-data":
		fields, err := formFields(body)
		if err != nil {
			return nil, err
		}
		var encoded bytes.Buffer
		writer := multipart.NewWriter(&encoded)
		for _, field := range fields {
			if err := writer.WriteField(field.name, field.value); err != nil {
				return nil, fmt.Errorf("write multipart field %q: %w", field.name, err)
			}
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close multipart body: %w", err)
		}
		headers[contentTypeKey] = writer.FormDataContentType()
		return encoded.Bytes(), nil
	default:
		return body, nil
	}
}

func isFormMediaType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	for _, mediaType := range []string{"application/x-www-form-urlencoded", "multipart/form-data"} {
		if contentType == mediaType {
			return true
		}
		if !strings.HasPrefix(contentType, mediaType) {
			continue
		}
		switch contentType[len(mediaType)] {
		case ';', ',', ' ', '\t':
			return true
		}
	}
	return false
}

type formField struct {
	name  string
	value string
}

func formFields(body []byte) ([]formField, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("form body must render to a JSON object")
	}

	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var fields []formField
	for _, key := range keys {
		value := object[key]
		if value == nil {
			continue
		}
		if values, ok := value.([]any); ok {
			for _, item := range values {
				if item == nil {
					continue
				}
				encoded, err := formValue(item)
				if err != nil {
					return nil, fmt.Errorf("encode form field %q: %w", key, err)
				}
				fields = append(fields, formField{name: key, value: encoded})
			}
			continue
		}
		encoded, err := formValue(value)
		if err != nil {
			return nil, fmt.Errorf("encode form field %q: %w", key, err)
		}
		fields = append(fields, formField{name: key, value: encoded})
	}
	return fields, nil
}

func formValue(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case json.Number:
		return value.String(), nil
	case bool:
		return fmt.Sprintf("%v", value), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

func headerValue(headers map[string]string, name string) (string, string) {
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return key, value
		}
	}
	return name, ""
}

// validateTarget checks scheme, host:port against the allowlist and that the
// host resolves.
func (e *HTTPExecutor) validateTarget(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed", u.Scheme)
	}
	if !e.allowed(u.Hostname(), portOf(u)) {
		return fmt.Errorf("target %s not in HTTP allowlist", u.Host)
	}
	if _, err := net.LookupHost(u.Hostname()); err != nil {
		return fmt.Errorf("target %s does not resolve: %w", u.Hostname(), err)
	}
	return nil
}

// ValidateTarget applies this executor's outbound policy to an absolute
// target URL without issuing a request. Custom nodes that build their own
// HTTP client (S3/MinIO, and any future integration) must call it on their
// rendered destination first, so every outbound path the engine can take
// clears the same scheme, allowlist, and DNS checks as external_call and the
// pollers.
func (e *HTTPExecutor) ValidateTarget(target string) error {
	parsed, err := url.Parse(target)
	if err != nil {
		return err
	}
	return e.validateTarget(parsed)
}

func (e *HTTPExecutor) allowed(host, port string) bool {
	for _, entry := range e.allowlist {
		if entry == "*" {
			slog.Warn("HTTP allowlist is set to *; outbound calls are unrestricted (development only)")
			return true
		}
		if host == entry {
			return true
		}
		if entry == net.JoinHostPort(host, port) {
			return true
		}
	}
	return false
}

// cappedReader limits reads to max bytes; a non-positive max is unlimited.
func cappedReader(r io.Reader, max int) io.Reader {
	if max <= 0 {
		return r
	}
	return io.LimitReader(r, int64(max))
}

func nodeTimeout(n *model.NodeContent) time.Duration {
	if n != nil && n.Timeout > 0 {
		return n.Timeout
	}
	return 30 * time.Second
}

// portOf returns the effective port of a URL, applying scheme defaults.
func portOf(u *url.URL) string {
	if u.Port() != "" {
		return u.Port()
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}
