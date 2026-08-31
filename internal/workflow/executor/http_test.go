package executor_test

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func httpNode(cfg *model.HTTPConfig) *model.NodeContent {
	return &model.NodeContent{Type: model.NodeTypeExternalCall, HTTP: cfg, Timeout: testTimeout}
}

func TestHTTPExecutorPostsTemplatedBody(t *testing.T) {
	var gotBody string
	var gotAuth string
	var gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.Header().Set("X-Reply", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node: httpNode(&model.HTTPConfig{
			URL:    srv.URL + "/submit",
			Method: "POST",
			Headers: map[string]string{
				"Authorization": "Bearer {{ user.token }}",
			},
			Body: json.RawMessage(`{"name": "{{ user.name }}", "age": "{{ user.age }}"}`),
		}),
		Context: map[string]any{
			"user": map[string]any{"token": "abc", "name": "Jono", "age": 30},
		},
		IdempotencyKey: "key-123",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	hr, ok := res.Output.(*executor.HTTPResult)
	if !ok {
		t.Fatalf("output type = %T", res.Output)
	}
	if hr.Status != http.StatusCreated {
		t.Errorf("status = %d, want 201", hr.Status)
	}
	if gotBody != `{"age":30,"name":"Jono"}` && gotBody != `{"name":"Jono","age":30}` {
		t.Errorf("body = %q", gotBody)
	}
	if gotAuth != "Bearer abc" {
		t.Errorf("auth header = %q", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if v := hr.Headers["X-Reply"]; len(v) != 1 || v[0] != "yes" {
		t.Errorf("response headers = %v", hr.Headers)
	}
	body, ok := hr.Body.(map[string]any)
	if !ok || body["ok"] != true {
		t.Errorf("parsed body = %v", hr.Body)
	}
}

func TestHTTPExecutorDynamicConfig(t *testing.T) {
	var gotPath, gotMethod, gotAuth string
	var gotBody any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	res, err := ex.Execute(context.Background(), executor.Request{
		Node: httpNode(&model.HTTPConfig{
			URL:    srv.URL + "{{ notification.path }}",
			Method: "{{ notification.method }}",
			Headers: map[string]string{
				"Authorization": "Bearer {{ notification.token }}",
			},
			Body: json.RawMessage(`{"name":"{{ notification.name }}","age":"{{ notification.age }}","admin":"{{ notification.admin }}","meta":{"tags":"{{ notification.tags }}"}}`),
		}),
		Context: map[string]any{
			"notification": map[string]any{
				"path":   "/notify",
				"method": "post",
				"token":  "sec-123",
				"name":   "Jono",
				"age":    30,
				"admin":  true,
				"tags":   []any{"a", "b"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if hr := res.Output.(*executor.HTTPResult); hr.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", hr.Status)
	}
	if gotPath != "/notify" {
		t.Errorf("path = %q, want /notify", gotPath)
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotAuth != "Bearer sec-123" {
		t.Errorf("auth = %q", gotAuth)
	}
	m, ok := gotBody.(map[string]any)
	if !ok {
		t.Fatalf("body = %#v, want object", gotBody)
	}
	if m["name"] != "Jono" {
		t.Errorf("name = %v", m["name"])
	}
	if v, ok := m["age"].(float64); !ok || v != 30 {
		t.Errorf("age = %v", m["age"])
	}
	if m["admin"] != true {
		t.Errorf("admin = %v", m["admin"])
	}
	meta, ok := m["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta = %#v, want object", m["meta"])
	}
	tags, ok := meta["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("meta.tags = %v", meta["tags"])
	}
}

func TestHTTPExecutorEncodesURLEncodedBody(t *testing.T) {
	const contentType = "Application/X-Www-Form-Urlencoded; charset=utf-8"
	var gotContentType string
	var gotForm url.Values
	var parseErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		parseErr = r.ParseForm()
		gotForm = r.PostForm
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: httpNode(&model.HTTPConfig{
			URL:     srv.URL + "/submit",
			Method:  "POST",
			Headers: map[string]string{"Content-Type": contentType},
			Body: json.RawMessage(`{
				"name":"{{ form.name }}",
				"age":"{{ form.age }}",
				"active":"{{ form.active }}",
				"tags":"{{ form.tags }}",
				"meta":"{{ form.meta }}",
				"nullable":"{{ form.nullable }}",
				"large_id":9007199254740993
			}`),
		}),
		Context: map[string]any{
			"form": map[string]any{
				"name":     "Jono Doe",
				"age":      30,
				"active":   true,
				"tags":     []any{"red", "blue"},
				"meta":     map[string]any{"role": "admin", "score": 2},
				"nullable": nil,
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if parseErr != nil {
		t.Fatalf("ParseForm() error = %v", parseErr)
	}
	if gotContentType != contentType {
		t.Errorf("Content-Type = %q, want %q", gotContentType, contentType)
	}
	if got := gotForm.Get("name"); got != "Jono Doe" {
		t.Errorf("name = %q", got)
	}
	if got := gotForm.Get("age"); got != "30" {
		t.Errorf("age = %q", got)
	}
	if got := gotForm.Get("active"); got != "true" {
		t.Errorf("active = %q", got)
	}
	if got := gotForm["tags"]; len(got) != 2 || got[0] != "red" || got[1] != "blue" {
		t.Errorf("tags = %v", got)
	}
	if got := gotForm.Get("meta"); got != `{"role":"admin","score":2}` {
		t.Errorf("meta = %q", got)
	}
	if got := gotForm.Get("large_id"); got != "9007199254740993" {
		t.Errorf("large_id = %q", got)
	}
	if _, ok := gotForm["nullable"]; ok {
		t.Errorf("nullable field present: %v", gotForm["nullable"])
	}
}

func TestHTTPExecutorEncodesMultipartBody(t *testing.T) {
	var gotMediaType string
	var gotBoundary string
	var gotForm map[string][]string
	var gotFileCount int
	var parseErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params map[string]string
		gotMediaType, params, parseErr = mime.ParseMediaType(r.Header.Get("Content-Type"))
		if parseErr == nil {
			gotBoundary = params["boundary"]
			parseErr = r.ParseMultipartForm(1 << 20)
		}
		if r.MultipartForm != nil {
			gotForm = r.MultipartForm.Value
			for _, files := range r.MultipartForm.File {
				gotFileCount += len(files)
			}
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node: httpNode(&model.HTTPConfig{
			URL:     srv.URL + "/upload",
			Method:  "POST",
			Headers: map[string]string{"content-type": "multipart/form-data"},
			Body: json.RawMessage(`{
				"title":"{{ form.title }}",
				"labels":"{{ form.labels }}",
				"details":"{{ form.details }}",
				"nullable":"{{ form.nullable }}"
			}`),
		}),
		Context: map[string]any{
			"form": map[string]any{
				"title":    "Quarterly report",
				"labels":   []any{"finance", "draft"},
				"details":  map[string]any{"pages": 3},
				"nullable": nil,
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if parseErr != nil {
		t.Fatalf("parse multipart request: %v", parseErr)
	}
	if gotMediaType != "multipart/form-data" {
		t.Errorf("media type = %q", gotMediaType)
	}
	if gotBoundary == "" {
		t.Error("multipart boundary is empty")
	}
	if got := gotForm["title"]; len(got) != 1 || got[0] != "Quarterly report" {
		t.Errorf("title = %v", got)
	}
	if got := gotForm["labels"]; len(got) != 2 || got[0] != "finance" || got[1] != "draft" {
		t.Errorf("labels = %v", got)
	}
	if got := gotForm["details"]; len(got) != 1 || got[0] != `{"pages":3}` {
		t.Errorf("details = %v", got)
	}
	if _, ok := gotForm["nullable"]; ok {
		t.Errorf("nullable field present: %v", gotForm["nullable"])
	}
	if gotFileCount != 0 {
		t.Errorf("file count = %d, want 0", gotFileCount)
	}
}

func TestHTTPExecutorRejectsInvalidFormBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	cases := []struct {
		name        string
		contentType string
		headers     map[string]string
		body        json.RawMessage
		wantSub     string
	}{
		{
			name:        "urlencoded array",
			contentType: "application/x-www-form-urlencoded",
			body:        json.RawMessage(`["a","b"]`),
			wantSub:     "form body must render to a JSON object",
		},
		{
			name:        "multipart scalar",
			contentType: "multipart/form-data",
			body:        json.RawMessage(`"value"`),
			wantSub:     "form body must render to a JSON object",
		},
		{
			name:        "urlencoded missing body",
			contentType: "application/x-www-form-urlencoded",
			wantSub:     "form body must render to a JSON object",
		},
		{
			name:        "multipart missing body",
			contentType: "multipart/form-data",
			wantSub:     "form body must render to a JSON object",
		},
		{
			name:        "malformed multipart content type",
			contentType: "multipart/form-data; boundary",
			body:        json.RawMessage(`{"name":"value"}`),
			wantSub:     "parse Content-Type",
		},
		{
			name:        "malformed multipart content type without semicolon",
			contentType: "multipart/form-data boundary=x",
			body:        json.RawMessage(`{"name":"value"}`),
			wantSub:     "parse Content-Type",
		},
		{
			name:        "malformed multipart content type with comma",
			contentType: "multipart/form-data, boundary=x",
			body:        json.RawMessage(`{"name":"value"}`),
			wantSub:     "parse Content-Type",
		},
		{
			name: "duplicate content type headers",
			headers: map[string]string{
				"Content-Type": "application/json",
				"content-type": "multipart/form-data",
			},
			body:    json.RawMessage(`{"name":"value"}`),
			wantSub: "duplicate Content-Type header",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			headers := c.headers
			if headers == nil {
				headers = map[string]string{"Content-Type": c.contentType}
			}
			_, err := ex.Execute(context.Background(), executor.Request{
				Node: httpNode(&model.HTTPConfig{
					URL:     srv.URL + "/submit",
					Method:  "POST",
					Headers: headers,
					Body:    c.body,
				}),
				Context: map[string]any{},
			})
			if err == nil {
				t.Fatal("Execute() error = nil, want error")
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error = %v, want substring %q", err, c.wantSub)
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("server calls = %d, want 0", got)
	}
}

func TestHTTPExecutorPreservesUnsupportedContentType(t *testing.T) {
	for _, contentType := range []string{
		"text/plain; charset",
		"multipart/form-data-v2; charset",
	} {
		t.Run(contentType, func(t *testing.T) {
			var gotContentType string
			var gotBody string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotContentType = r.Header.Get("Content-Type")
				var err error
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("ReadAll() error = %v", err)
				}
				gotBody = string(body)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer srv.Close()

			ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
			_, err := ex.Execute(context.Background(), executor.Request{
				Node: httpNode(&model.HTTPConfig{
					URL:     srv.URL + "/submit",
					Method:  "POST",
					Headers: map[string]string{"Content-Type": contentType},
					Body:    json.RawMessage(`{"name":"{{ name }}"}`),
				}),
				Context: map[string]any{"name": "Jono"},
			})
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if gotContentType != contentType {
				t.Errorf("Content-Type = %q, want %q", gotContentType, contentType)
			}
			if gotBody != `{"name":"Jono"}` {
				t.Errorf("body = %q", gotBody)
			}
		})
	}
}

func TestHTTPExecutorRejectsBadDynamicConfig(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	cases := []struct {
		name    string
		node    *model.NodeContent
		context map[string]any
		wantSub string
	}{
		{
			name:    "missing url path",
			node:    httpNode(&model.HTTPConfig{URL: "{{ notification.url }}", Method: "GET"}),
			context: map[string]any{},
			wantSub: "render url",
		},
		{
			name:    "url renders non-string",
			node:    httpNode(&model.HTTPConfig{URL: "{{ notification.url }}", Method: "GET"}),
			context: map[string]any{"notification": map[string]any{"url": map[string]any{"x": 1}}},
			wantSub: "url rendered to",
		},
		{
			name:    "url renders invalid scheme",
			node:    httpNode(&model.HTTPConfig{URL: "{{ notification.url }}", Method: "GET"}),
			context: map[string]any{"notification": map[string]any{"url": "ftp://example.com/x"}},
			wantSub: "scheme",
		},
		{
			name:    "missing method path",
			node:    httpNode(&model.HTTPConfig{URL: "http://example.com/x", Method: "{{ notification.method }}"}),
			context: map[string]any{},
			wantSub: "render method",
		},
		{
			name:    "method renders non-string",
			node:    httpNode(&model.HTTPConfig{URL: "http://example.com/x", Method: "{{ notification.method }}"}),
			context: map[string]any{"notification": map[string]any{"method": 42}},
			wantSub: "method rendered to",
		},
		{
			name:    "method renders empty",
			node:    httpNode(&model.HTTPConfig{URL: "http://example.com/x", Method: "{{ notification.method }}"}),
			context: map[string]any{"notification": map[string]any{"method": "  "}},
			wantSub: "method is empty",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ex.Execute(context.Background(), executor.Request{Node: c.node, Context: c.context})
			if err == nil {
				t.Fatal("Execute() error = nil, want error")
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error = %v, want substring %q", err, c.wantSub)
			}
		})
	}
}

func TestHTTPExecutorRejectsNonAllowlisted(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"allowed.example.com"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    httpNode(&model.HTTPConfig{URL: "http://other.example.com/x", Method: "GET"}),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute() error = nil, want allowlist rejection")
	}
}

func TestHTTPExecutorRejectsBadScheme(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    httpNode(&model.HTTPConfig{URL: "ftp://example.com/x", Method: "GET"}),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute() error = nil, want scheme rejection")
	}
}

func TestHTTPExecutorRejectsUnknownHostDNS(t *testing.T) {
	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	_, err := ex.Execute(context.Background(), executor.Request{
		Node:    httpNode(&model.HTTPConfig{URL: "http://no-such-host-simpwf.invalid/x", Method: "GET"}),
		Context: map[string]any{},
	})
	if err == nil {
		t.Error("Execute() error = nil, want DNS failure")
	}
}

func TestHTTPExecutorRedirectValidated(t *testing.T) {
	// target redirects to a host outside the allowlist -> must fail
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("no"))
	}))
	defer blocked.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, blocked.URL, http.StatusFound)
	}))
	defer redirector.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	// unrestricted allowlist: both hosts allowed, so redirect succeeds
	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    httpNode(&model.HTTPConfig{URL: redirector.URL, Method: "GET"}),
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() with wildcard error = %v", err)
	}
	if hr := res.Output.(*executor.HTTPResult); hr.Status != http.StatusOK {
		t.Errorf("redirect status = %d, want 200", hr.Status)
	}

	// now restrict: redirect target host rejected
	ex = executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"example.invalid"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	if _, err := ex.Execute(context.Background(), executor.Request{
		Node:    httpNode(&model.HTTPConfig{URL: redirector.URL, Method: "GET"}),
		Context: map[string]any{},
	}); err == nil {
		t.Error("Execute() with blocked redirect error = nil, want rejection")
	}
}

func TestHTTPExecutorStatusFailureWithOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "val")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal fault"}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	node := httpNode(&model.HTTPConfig{URL: srv.URL, Method: "GET"})
	node.OnFailure = &model.FailureRoute{
		NextNode:       "11111111-1111-7111-8111-111111111111",
		OutputProperty: "ext_error",
	}

	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    node,
		Context: map[string]any{},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want NodeError for status >= 300 with on_failure")
	}
	nodeErr, ok := err.(*executor.NodeError)
	if !ok {
		t.Fatalf("error type = %T, want *executor.NodeError", err)
	}
	if nodeErr.Reason != "http-status" {
		t.Errorf("nodeErr.Reason = %q, want 'http-status'", nodeErr.Reason)
	}
	if res == nil {
		t.Fatal("res = nil, want normalized HTTPResult preserved on status failure with on_failure")
	}
	hr, ok := res.Output.(*executor.HTTPResult)
	if !ok {
		t.Fatalf("output type = %T, want *executor.HTTPResult", res.Output)
	}
	if hr.Status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", hr.Status)
	}
	if hr.Headers["X-Custom"][0] != "val" {
		t.Errorf("headers = %v", hr.Headers)
	}
	body, ok := hr.Body.(map[string]any)
	if !ok || body["error"] != "internal fault" {
		t.Errorf("body = %v", hr.Body)
	}
}

func TestHTTPExecutorStatusSuccessWithoutOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal fault"}`))
	}))
	defer srv.Close()

	ex := executor.NewExecutors(executor.Limits{HTTPAllowlist: []string{"*"}, MaxRedirects: 5}, nil, executor.Dependencies{})[model.NodeTypeExternalCall]
	node := httpNode(&model.HTTPConfig{URL: srv.URL, Method: "GET"})
	node.OnFailure = nil

	res, err := ex.Execute(context.Background(), executor.Request{
		Node:    node,
		Context: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil without on_failure", err)
	}
	if res == nil {
		t.Fatal("res = nil, want HTTPResult")
	}
	hr, ok := res.Output.(*executor.HTTPResult)
	if !ok || hr.Status != http.StatusInternalServerError {
		t.Errorf("output = %+v, want status 500", res.Output)
	}
}
