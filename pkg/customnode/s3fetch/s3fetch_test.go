package s3fetch_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/customnode"
	"github.com/simpwf/workflow-engine/pkg/customnode/s3fetch"
	_ "github.com/simpwf/workflow-engine/pkg/customnode/s3fetch"
)

type fakeHTTP struct {
	body   []byte
	status int
	err    error
	gotURL string
}

func (f *fakeHTTP) Do(_ context.Context, _, target string, _ map[string]string, _ []byte, _ time.Duration) ([]byte, int, http.Header, error) {
	f.gotURL = target
	return f.body, f.status, http.Header{}, f.err
}

type fakeS3 struct {
	putBucket string
	putKey    string
	putBytes  []byte
	putErr    error
	statErr   error
	presign   string
	presignEr error
}

func (f *fakeS3) Put(bucket, key string, data []byte) error {
	f.putBucket, f.putKey, f.putBytes = bucket, key, data
	return f.putErr
}

func (f *fakeS3) Stat(bucket, key string) error {
	_ = bucket
	_ = key
	return f.statErr
}

func (f *fakeS3) Presigned(bucket, key string, expiry time.Duration) (string, error) {
	_ = expiry
	return f.presign, f.presignEr
}

func s3fetchNode(t *testing.T, config string) *model.NodeContent {
	t.Helper()
	raw := `{"type":"s3fetch","config":` + config + `}`
	nc, err := model.ParseNodeContent([]byte(raw), model.NodeLimits{DefaultTimeout: 5 * time.Second, MaxTimeout: 60 * time.Second, ConditionTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("ParseNodeContent() error = %v", err)
	}
	return nc
}

func runS3Exec(nc *model.NodeContent, h *fakeHTTP, s *fakeS3) (*executor.Result, error) {
	return s3fetch.ExecuteForTest(context.Background(), executor.Request{Node: nc, Context: map[string]any{}}, h, s)
}

func outMap(t *testing.T, res *executor.Result) map[string]any {
	t.Helper()
	out, ok := res.Output.(map[string]any)
	if !ok {
		t.Fatalf("Output = %#v, want map", res.Output)
	}
	return out
}

func TestValidateAcceptPipe(t *testing.T) {
	v, ok := customnode.ByType("s3fetch")
	if !ok {
		t.Fatal("ByType(s3fetch) missing; want registered via blank import")
	}
	got, err := v.Validate(json.RawMessage(`{"operation":"pipe","endpoint":"play.min.io:9000","bucket":"b","key":"k","source_url":"https://x/y","access_key":"a","secret_key":"s"}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg, ok := got.(s3fetch.Config)
	if !ok || cfg.Operation != "pipe" || cfg.Expiry != 3600 {
		t.Fatalf("Validate() = %#v, want pipe/3600 default", got)
	}
}

func TestValidateAcceptPresign(t *testing.T) {
	v, _ := customnode.ByType("s3fetch")
	got, err := v.Validate(json.RawMessage(`{"operation":"presign","endpoint":"https://play.min.io:9000","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg := got.(s3fetch.Config)
	if cfg.Endpoint != "play.min.io:9000" {
		t.Fatalf("Endpoint = %q, want scheme stripped", cfg.Endpoint)
	}
}

func TestValidateReject(t *testing.T) {
	v, _ := customnode.ByType("s3fetch")
	for name, raw := range map[string]string{
		"missing operation":    `{"endpoint":"e","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`,
		"bad operation":        `{"operation":"fetch","endpoint":"e","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`,
		"pipe missing source":  `{"operation":"pipe","endpoint":"e","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`,
		"presign with source":  `{"operation":"presign","endpoint":"e","bucket":"b","key":"k","source_url":"https://x","access_key":"a","secret_key":"s"}`,
		"missing bucket":       `{"operation":"pipe","endpoint":"e","key":"k","source_url":"https://x","access_key":"a","secret_key":"s"}`,
		"missing key":          `{"operation":"pipe","endpoint":"e","bucket":"b","source_url":"https://x","access_key":"a","secret_key":"s"}`,
		"missing access":       `{"operation":"pipe","endpoint":"e","bucket":"b","key":"k","source_url":"https://x","secret_key":"s"}`,
		"missing secret":       `{"operation":"pipe","endpoint":"e","bucket":"b","key":"k","source_url":"https://x","access_key":"a"}`,
		"missing endpoint":     `{"operation":"pipe","bucket":"b","key":"k","source_url":"https://x","access_key":"a","secret_key":"s"}`,
		"expiry negative":      `{"operation":"pipe","endpoint":"e","bucket":"b","key":"k","source_url":"https://x","access_key":"a","secret_key":"s","expiry_seconds":-5}`,
		"expiry too big":       `{"operation":"pipe","endpoint":"e","bucket":"b","key":"k","source_url":"https://x","access_key":"a","secret_key":"s","expiry_seconds":9999999}`,
		"template shapes pass": `{"operation":"pipe","endpoint":"{{ env.SIMPWF_S3_ENDPOINT }}","bucket":"b","key":"k","source_url":"https://x","access_key":"{{ env.SIMPWF_S3_ACCESS_KEY }}","secret_key":"{{ env.SIMPWF_S3_SECRET_KEY }}"}`,
		"not object":           `[1,2]`,
		"null":                 `null`,
	} {
		_, err := v.Validate(json.RawMessage(raw))
		if name == "template shapes pass" {
			if err != nil {
				t.Errorf("%s: Validate() error = %v, want accept", name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}

func TestExecutePipe(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"pipe","endpoint":"play.min.io:9000","bucket":"b","key":"k","source_url":"https://x/y","access_key":"a","secret_key":"s"}`)
	h := &fakeHTTP{body: []byte("hello"), status: 200}
	s := &fakeS3{presign: "https://presigned/get"}
	res, err := runS3Exec(nc, h, s)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != "https://x/y" {
		t.Errorf("HTTP target = %q, want source_url", h.gotURL)
	}
	if s.putBucket != "b" || s.putKey != "k" || string(s.putBytes) != "hello" {
		t.Errorf("Put = %s/%s %q, want b/k hello", s.putBucket, s.putKey, s.putBytes)
	}
	out := outMap(t, res)
	if out["bucket"] != "b" || out["key"] != "k" || out["url"] != "https://presigned/get" {
		t.Errorf("Output = %#v, want bucket/key/url", out)
	}
	if out["size"] != int64(5) {
		t.Errorf("Output size = %#v, want 5", out["size"])
	}
	if _, err := time.Parse(time.RFC3339, out["expires_at"].(string)); err != nil {
		t.Errorf("expires_at = %v, want RFC3339: %v", out["expires_at"], err)
	}
}

func TestExecutePresign(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"presign","endpoint":"play.min.io:9000","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`)
	h := &fakeHTTP{}
	s := &fakeS3{presign: "https://presigned/get"}
	res, err := runS3Exec(nc, h, s)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if h.gotURL != "" {
		t.Errorf("HTTP called for presign, want no download")
	}
	out := outMap(t, res)
	if out["bucket"] != "b" || out["key"] != "k" || out["url"] != "https://presigned/get" {
		t.Errorf("Output = %#v, want bucket/key/url", out)
	}
	if _, ok := out["size"]; ok {
		t.Errorf("Output = %#v, want no size on presign", out)
	}
}

func TestExecutePresignMissingKey(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"presign","endpoint":"play.min.io:9000","bucket":"b","key":"k","access_key":"a","secret_key":"s"}`)
	_, err := runS3Exec(nc, &fakeHTTP{}, &fakeS3{statErr: errors.New("not found")})
	if err == nil {
		t.Fatal("Execute() = nil, want stat error")
	}
}

func TestExecuteHTTPError(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"pipe","endpoint":"play.min.io:9000","bucket":"b","key":"k","source_url":"https://x/y","access_key":"a","secret_key":"s"}`)
	_, err := runS3Exec(nc, &fakeHTTP{err: errors.New("dial refused")}, &fakeS3{})
	if err == nil {
		t.Fatal("Execute() = nil, want error")
	}
}

func TestExecuteStatusErrorWithOnFailure(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"pipe","endpoint":"play.min.io:9000","bucket":"b","key":"k","source_url":"https://x/y","access_key":"a","secret_key":"s"}`)
	nc.OnFailure = &model.FailureRoute{NextNode: "11111111-1111-7111-8111-111111111111", OutputProperty: "s3fetch_err"}
	res, err := runS3Exec(nc, &fakeHTTP{body: []byte(`oops`), status: 500}, &fakeS3{})
	if err == nil {
		t.Fatal("Execute() = nil, want status error")
	}
	if res == nil || res.Output == nil {
		t.Fatal("Execute() result missing; want partial output for routeFailure")
	}
	out := outMap(t, res)
	if out["bucket"] != "b" || out["key"] != "k" {
		t.Errorf("partial Output = %#v, want bucket/key", out)
	}
}

func TestExecutePutErrorWithOnFailure(t *testing.T) {
	nc := s3fetchNode(t, `{"operation":"pipe","endpoint":"play.min.io:9000","bucket":"b","key":"k","source_url":"https://x/y","access_key":"a","secret_key":"s"}`)
	nc.OnFailure = &model.FailureRoute{NextNode: "11111111-1111-7111-8111-111111111111", OutputProperty: "s3fetch_err"}
	res, err := runS3Exec(nc, &fakeHTTP{body: []byte("hi"), status: 200}, &fakeS3{putErr: errors.New("denied")})
	if err == nil {
		t.Fatal("Execute() = nil, want put error")
	}
	out := outMap(t, res)
	if out["bucket"] != "b" || out["key"] != "k" {
		t.Errorf("partial Output = %#v, want bucket/key", out)
	}
}
