package testdb

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"gorm.io/gorm"
)

// fakeTB records skip/fatal decisions instead of exiting, so the decision
// matrix runs DB-free without -short dependence.
type fakeTB struct {
	skipped  bool
	failed   bool
	skipMsg  string
	fatalMsg string
	cleanups int
}

var _ tb = (*fakeTB)(nil)

func (f *fakeTB) Helper()           {}
func (f *fakeTB) Cleanup(func())    { f.cleanups++ }
func (f *fakeTB) Skip(args ...any)  { f.skipped = true; f.skipMsg = fmt.Sprint(args...) }
func (f *fakeTB) Fatal(args ...any) { f.failed = true; f.fatalMsg = fmt.Sprint(args...) }

func TestResolveMatrix(t *testing.T) {
	suiteDSN := "host=db user=u password=p dbname=custom port=1 sslmode=disable"
	baseDSN := "host=localhost user=gorm password=gorm dbname=ignored port=9921 sslmode=disable"
	tests := []struct {
		name       string
		short      bool
		suite      string
		env        map[string]string
		wantAction action
		wantDSN    string
		wantMsg    []string
		notMsg     []string
	}{
		{
			name: "short unset skips loudly", short: true, suite: "engine",
			wantAction: skip, wantMsg: []string{"SHORT-MODE-SKIP", `"engine"`},
		},
		{
			name: "short ignores suite var", short: true, suite: "engine",
			env:        map[string]string{"TEST_DATABASE_DSN_ENGINE": suiteDSN},
			wantAction: skip, wantMsg: []string{"SHORT-MODE-SKIP"},
		},
		{
			name: "short ignores base var", short: true, suite: "engine",
			env:        map[string]string{"TEST_DATABASE_DSN": baseDSN},
			wantAction: skip, wantMsg: []string{"SHORT-MODE-SKIP"},
		},
		{
			name: "full unset fails naming suite vars and fix", suite: "engine",
			wantAction: fatal,
			wantMsg:    []string{`"engine"`, "TEST_DATABASE_DSN_ENGINE", "TEST_DATABASE_DSN", "simpwf_test_engine", "-short"},
		},
		{
			name: "full suite var honored verbatim", suite: "engine",
			env:        map[string]string{"TEST_DATABASE_DSN_ENGINE": suiteDSN},
			wantAction: proceed, wantDSN: suiteDSN,
		},
		{
			name: "full base only derives suite dbname", suite: "engine",
			env:        map[string]string{"TEST_DATABASE_DSN": baseDSN},
			wantAction: proceed,
			wantDSN:    "host=localhost user=gorm password=gorm dbname=simpwf_test_engine port=9921 sslmode=disable",
		},
		{
			name: "full suite var beats base", suite: "engine",
			env: map[string]string{
				"TEST_DATABASE_DSN_ENGINE": suiteDSN,
				"TEST_DATABASE_DSN":        baseDSN,
			},
			wantAction: proceed, wantDSN: suiteDSN,
		},
		{
			name: "full unknown suite fails", suite: "engin",
			env:        map[string]string{"TEST_DATABASE_DSN": baseDSN},
			wantAction: fatal, wantMsg: []string{`unknown suite "engin"`},
		},
		{
			name: "short unknown suite still fails", short: true, suite: "engin",
			wantAction: fatal, wantMsg: []string{`unknown suite "engin"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string { return tt.env[k] }
			act, dsn, msg := resolve(tt.short, tt.suite, getenv)
			if act != tt.wantAction {
				t.Fatalf("resolve() action = %d, want %d", act, tt.wantAction)
			}
			if tt.wantAction == proceed && dsn != tt.wantDSN {
				t.Errorf("resolve() dsn = %q, want %q", dsn, tt.wantDSN)
			}
			for _, want := range tt.wantMsg {
				if !strings.Contains(msg, want) {
					t.Errorf("resolve() msg = %q, want substring %q", msg, want)
				}
			}
			for _, not := range tt.notMsg {
				if strings.Contains(msg, not) {
					t.Errorf("resolve() msg = %q, want no substring %q", msg, not)
				}
			}
		})
	}
}

func TestResolveDerivesEverySuite(t *testing.T) {
	base := "host=localhost user=gorm password=gorm dbname=ignored port=9921 sslmode=disable"
	getenv := func(k string) string {
		if k == "TEST_DATABASE_DSN" {
			return base
		}
		return ""
	}
	for suite, db := range suiteDBs {
		act, dsn, _ := resolve(false, suite, getenv)
		if act != proceed {
			t.Errorf("resolve(%q) action = %d, want proceed", suite, act)
			continue
		}
		if !strings.Contains(dsn, "dbname="+db) {
			t.Errorf("resolve(%q) dsn = %q, want dbname=%s", suite, dsn, db)
		}
		if strings.Contains(dsn, "ignored") {
			t.Errorf("resolve(%q) dsn = %q, base dbname must be ignored", suite, dsn)
		}
	}
}

func TestWithDBName(t *testing.T) {
	tests := []struct{ name, dsn, nameArg, want string }{
		{"appends when missing", "host=h user=u", "simpwf_test_engine", "host=h user=u dbname=simpwf_test_engine"},
		{"replaces first", "dbname=old host=h", "simpwf_test_engine", "dbname=simpwf_test_engine host=h"},
		{"replaces middle", "host=h dbname=old user=u", "simpwf_test_engine", "host=h dbname=simpwf_test_engine user=u"},
		{"replaces last", "host=h dbname=old", "simpwf_test_engine", "host=h dbname=simpwf_test_engine"},
		{"normalizes odd spacing", "host=h   dbname=old\tuser=u", "simpwf_test_engine", "host=h dbname=simpwf_test_engine user=u"},
		{"matches key case-insensitively", "DBNAME=old host=h", "simpwf_test_engine", "dbname=simpwf_test_engine host=h"},
		{"passes quoted values through", "host=h password='a b' dbname=old", "simpwf_test_engine", "host=h password='a b' dbname=simpwf_test_engine"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := withDBName(tt.dsn, tt.nameArg); got != tt.want {
				t.Errorf("withDBName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractDBName(t *testing.T) {
	tests := []struct{ dsn, want string }{
		{"host=h dbname=foo user=u", "foo"},
		{"host=h user=u", ""},
		{"", ""},
		{"host=h dbname='foo' user=u", "foo"},
		{"host=h DBNAME=foo user=u", "foo"},
	}
	for _, tt := range tests {
		if got := extractDBName(tt.dsn); got != tt.want {
			t.Errorf("extractDBName(%q) = %q, want %q", tt.dsn, got, tt.want)
		}
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct{ name, dsn, want string }{
		{"unquoted", "host=h user=gorm password=s3cr3t dbname=x", "host=h user=gorm password=<redacted> dbname=x"},
		{"quoted with space", "password='s3 cr3t' host=h", "password=<redacted> host=h"},
		{"quoted empty", "password='' host=h", "password=<redacted> host=h"},
		{"absent password unchanged", "host=h user=u dbname=x", "host=h user=u dbname=x"},
		{"url form", "postgres://gorm:s3cr3t@localhost:5432/x", "postgres://gorm:<redacted>@localhost:5432/x"},
		{"url without password unchanged", "postgres://gorm@localhost:5432/x", "postgres://gorm@localhost:5432/x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactDSN(tt.dsn)
			if got != tt.want {
				t.Errorf("redactDSN() = %q, want %q", got, tt.want)
			}
			if strings.Contains(tt.dsn, "s3cr") && strings.Contains(got, "s3cr") {
				t.Errorf("redactDSN() leaked secret: %q", got)
			}
		})
	}
}

func TestRequireDSNMatrix(t *testing.T) {
	base := "host=localhost user=gorm password=topsecret dbname=ignored port=9921 sslmode=disable"
	derived := "host=localhost user=gorm password=topsecret dbname=simpwf_test_engine port=9921 sslmode=disable"
	tests := []struct {
		name       string
		short      bool
		env        map[string]string
		ensureErr  error
		wantDSN    string
		wantSkip   string
		wantFatal  []string
		wantEnsure bool
		mustNoLeak bool
	}{
		{name: "short unset skips", short: true, wantSkip: "SHORT-MODE-SKIP"},
		{
			name: "short never touches ensure", short: true,
			env: map[string]string{"TEST_DATABASE_DSN": base}, wantSkip: "SHORT-MODE-SKIP",
		},
		{
			name: "full unset fails", env: nil,
			wantFatal: []string{`"engine"`, "TEST_DATABASE_DSN"},
		},
		{
			name: "full unreachable fails redacted", env: map[string]string{"TEST_DATABASE_DSN": base},
			ensureErr:  errors.New("connection refused"),
			wantFatal:  []string{`"engine"`, "password=<redacted>", "connection refused"},
			wantEnsure: true, mustNoLeak: true,
		},
		{
			name: "full reachable proceeds", env: map[string]string{"TEST_DATABASE_DSN": base},
			wantDSN: derived, wantEnsure: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &fakeTB{}
			getenv := func(k string) string { return tt.env[k] }
			ensureCalled := false
			ensure := func(dsn string) error { ensureCalled = true; return tt.ensureErr }
			got := requireDSN(tb, "engine", tt.short, getenv, ensure)
			if ensureCalled != tt.wantEnsure {
				t.Errorf("ensure called = %v, want %v", ensureCalled, tt.wantEnsure)
			}
			if tt.wantSkip != "" {
				if !tb.skipped || !strings.Contains(tb.skipMsg, tt.wantSkip) {
					t.Errorf("skipped = %v msg = %q, want skip containing %q", tb.skipped, tb.skipMsg, tt.wantSkip)
				}
				return
			}
			if tt.wantFatal != nil {
				if !tb.failed {
					t.Fatalf("failed = false, want fatal containing %q", tt.wantFatal)
				}
				for _, want := range tt.wantFatal {
					if !strings.Contains(tb.fatalMsg, want) {
						t.Errorf("fatal msg = %q, want substring %q", tb.fatalMsg, want)
					}
				}
				if tt.mustNoLeak && strings.Contains(tb.fatalMsg, "topsecret") {
					t.Errorf("fatal msg leaked password: %q", tb.fatalMsg)
				}
				return
			}
			if tb.skipped || tb.failed {
				t.Fatalf("skip/fatal called: skip=%q fatal=%q", tb.skipMsg, tb.fatalMsg)
			}
			if got != tt.wantDSN {
				t.Errorf("requireDSN() = %q, want %q", got, tt.wantDSN)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	base := "host=localhost user=gorm password=topsecret dbname=ignored port=9921 sslmode=disable"
	derived := "host=localhost user=gorm password=topsecret dbname=simpwf_test_engine port=9921 sslmode=disable"
	baseOnly := func(k string) string {
		if k == "TEST_DATABASE_DSN" {
			return base
		}
		return ""
	}
	t.Run("short skips without touching ensure or open", func(t *testing.T) {
		tb := &fakeTB{}
		ensure, openDB := false, false
		got := open(tb, "engine", true,
			baseOnly,
			func(string) error { ensure = true; return nil },
			func(string) (*gorm.DB, error) { openDB = true; return &gorm.DB{}, nil })
		if got != nil || !tb.skipped || ensure || openDB {
			t.Errorf("got=%v skipped=%v ensure=%v open=%v, want nil/skip/no-touch", got, tb.skipped, ensure, openDB)
		}
		if !strings.Contains(tb.skipMsg, "SHORT-MODE-SKIP") {
			t.Errorf("skip msg = %q, want SHORT-MODE-SKIP", tb.skipMsg)
		}
	})
	t.Run("unreachable ensure fails redacted", func(t *testing.T) {
		tb := &fakeTB{}
		got := open(tb, "engine", false,
			baseOnly,
			func(string) error { return errors.New("connection refused") },
			func(string) (*gorm.DB, error) { t.Error("open called after ensure failure"); return nil, nil })
		if got != nil || !tb.failed {
			t.Fatalf("got=%v failed=%v, want nil/fail", got, tb.failed)
		}
		if !strings.Contains(tb.fatalMsg, "password=<redacted>") || strings.Contains(tb.fatalMsg, "topsecret") {
			t.Errorf("fatal msg = %q, want redacted password", tb.fatalMsg)
		}
	})
	t.Run("open error fails redacted", func(t *testing.T) {
		tb := &fakeTB{}
		got := open(tb, "engine", false,
			baseOnly,
			func(string) error { return nil },
			func(string) (*gorm.DB, error) { return nil, errors.New("boom") })
		if got != nil || !tb.failed {
			t.Fatalf("got=%v failed=%v, want nil/fail", got, tb.failed)
		}
		if !strings.Contains(tb.fatalMsg, `"engine"`) || strings.Contains(tb.fatalMsg, "topsecret") {
			t.Errorf("fatal msg = %q, want suite and redacted password", tb.fatalMsg)
		}
	})
	t.Run("reachable opens and registers cleanup", func(t *testing.T) {
		tb := &fakeTB{}
		var gotDSN string
		got := open(tb, "engine", false,
			baseOnly,
			func(string) error { return nil },
			func(dsn string) (*gorm.DB, error) { gotDSN = dsn; return &gorm.DB{}, nil })
		if got == nil || tb.skipped || tb.failed {
			t.Fatalf("got=%v skipped=%v failed=%v", got, tb.skipped, tb.failed)
		}
		if gotDSN != derived {
			t.Errorf("open dsn = %q, want %q", gotDSN, derived)
		}
		if tb.cleanups != 1 {
			t.Errorf("cleanups = %d, want 1", tb.cleanups)
		}
	})
}
