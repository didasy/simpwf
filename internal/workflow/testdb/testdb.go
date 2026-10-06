// Package testdb centralizes PostgreSQL access for tests.
//
// Full mode (default) fails fast when Postgres is missing or unreachable,
// so DB suites can never go green-with-skips on a bare machine. Short mode
// (-short) skips loudly without touching the network. A single base DSN
// (TEST_DATABASE_DSN) auto-provisions the per-suite scratch database; a
// suite-specific TEST_DATABASE_DSN_<SUITE> var, when set, is honored
// verbatim for backward compatibility.
package testdb

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/simpwf/workflow-engine/pkg/database"
	"gorm.io/gorm"
)

// baseEnv is the single DSN every suite derives its scratch database from.
// Its dbname is credentials-only: it is replaced, never written to.
const baseEnv = "TEST_DATABASE_DSN"

// suiteDBs maps each test suite to its scratch database. Suites never share
// a database: parallel packages plus per-test TRUNCATE forbid it.
var suiteDBs = map[string]string{
	"app":          "simpwf_test_app",
	"database":     "simpwf_test_database",
	"engine":       "simpwf_test_engine",
	"repository":   "simpwf_test_repository",
	"service":      "simpwf_test_service",
	"statusupdate": "simpwf_test_statusupdate",
}

// suiteEnv returns the suite-specific override var, e.g. TEST_DATABASE_DSN_ENGINE.
func suiteEnv(suite string) string {
	return baseEnv + "_" + strings.ToUpper(suite)
}

// action is the resolve decision: proceed with a DSN, skip, or fail.
type action int

const (
	proceed action = iota
	skip
	fatal
)

// resolve implements the env-resolution matrix: suite var verbatim, else
// base-derived, else unset. Short mode always skips without consulting env,
// so -short never needs Postgres or any DSN. Unknown suites always fail:
// that is a bug in the test, not an environment issue.
func resolve(short bool, suite string, getenv func(string) string) (action, string, string) {
	dbName, ok := suiteDBs[suite]
	if !ok {
		return fatal, "", fmt.Sprintf("testdb: unknown suite %q: no scratch database mapped; fix the test to pass a known suite", suite)
	}
	if short {
		return skip, "", fmt.Sprintf("SHORT-MODE-SKIP: suite %q needs PostgreSQL; re-run without -short with %s set to exercise DB tests", suite, baseEnv)
	}
	if dsn := getenv(suiteEnv(suite)); dsn != "" {
		return proceed, dsn, ""
	}
	if base := getenv(baseEnv); base != "" {
		return proceed, withDBName(base, dbName), ""
	}
	return fatal, "", fmt.Sprintf("testdb: suite %q needs PostgreSQL but no DSN is set (checked %s, %s); start Postgres and set %s (single DSN auto-provisions %s), or run with -short to skip DB tests",
		suite, suiteEnv(suite), baseEnv, baseEnv, dbName)
}

// tb is the testing surface the helper needs. testing.TB provides it;
// unit tests use a recording fake (testing.TB itself is sealed by an
// unexported method, so it cannot be faked directly).
type tb interface {
	Helper()
	Cleanup(func())
	Skip(args ...any)
	Fatal(args ...any)
}

// RequireDSN resolves the suite DSN and ensures the scratch database
// exists, failing fast in full mode or skipping loudly under -short.
func RequireDSN(t testing.TB, suite string) string {
	t.Helper()
	return requireDSN(t, suite, testing.Short(), os.Getenv, ensureDatabase)
}

// requireDSN is the testable core of RequireDSN: mode, env, and the
// ensure-if-missing step (the reachability seam) are all injected.
func requireDSN(t tb, suite string, short bool, getenv func(string) string, ensure func(dsn string) error) string {
	t.Helper()
	act, dsn, msg := resolve(short, suite, getenv)
	switch act {
	case skip:
		t.Skip(msg)
		return ""
	case fatal:
		t.Fatal(msg)
		return ""
	}
	// Short mode never reaches here: resolve skips before any network use.
	if err := ensure(dsn); err != nil {
		t.Fatal(unreachableMessage(suite, dsn, err))
		return ""
	}
	return dsn
}

// Open resolves the suite DSN, ensures the scratch database exists, opens
// it with default pool options, and registers a cleanup close.
func Open(t testing.TB, suite string) *gorm.DB {
	t.Helper()
	return open(t, suite, testing.Short(), os.Getenv, ensureDatabase, openDB)
}

// open is the testable core of Open.
func open(t tb, suite string, short bool, getenv func(string) string, ensure func(dsn string) error, openDB func(dsn string) (*gorm.DB, error)) *gorm.DB {
	t.Helper()
	dsn := requireDSN(t, suite, short, getenv, ensure)
	if dsn == "" {
		// Unreachable with a real TB (skip/fatal never return); unit
		// tests drive this path with a recording fake.
		return nil
	}
	db, err := openDB(dsn)
	if err != nil {
		t.Fatal(unreachableMessage(suite, dsn, err))
		return nil
	}
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// openDB opens a suite database with default pool options.
func openDB(dsn string) (*gorm.DB, error) {
	opts := database.DefaultOptions()
	opts.DSN = dsn
	return database.New(opts)
}

// unreachableMessage names the suite, the redacted DSN, and the fix.
func unreachableMessage(suite, dsn string, err error) string {
	return fmt.Sprintf("testdb: suite %q cannot reach PostgreSQL (dsn=%s): %v; start Postgres (single %s auto-provisions the suite database), or run with -short to skip DB tests",
		suite, redactDSN(dsn), err, baseEnv)
}

// ensureDatabase creates the DSN's database when missing, via a
// dbname=postgres maintenance connection. It needs CREATEDB only while
// creating; afterwards the existence check is a no-op. A concurrent
// CREATE of the same name (parallel same-package tests) surfaces as
// 42P04 and is tolerated.
func ensureDatabase(dsn string) error {
	name := extractDBName(dsn)
	if name == "" {
		// No dbname to provision (a suite DSN without one); the open
		// will fail loudly if the server is unreachable.
		return nil
	}
	opts := database.DefaultOptions()
	opts.DSN = withDBName(dsn, "postgres")
	db, err := database.New(opts)
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer func() { _ = sqlDB.Close() }()
	var exists bool
	if err := db.Raw("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = ?)", name).Scan(&exists).Error; err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := db.Exec("CREATE DATABASE " + quoteIdent(name)).Error; err != nil {
		if isAlreadyExists(err) {
			return nil
		}
		return err
	}
	return nil
}

// isAlreadyExists reports a lost CREATE DATABASE race (42P04).
func isAlreadyExists(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P04" {
		return true
	}
	return strings.Contains(err.Error(), "already exists")
}

// withDBName returns dsn with its dbname keyword replaced by name, or
// appended when absent. DSNs stay keyword format (assumption).
func withDBName(dsn, name string) string {
	tokens := strings.Fields(dsn)
	out := make([]string, 0, len(tokens)+1)
	replaced := false
	for _, tok := range tokens {
		if !replaced && strings.HasPrefix(strings.ToLower(tok), "dbname=") {
			out = append(out, "dbname="+name)
			replaced = true
			continue
		}
		out = append(out, tok)
	}
	if !replaced {
		out = append(out, "dbname="+name)
	}
	return strings.Join(out, " ")
}

// extractDBName returns the dsn's dbname value, or "" when absent.
func extractDBName(dsn string) string {
	for _, tok := range strings.Fields(dsn) {
		if strings.HasPrefix(strings.ToLower(tok), "dbname=") {
			return strings.Trim(tok[len("dbname="):], "'")
		}
	}
	return ""
}

var (
	keywordPassword = regexp.MustCompile(`(?i)password\s*=\s*('[^']*'|[^\s]+)`)
	urlPassword     = regexp.MustCompile(`(?i)(\w+://[^:/@\s]+:)[^@\s]+(@)`)
)

// redactDSN replaces password values with <redacted> for safe output.
func redactDSN(dsn string) string {
	dsn = keywordPassword.ReplaceAllString(dsn, `password=<redacted>`)
	return urlPassword.ReplaceAllString(dsn, `${1}<redacted>${2}`)
}

// quoteIdent quotes a database identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
