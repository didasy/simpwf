package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// queryCounter is a gorm logger that counts every SQL round-trip including
// misses (ErrRecordNotFound still went to the database).
type queryCounter struct {
	mu sync.Mutex
	n  int
}

func (l *queryCounter) LogMode(gormlogger.LogLevel) gormlogger.Interface { return l }
func (l *queryCounter) Info(context.Context, string, ...any)             {}
func (l *queryCounter) Warn(context.Context, string, ...any)             {}
func (l *queryCounter) Error(context.Context, string, ...any)            {}
func (l *queryCounter) Trace(context.Context, time.Time, func() (string, int64), error) {
	l.mu.Lock()
	l.n++
	l.mu.Unlock()
}

func (l *queryCounter) reset() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.n
	l.n = 0
	return n
}

// TestGetStatusDetailQueryCount proves one status poll costs a constant
// number of SQL statements regardless of instance shape: a 1-execution x
// 1-branch instance and a 5 x 8 one on the same definition must produce
// identical counts. Any per-item query (branch fan-out, second resolve,
// per-id walk with SQL) breaks the equality without hardcoding counts. The
// absolute ceiling documents the ~5 steady-state budget with headroom.
func TestGetStatusDetailQueryCount(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "script", "a", "return 1;", "", map[string]any{"output_property": "out"}),
	)
	setupSvc := svcInstanceService(db)
	small := seedParallelShape(t, db, setupSvc, wfID, 1, 1)
	large := seedParallelShape(t, db, setupSvc, wfID, 5, 8)

	counter := &queryCounter{}
	svc := svcInstanceService(db.Session(&gorm.Session{Logger: counter}))
	// Warm the Resolve definition cache: a warmed Resolve contributes
	// zero SQL to both measurements.
	if _, err := svc.GetStatusDetail(ctx, small, auth.Principal{}); err != nil {
		t.Fatalf("warm GetStatusDetail() error = %v", err)
	}
	counter.reset()

	if _, err := svc.GetStatusDetail(ctx, small, auth.Principal{}); err != nil {
		t.Fatalf("GetStatusDetail(small) error = %v", err)
	}
	smallN := counter.reset()
	if _, err := svc.GetStatusDetail(ctx, large, auth.Principal{}); err != nil {
		t.Fatalf("GetStatusDetail(large) error = %v", err)
	}
	largeN := counter.reset()
	t.Logf("status SQL: 1x1=%d 5x8=%d", smallN, largeN)
	if smallN != largeN {
		t.Fatalf("status SQL depends on shape: 1x1=%d 5x8=%d", smallN, largeN)
	}
	if largeN > 8 {
		t.Fatalf("status SQL = %d, want <= 8", largeN)
	}
}
