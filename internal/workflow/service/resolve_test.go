// Internal test: Resolve's fetch-once behavior is asserted through
// counting repos, and the hit itself through white-box cache stats.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

var resolveLimits = model.NodeLimits{
	DefaultTimeout:   30 * time.Second,
	MaxTimeout:       5 * time.Minute,
	ConditionTimeout: 5 * time.Second,
}

const (
	resolveDefID     = "aaaaaaaa-0000-7000-8000-000000000001"
	resolveDefID2    = "aaaaaaaa-0000-7000-8000-000000000002"
	resolveNodeDefID = "bbbbbbbb-0000-7000-8000-000000000001"
)

const resolvePlainContent = `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
	{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"},
	{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
]}`

const resolveRefContent = `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
	{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"bbbbbbbb-0000-7000-8000-000000000001","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"},
	{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
]}`

// resolveCountingWfRepo counts GetByID calls; other methods come from the
// embedded interface and panic if touched.
type resolveCountingWfRepo struct {
	repository.WorkflowDefinitionRepository
	mu    sync.Mutex
	defs  map[string]model.WorkflowDefinition
	calls int
	err   error
}

func (r *resolveCountingWfRepo) GetByID(_ context.Context, id string) (model.WorkflowDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return model.WorkflowDefinition{}, r.err
	}
	def, ok := r.defs[id]
	if !ok {
		return model.WorkflowDefinition{}, errors.Join(model.ErrNotFound, errors.New("workflow definition "+id))
	}
	return def, nil
}

func (r *resolveCountingWfRepo) getCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type resolveCountingNodeRepo struct {
	repository.NodeDefinitionRepository
	mu    sync.Mutex
	defs  map[string]model.NodeDefinition
	calls int
	err   error
}

func (r *resolveCountingNodeRepo) GetByID(_ context.Context, id string) (model.NodeDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return model.NodeDefinition{}, r.err
	}
	def, ok := r.defs[id]
	if !ok {
		return model.NodeDefinition{}, errors.Join(model.ErrNotFound, errors.New("node definition "+id))
	}
	return def, nil
}

func (r *resolveCountingNodeRepo) getCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func resolveTestService(wf *resolveCountingWfRepo, nd *resolveCountingNodeRepo) *workflowDefinitionService {
	return NewWorkflowDefinitionService(wf, nd, resolveLimits, "actor").(*workflowDefinitionService)
}

func resolveTestNodeRepo() *resolveCountingNodeRepo {
	return &resolveCountingNodeRepo{defs: map[string]model.NodeDefinition{
		resolveNodeDefID: {
			ID: resolveNodeDefID, Name: "shared-script", Version: 1, LineageID: resolveNodeDefID,
			Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 42;","timeout":"45s"}`),
		},
	}}
}

func TestResolveFetchesOnce(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{
		resolveDefID: {ID: resolveDefID, Content: json.RawMessage(resolveRefContent)},
	}}
	nd := resolveTestNodeRepo()
	svc := resolveTestService(wf, nd)
	ctx := context.Background()

	first, err := svc.Resolve(ctx, resolveDefID)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if first.Nodes[0].Script != "return 42;" {
		t.Errorf("script = %q, want merged from node definition", first.Nodes[0].Script)
	}
	second, err := svc.Resolve(ctx, resolveDefID)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if second != first {
		t.Error("second Resolve() returned a different pointer, want the cached tree")
	}
	if got := wf.getCalls(); got != 1 {
		t.Errorf("workflow GetByID calls = %d, want 1", got)
	}
	if got := nd.getCalls(); got != 1 {
		t.Errorf("node GetByID calls = %d, want 1", got)
	}
	if got := svc.resolveCache.Hits(); got != 1 {
		t.Errorf("cache Hits() = %d, want 1", got)
	}
}

func TestResolveDistinctIDsComputeIndependently(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{
		resolveDefID:  {ID: resolveDefID, Content: json.RawMessage(resolvePlainContent)},
		resolveDefID2: {ID: resolveDefID2, Content: json.RawMessage(resolvePlainContent)},
	}}
	svc := resolveTestService(wf, resolveTestNodeRepo())
	ctx := context.Background()

	if _, err := svc.Resolve(ctx, resolveDefID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resolve(ctx, resolveDefID2); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Resolve(ctx, resolveDefID); err != nil {
		t.Fatal(err)
	}
	if got := wf.getCalls(); got != 2 {
		t.Errorf("workflow GetByID calls = %d, want 2 (once per id)", got)
	}
	if got := svc.resolveCache.Hits(); got != 1 {
		t.Errorf("cache Hits() = %d, want 1", got)
	}
}

func TestResolveUnknownIDFailsWithoutCaching(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{}}
	svc := resolveTestService(wf, resolveTestNodeRepo())
	ctx := context.Background()

	sentinel := errors.New("store exploded")
	wf.err = sentinel
	for i := 0; i < 2; i++ {
		_, err := svc.Resolve(ctx, "missing")
		if !errors.Is(err, sentinel) {
			t.Fatalf("Resolve() error = %v, want the store error unwrapped", err)
		}
		if errors.Is(err, model.ErrInvalid) {
			t.Fatalf("Resolve() error = %v, must not wrap a fetch failure as invalid", err)
		}
	}
	if got := wf.getCalls(); got != 2 {
		t.Errorf("workflow GetByID calls = %d, want 2 (failures retried)", got)
	}
}

func TestResolveCorruptContentFailsInvalidWithoutCaching(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{
		resolveDefID: {ID: resolveDefID, Content: json.RawMessage(`{"nodes":[`)},
	}}
	svc := resolveTestService(wf, resolveTestNodeRepo())
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := svc.Resolve(ctx, resolveDefID); !errors.Is(err, model.ErrInvalid) {
			t.Fatalf("Resolve() error = %v, want ErrInvalid", err)
		}
	}
	if got := wf.getCalls(); got != 2 {
		t.Errorf("workflow GetByID calls = %d, want 2 (failures retried)", got)
	}
}

func TestResolveNodeErrorFailsInvalidAndRetries(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{
		resolveDefID: {ID: resolveDefID, Content: json.RawMessage(resolveRefContent)},
	}}
	nd := resolveTestNodeRepo()
	nd.err = errors.New("node store exploded")
	svc := resolveTestService(wf, nd)
	ctx := context.Background()

	if _, err := svc.Resolve(ctx, resolveDefID); !errors.Is(err, model.ErrInvalid) {
		t.Fatalf("Resolve() error = %v, want ErrInvalid", err)
	}
	nd.err = nil
	got, err := svc.Resolve(ctx, resolveDefID)
	if err != nil {
		t.Fatalf("Resolve() after recovery error = %v", err)
	}
	if got.Nodes[0].Script != "return 42;" {
		t.Errorf("script = %q, want merged after recovery", got.Nodes[0].Script)
	}
	if n := nd.getCalls(); n != 2 {
		t.Errorf("node GetByID calls = %d, want 2 (failed materialize retried)", n)
	}
}

func TestResolveConcurrentHammer(t *testing.T) {
	wf := &resolveCountingWfRepo{defs: map[string]model.WorkflowDefinition{
		resolveDefID: {ID: resolveDefID, Content: json.RawMessage(resolveRefContent)},
	}}
	svc := resolveTestService(wf, resolveTestNodeRepo())
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := svc.Resolve(ctx, resolveDefID); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := wf.getCalls(); got != 1 {
		t.Errorf("workflow GetByID calls = %d, want 1", got)
	}
}
