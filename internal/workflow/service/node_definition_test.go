package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

const (
	actorID       = "00000000-0000-7000-8000-000000000001"
	lineageID     = "99999999-9999-7999-8999-999999999999"
	defIDV1       = "11111111-1111-7111-8111-111111111111"
	defIDV2       = "22222222-2222-7222-8222-222222222222"
	defIDOther    = "33333333-3333-7333-8333-333333333333"
	scriptContent = `{"type":"script","script":"return 1;"}`
)

var testLimits = model.NodeLimits{
	DefaultTimeout:   30 * time.Second,
	MaxTimeout:       5 * time.Minute,
	ConditionTimeout: 5 * time.Second,
}

// fakeNodeRepo is an in-memory NodeDefinitionRepository used to exercise the
// service's orchestration and versioning logic.
type fakeNodeRepo struct {
	defs    map[string]model.NodeDefinition
	refs    map[string]bool // node def ids referenced by workflows/instances
	raceErr bool
}

func newFakeNodeRepo() *fakeNodeRepo {
	return &fakeNodeRepo{defs: map[string]model.NodeDefinition{}, refs: map[string]bool{}}
}

func (f *fakeNodeRepo) Create(_ context.Context, def model.NodeDefinition) error {
	if f.raceErr {
		return errors.Join(model.ErrConflict, errors.New("duplicate previous_version_id"))
	}
	for _, existing := range f.defs {
		if existing.PreviousVersionID != nil && def.PreviousVersionID != nil &&
			*existing.PreviousVersionID == *def.PreviousVersionID {
			return model.ErrConflict
		}
	}
	f.defs[def.ID] = def
	return nil
}

func (f *fakeNodeRepo) GetByID(_ context.Context, id string) (model.NodeDefinition, error) {
	def, ok := f.defs[id]
	if !ok {
		return model.NodeDefinition{}, errors.Join(model.ErrNotFound, errors.New("node definition "+id))
	}
	return def, nil
}

func (f *fakeNodeRepo) List(_ context.Context, q repository.DefinitionListQuery) ([]model.NodeDefinition, int64, error) {
	var items []model.NodeDefinition
	for _, def := range f.defs {
		if q.Name != "" && def.Name != q.Name {
			continue
		}
		if q.Type != "" && def.Type != q.Type {
			continue
		}
		if len(q.IDs) > 0 && !contains(q.IDs, def.ID) {
			continue
		}
		items = append(items, def)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, int64(len(items)), nil
}

func (f *fakeNodeRepo) Delete(_ context.Context, id string) error {
	if f.refs[id] {
		return model.ErrConflict
	}
	if _, ok := f.defs[id]; !ok {
		return model.ErrNotFound
	}
	delete(f.defs, id)
	return nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func newService(repo *fakeNodeRepo) service.NodeDefinitionService {
	return service.NewNodeDefinitionService(repo, testLimits, actorID)
}

func TestCreateNodeDefinitionV1(t *testing.T) {
	svc := newService(newFakeNodeRepo())
	def, err := svc.Create(context.Background(), service.CreateNodeDefinition{
		Name:    "transform",
		Type:    "script",
		Content: json.RawMessage(scriptContent),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if def.Version != 1 {
		t.Errorf("version = %d, want 1", def.Version)
	}
	if !ids.Valid(def.ID) {
		t.Errorf("id %q is not a valid uuid", def.ID)
	}
	if !ids.Valid(def.LineageID) {
		t.Errorf("lineage_id %q is not a valid uuid", def.LineageID)
	}
	if def.PreviousVersionID != nil {
		t.Errorf("previous_version_id = %v, want nil", *def.PreviousVersionID)
	}
	if def.CreatedBy != actorID || def.UpdatedBy != actorID {
		t.Errorf("actor mismatch: %+v", def)
	}
}

func TestCreateNodeDefinitionNextVersion(t *testing.T) {
	repo := newFakeNodeRepo()
	repo.defs[defIDV1] = model.NodeDefinition{
		ID: defIDV1, Name: "transform", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(scriptContent),
	}
	svc := newService(repo)

	prev := defIDV1
	def, err := svc.Create(context.Background(), service.CreateNodeDefinition{
		Name:              "transform",
		Type:              "script",
		PreviousVersionID: &prev,
		Content:           json.RawMessage(scriptContent),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if def.Version != 2 {
		t.Errorf("version = %d, want 2", def.Version)
	}
	if def.LineageID != lineageID {
		t.Errorf("lineage = %s, want inherited %s", def.LineageID, lineageID)
	}
	if def.PreviousVersionID == nil || *def.PreviousVersionID != defIDV1 {
		t.Errorf("previous = %v, want %s", def.PreviousVersionID, defIDV1)
	}
}

func TestCreateNodeDefinitionRejectsInvalidContent(t *testing.T) {
	svc := newService(newFakeNodeRepo())
	bad := []service.CreateNodeDefinition{
		{Name: "x", Type: "output", Content: json.RawMessage(`{"type":"output"}`)},
		{Name: "x", Type: "magic", Content: json.RawMessage(`{"type":"magic"}`)},
		{Name: "x", Type: "script", Content: json.RawMessage(`{"type":"script"}`)},                                      // missing script
		{Name: "x", Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 1;","timeout":"10m"}`)}, // over cap
		{Name: "  ", Type: "script", Content: json.RawMessage(scriptContent)},                                           // blank name
		{
			Name: "gate", Type: "conditions",
			Content: json.RawMessage(`{"type":"conditions","conditions":[{"key":"yes","condition":"return true;"},{"key":"no","condition":"return false;"}],"branches":{"yes":""}}`),
		}, // graph routing belongs to workflows
		{
			Name: "gate", Type: "conditions",
			Content: json.RawMessage(`{"type":"conditions","conditions":[{"key":"yes","condition":"return true;"},{"key":"no","condition":"return false;"}],"branches":{}}`),
		}, // even an empty graph binding object belongs to workflows
		{
			Name: "group", Type: "group",
			Content: json.RawMessage(`{
				"type":"group",
				"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
				"keys":{},
				"nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","script":"return 1;"}]
			}`),
		}, // keys belong to workflow/group occurrences, not reusable definitions
	}
	for i, req := range bad {
		if _, err := svc.Create(context.Background(), req); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("case %d: error = %v, want ErrInvalid", i, err)
		}
	}
}

func TestCreateConditionNodeDefinitionWithoutRouting(t *testing.T) {
	svc := newService(newFakeNodeRepo())
	_, err := svc.Create(context.Background(), service.CreateNodeDefinition{
		Name: "gate",
		Type: "conditions",
		Content: json.RawMessage(`{
			"type":"conditions",
			"conditions":[
				{"key":"yes","condition":"return true;"},
				{"key":"no","condition":"return false;"}
			]
		}`),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
}

func TestCreateNodeDefinitionUnknownPrevious(t *testing.T) {
	svc := newService(newFakeNodeRepo())
	missing := "44444444-4444-7444-8444-444444444444"
	_, err := svc.Create(context.Background(), service.CreateNodeDefinition{
		Name:              "transform",
		Type:              "script",
		PreviousVersionID: &missing,
		Content:           json.RawMessage(scriptContent),
	})
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestCreateNodeDefinitionVersionRace(t *testing.T) {
	repo := newFakeNodeRepo()
	repo.raceErr = true
	svc := newService(repo)
	_, err := svc.Create(context.Background(), service.CreateNodeDefinition{
		Name:    "transform",
		Type:    "script",
		Content: json.RawMessage(scriptContent),
	})
	if !errors.Is(err, model.ErrConflict) {
		t.Errorf("error = %v, want ErrConflict", err)
	}
}

func TestNodeDefinitionGetAndDelete(t *testing.T) {
	repo := newFakeNodeRepo()
	repo.defs[defIDV1] = model.NodeDefinition{
		ID: defIDV1, Name: "transform", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(scriptContent),
	}
	svc := newService(repo)

	got, err := svc.Get(context.Background(), defIDV1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != "transform" {
		t.Errorf("name = %q", got.Name)
	}

	if err := svc.Delete(context.Background(), defIDV1); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := svc.Get(context.Background(), defIDV1); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get() after delete error = %v, want ErrNotFound", err)
	}
}

func TestNodeDefinitionDeleteReferencedConflict(t *testing.T) {
	repo := newFakeNodeRepo()
	repo.defs[defIDV1] = model.NodeDefinition{ID: defIDV1, Name: "transform", Version: 1, LineageID: lineageID, Type: "script"}
	repo.refs[defIDV1] = true
	svc := newService(repo)

	err := svc.Delete(context.Background(), defIDV1)
	if !errors.Is(err, model.ErrConflict) {
		t.Errorf("Delete() error = %v, want ErrConflict", err)
	}
}

func TestNodeDefinitionList(t *testing.T) {
	repo := newFakeNodeRepo()
	repo.defs[defIDV1] = model.NodeDefinition{ID: defIDV1, Name: "transform", Version: 1, LineageID: lineageID, Type: "script"}
	repo.defs[defIDOther] = model.NodeDefinition{ID: defIDOther, Name: "gate", Version: 1, LineageID: "88888888-8888-7888-8888-888888888888", Type: "conditions"}
	svc := newService(repo)

	items, total, err := svc.List(context.Background(), repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at", Type: "script"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != defIDV1 {
		t.Errorf("list: total %d items %+v", total, items)
	}
	if !strings.Contains(items[0].Type, "script") {
		t.Errorf("type = %q", items[0].Type)
	}
}
