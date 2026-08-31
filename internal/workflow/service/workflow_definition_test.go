package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

const (
	wfIDV1   = "11111111-1111-7111-8111-111111111111"
	wfIDV2   = "22222222-2222-7222-8222-222222222222"
	nodeDefA = "99999999-9999-7999-8999-999999999999"
)

const validWorkflowContent = `{
	"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
	"nodes": [
		{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;", "next_node": "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"},
		{"id": "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb", "type": "script", "script": "return 2;"}
	]
}`

// fakeWorkflowRepo is an in-memory WorkflowDefinitionRepository.
type fakeWorkflowRepo struct {
	defs     map[string]model.WorkflowDefinition
	refs     map[string][]string
	raceErr  bool
	reqRefs  map[string]bool
	instRefs map[string]bool
}

func newFakeWorkflowRepo() *fakeWorkflowRepo {
	return &fakeWorkflowRepo{
		defs:     map[string]model.WorkflowDefinition{},
		refs:     map[string][]string{},
		reqRefs:  map[string]bool{},
		instRefs: map[string]bool{},
	}
}

func (f *fakeWorkflowRepo) Create(_ context.Context, def model.WorkflowDefinition) error {
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

func (f *fakeWorkflowRepo) GetByID(_ context.Context, id string) (model.WorkflowDefinition, error) {
	def, ok := f.defs[id]
	if !ok {
		return model.WorkflowDefinition{}, errors.Join(model.ErrNotFound, errors.New("workflow definition "+id))
	}
	return def, nil
}

func (f *fakeWorkflowRepo) List(_ context.Context, q repository.DefinitionListQuery) ([]model.WorkflowDefinition, int64, error) {
	var items []model.WorkflowDefinition
	for _, def := range f.defs {
		if q.Name != "" && def.Name != q.Name {
			continue
		}
		if len(q.IDs) > 0 && !contains(q.IDs, def.ID) {
			continue
		}
		items = append(items, def)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Version > items[j].Version })
	return items, int64(len(items)), nil
}

func (f *fakeWorkflowRepo) Delete(_ context.Context, id string) error {
	if f.reqRefs[id] || f.instRefs[id] {
		return model.ErrConflict
	}
	if _, ok := f.defs[id]; !ok {
		return model.ErrNotFound
	}
	delete(f.defs, id)
	return nil
}

func (f *fakeWorkflowRepo) SetNodeRefs(_ context.Context, workflowDefinitionID string, nodeDefinitionIDs []string) error {
	f.refs[workflowDefinitionID] = nodeDefinitionIDs
	return nil
}

func newWorkflowService(wfRepo *fakeWorkflowRepo, ndRepo *fakeNodeRepo) service.WorkflowDefinitionService {
	if ndRepo == nil {
		ndRepo = newFakeNodeRepo()
	}
	return service.NewWorkflowDefinitionService(wfRepo, ndRepo, testLimits, actorID)
}

func TestCreateWorkflowDefinitionV1(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	def, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:    "flow",
		Content: json.RawMessage(validWorkflowContent),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if def.Version != 1 || def.PreviousVersionID != nil {
		t.Errorf("version fields mismatch: %+v", def)
	}
	if def.CreatedBy != actorID || def.UpdatedBy != actorID {
		t.Errorf("actor mismatch: %+v", def)
	}
}

func TestCreateWorkflowDefinitionNextVersion(t *testing.T) {
	repo := newFakeWorkflowRepo()
	repo.defs[wfIDV1] = model.WorkflowDefinition{ID: wfIDV1, Name: "flow", Version: 1, LineageID: lineageID, Content: json.RawMessage(validWorkflowContent)}
	svc := newWorkflowService(repo, nil)

	prev := wfIDV1
	def, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:              "flow",
		PreviousVersionID: &prev,
		Content:           json.RawMessage(validWorkflowContent),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if def.Version != 2 || def.LineageID != lineageID || def.PreviousVersionID == nil || *def.PreviousVersionID != wfIDV1 {
		t.Errorf("version chain mismatch: %+v", def)
	}
}

func TestCreateWorkflowDefinitionRejectsInvalid(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	bad := []service.CreateWorkflowDefinition{
		{Name: "  ", Content: json.RawMessage(validWorkflowContent)},
		{Name: "flow", Content: json.RawMessage(`{"nodes": []}`)},                    // no start
		{Name: "flow", Content: json.RawMessage(`{"start_node_id":"x","nodes":[]}`)}, // bad start
		{Name: "flow", Content: json.RawMessage(`{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script"}]}`)}, // missing script
	}
	for i, req := range bad {
		if _, err := svc.Create(context.Background(), req); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("case %d: error = %v, want ErrInvalid", i, err)
		}
	}
}

func TestCreateWorkflowDefinitionUnknownNodeRef(t *testing.T) {
	ndRepo := newFakeNodeRepo() // empty: no node definitions exist
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `"}
	]}`
	_, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:    "flow",
		Content: json.RawMessage(content),
	})
	if !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid for unknown node definition", err)
	}
}

func TestCreateWorkflowDefinitionWithNodeRefs(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-script", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 42;","timeout":"30s"}`),
	}
	wfRepo := newFakeWorkflowRepo()
	svc := newWorkflowService(wfRepo, ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
	]}`
	def, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:    "flow",
		Content: json.RawMessage(content),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	refs := wfRepo.refs[def.ID]
	if len(refs) != 1 || refs[0] != nodeDefA {
		t.Errorf("refs = %v, want [%s]", refs, nodeDefA)
	}
}

func TestMaterializeMergesNodeDefinition(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-script", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 42;","timeout":"45s"}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	first := merged.Nodes[0]
	if first.Type != model.NodeTypeScript {
		t.Errorf("type = %s, want script", first.Type)
	}
	if first.Script != "return 42;" {
		t.Errorf("script = %q, want merged from definition", first.Script)
	}
	if first.ID != "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa" || first.NextNode != "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb" {
		t.Errorf("graph fields lost: %+v", first)
	}
	if first.NodeDefinitionID != nodeDefA {
		t.Errorf("node_definition_id = %q", first.NodeDefinitionID)
	}
}

func TestMaterializeValidatesConditionDefinitionWithWorkflowKeys(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-gate", Version: 1, LineageID: lineageID,
		Type: "conditions", Content: json.RawMessage(`{
			"type":"conditions",
			"conditions":[
				{"key":"yes","condition":"return true;"},
				{"key":"exit","condition":"return false;"}
			]
		}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","keys":{
			"yes":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
			"exit":"",
			"future":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
		},"nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `"},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	first := merged.Nodes[0]
	if first.Type != model.NodeTypeConditions || len(first.Conditions) != 2 {
		t.Fatalf("materialized condition node = %+v", first)
	}
	if merged.Keys["yes"] != "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb" {
		t.Errorf("yes key = %q", merged.Keys["yes"])
	}
	if target, ok := merged.Keys["exit"]; !ok || target != "" {
		t.Errorf("exit key = %q, present %v", target, ok)
	}
}

func TestCreateWorkflowDefinitionRejectsMissingConditionKey(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-gate", Version: 1, LineageID: lineageID,
		Type: "conditions", Content: json.RawMessage(`{
			"type":"conditions",
			"conditions":[
				{"key":"yes","condition":"return true;"},
				{"key":"missing","condition":"return false;"}
			]
		}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","keys":{
			"yes":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
		},"nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `"},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 2;"}
	]}`
	_, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:    "flow",
		Content: json.RawMessage(content),
	})
	if !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
}

func TestMaterializeRejectsKeysForNonGroupDefinition(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-script", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 1;"}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `","keys":{}}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Materialize(context.Background(), wc); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
}

func TestMaterializeRejectsTypeMismatch(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "gate", Version: 1, LineageID: lineageID,
		Type: "conditions", Content: json.RawMessage(`{"type":"conditions","conditions":[{"key":"yes","condition":"return true;"},{"key":"no","condition":"return false;"}]}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"script","node_definition_id":"` + nodeDefA + `"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Materialize(context.Background(), wc); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid on type mismatch", err)
	}
}

func TestWorkflowDefinitionGetListDelete(t *testing.T) {
	repo := newFakeWorkflowRepo()
	repo.defs[wfIDV1] = model.WorkflowDefinition{ID: wfIDV1, Name: "flow", Version: 1, LineageID: lineageID, Content: json.RawMessage(validWorkflowContent)}
	svc := newWorkflowService(repo, nil)

	if _, err := svc.Get(context.Background(), wfIDV1); err != nil {
		t.Errorf("Get() error = %v", err)
	}
	if _, err := svc.Get(context.Background(), "missing"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get(missing) error = %v, want ErrNotFound", err)
	}

	items, total, err := svc.List(context.Background(), repository.DefinitionListQuery{Page: 1, PerPage: 50, Order: "-created_at"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 {
		t.Errorf("list total %d items %d, want 1/1", total, len(items))
	}

	if err := svc.Delete(context.Background(), wfIDV1); err != nil {
		t.Errorf("Delete() error = %v", err)
	}
	repo.reqRefs[wfIDV1] = true
	if err := svc.Delete(context.Background(), wfIDV1); !errors.Is(err, model.ErrConflict) {
		t.Errorf("Delete(referenced) error = %v, want ErrConflict", err)
	}
}

func TestCreateWorkflowDefinitionVersionRace(t *testing.T) {
	repo := newFakeWorkflowRepo()
	repo.raceErr = true
	svc := newWorkflowService(repo, nil)
	_, err := svc.Create(context.Background(), service.CreateWorkflowDefinition{
		Name:    "flow",
		Content: json.RawMessage(validWorkflowContent),
	})
	if !errors.Is(err, model.ErrConflict) {
		t.Errorf("error = %v, want ErrConflict", err)
	}
}

func TestMaterializePreservesStatusUpdate(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	content := `{
		"start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
		"status_update": {"http": {"url": "https://hooks.example.com/wf"}},
		"nodes": [
			{"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"}
		]
	}`
	wc, err := model.ParseWorkflowContent(json.RawMessage(content), testLimits)
	if err != nil {
		t.Fatalf("ParseWorkflowContent() error = %v", err)
	}
	out, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	if out.StatusUpdate == nil || out.StatusUpdate.HTTP == nil || out.StatusUpdate.HTTP.URL != "https://hooks.example.com/wf" {
		t.Errorf("StatusUpdate lost by materialization: %+v", out.StatusUpdate)
	}
}

// hookedNodeDef is a reusable script definition carrying both lifecycle
// hooks, used to exercise occurrence inherit/replace/disable rules.
const hookedNodeDef = `{"type":"script","script":"return 1;",
	"pre_script":{"script":"context.def_pre = 1;"},
	"post_script":{"script":"context.def_post = 1;"}}`

func materializeHooked(t *testing.T, occurrenceJSON string) *model.NodeContent {
	t.Helper()
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "hooked", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(hookedNodeDef),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[` + occurrenceJSON + `]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatalf("ParseWorkflowContent() error = %v", err)
	}
	merged, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	return merged.Nodes[0]
}

func TestMaterializeHookOmittedInheritsDefinition(t *testing.T) {
	first := materializeHooked(t, `{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"`+nodeDefA+`"}`)
	if first.PreScript == nil || first.PreScript.Script != "context.def_pre = 1;" {
		t.Errorf("pre_script = %+v, want inherited from definition", first.PreScript)
	}
	if first.PostScript == nil || first.PostScript.Script != "context.def_post = 1;" {
		t.Errorf("post_script = %+v, want inherited from definition", first.PostScript)
	}
}

func TestMaterializeHookObjectReplacesDefinition(t *testing.T) {
	first := materializeHooked(t, `{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"`+nodeDefA+`","pre_script":{"script":"context.occ_pre = 1;"}}`)
	if first.PreScript == nil || first.PreScript.Script != "context.occ_pre = 1;" {
		t.Errorf("pre_script = %+v, want occurrence override", first.PreScript)
	}
	if first.PostScript == nil || first.PostScript.Script != "context.def_post = 1;" {
		t.Errorf("post_script = %+v, want definition hook untouched by pre override", first.PostScript)
	}
}

func TestMaterializeHookNullDisablesDefinition(t *testing.T) {
	first := materializeHooked(t, `{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"`+nodeDefA+`","post_script":null}`)
	if first.PreScript == nil || first.PreScript.Script != "context.def_pre = 1;" {
		t.Errorf("pre_script = %+v, want definition hook intact", first.PreScript)
	}
	if first.PostScript != nil {
		t.Errorf("post_script = %+v, want nil after explicit null", first.PostScript)
	}
	if !first.PostScriptSet {
		t.Error("post_script presence flag not recorded for explicit null")
	}
}

func TestMaterializeGroupChildHookOverrides(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "hooked", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(hookedNodeDef),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","type":"group","start_node_id":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","nodes":[
			{"id":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","node_definition_id":"` + nodeDefA + `","post_script":null},
			{"id":"dddddddd-dddd-7ddd-8ddd-dddddddddddd","type":"script","script":"return 2;"}
		],"keys":{}},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 3;"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatalf("ParseWorkflowContent() error = %v", err)
	}
	merged, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	child := merged.Nodes[0].Group.Nodes[0]
	if child.PreScript == nil || child.PreScript.Script != "context.def_pre = 1;" {
		t.Errorf("group child pre_script = %+v, want definition hook", child.PreScript)
	}
	if child.PostScript != nil {
		t.Errorf("group child post_script = %+v, want nil after explicit null", child.PostScript)
	}
}

func TestMaterializeOccurrenceOnFailureSurvives(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-http", Version: 1, LineageID: lineageID,
		Type: "external_call", Content: json.RawMessage(`{"type":"external_call","http_config":{"url":"https://example.com/api"}}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","on_failure":{"next_node":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","output_property":"err"}},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 1;"},
		{"id":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","type":"input","channel":"http","context_path":"fix"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := svc.Materialize(context.Background(), wc)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	first := merged.Nodes[0]
	if first.OnFailure == nil {
		t.Fatal("on_failure not preserved on materialized node")
	}
	if first.OnFailure.NextNode != "cccccccc-cccc-7ccc-8ccc-cccccccccccc" || first.OnFailure.OutputProperty != "err" {
		t.Errorf("on_failure mismatch: %+v", first.OnFailure)
	}
}

func TestMaterializeOccurrenceOnFailureRejectsUnsupportedDefinitionType(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID: nodeDefA, Name: "shared-script", Version: 1, LineageID: lineageID,
		Type: "script", Content: json.RawMessage(`{"type":"script","script":"return 1;"}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)

	content := `{"start_node_id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","nodes":[
		{"id":"aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa","node_definition_id":"` + nodeDefA + `","next_node":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","on_failure":{"next_node":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","output_property":"err"}},
		{"id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","type":"script","script":"return 1;"},
		{"id":"cccccccc-cccc-7ccc-8ccc-cccccccccccc","type":"input","channel":"http","context_path":"fix"}
	]}`
	wc, err := model.ParseWorkflowContent([]byte(content), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Materialize(context.Background(), wc); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Materialize() error = %v, want ErrInvalid for script definition with occurrence on_failure", err)
	}
}
