package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

const childIDForSchema = "dddddddd-dddd-7ddd-8ddd-dddddddddddd"

// schemasFor calls the read-path schema lookup under test.
func schemasFor(t *testing.T, svc service.WorkflowDefinitionService, content string) map[string]json.RawMessage {
	t.Helper()
	got := svc.Schemas(context.Background(), json.RawMessage(content))
	return got
}

func TestSchemasReturnsOnlyUsedTypes(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	content := `{
      "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
      "nodes": [
        {"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script", "script": "return 1;"},
        {"id": "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb", "type": "output", "channel": "redis", "context_path": "x"}
      ]
    }`
	got := schemasFor(t, svc, content)
	if len(got) != 2 {
		t.Fatalf("Schemas() has %d types (%v), want 2", len(got), schemaKeys(got))
	}
	for _, want := range []string{"script", "output"} {
		if _, ok := got[want]; !ok {
			t.Errorf("Schemas() missing %q", want)
		}
	}
	// Types the workflow does not use stay out.
	for _, unwanted := range []string{"input", "poller", "group", "conditions", "external_call"} {
		if _, present := got[unwanted]; present {
			t.Errorf("Schemas() included unused type %q", unwanted)
		}
	}
}

// TestSchemasWalksNestedGroups covers the nested-children walk: a type used
// only inside a group must still be reported.
func TestSchemasWalksNestedGroups(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	content := `{
      "start_node_id": "eeeeeeee-eeee-7eee-8eee-eeeeeeeeeeee",
      "nodes": [
        {
          "id": "eeeeeeee-eeee-7eee-8eee-eeeeeeeeeeee",
          "type": "group",
          "start_node_id": "` + childIDForSchema + `",
          "nodes": [
            {"id": "` + childIDForSchema + `", "type": "input", "channel": "http"}
          ]
        }
      ]
    }`
	got := schemasFor(t, svc, content)
	if len(got) != 2 {
		t.Fatalf("Schemas() has %d types (%v), want 2", len(got), schemaKeys(got))
	}
	for _, want := range []string{"group", "input"} {
		if _, ok := got[want]; !ok {
			t.Errorf("Schemas() missing nested type %q", want)
		}
	}
}

// TestSchemasResolvesReferenceTypeThroughNodeDefinition covers a
// node_definition_id reference that omits type: the reported schema must be
// the referenced definition's type, resolved the way Materialize does.
func TestSchemasResolvesReferenceTypeThroughNodeDefinition(t *testing.T) {
	ndRepo := newFakeNodeRepo()
	ndRepo.defs[nodeDefA] = model.NodeDefinition{
		ID:      nodeDefA,
		Name:    "stored-output",
		Type:    "output",
		Content: json.RawMessage(`{"type":"output","channel":"redis","context_path":"x"}`),
	}
	svc := newWorkflowService(newFakeWorkflowRepo(), ndRepo)
	content := `{
      "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
      "nodes": [
        {"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "node_definition_id": "` + nodeDefA + `"}
      ]
    }`
	got := schemasFor(t, svc, content)
	if len(got) != 1 {
		t.Fatalf("Schemas() has %d types (%v), want 1", len(got), schemaKeys(got))
	}
	if _, ok := got["output"]; !ok {
		t.Errorf("Schemas() = %v, want the referenced type output", schemaKeys(got))
	}
}

// TestSchemasDegradesOnUnparseableContent covers the read that must never
// fail: content the current parser rejects still yields the declared types.
func TestSchemasDegradesOnUnparseableContent(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	// A script node with no script no longer parses, but the read still
	// reports the type the raw content declares.
	content := `{
      "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
      "nodes": [
        {"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "script"}
      ]
    }`
	got := schemasFor(t, svc, content)
	if _, ok := got["script"]; !ok {
		t.Errorf("Schemas() = %v, want degraded type script", schemaKeys(got))
	}
}

// TestSchemasDegradesOnUnresolvableReference covers a reference whose
// definition is missing: the read reports what it can and does not fail.
func TestSchemasDegradesOnUnresolvableReference(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), newFakeNodeRepo())
	content := `{
      "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
      "nodes": [
        {"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "poller", "node_definition_id": "` + nodeDefA + `"}
      ]
    }`
	got := schemasFor(t, svc, content)
	if _, ok := got["poller"]; !ok {
		t.Errorf("Schemas() = %v, want declared type poller", schemaKeys(got))
	}
}

func TestSchemasGarbageContentIsEmpty(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	for _, content := range []string{``, `{oops`, `{"nodes":[]}`} {
		got := schemasFor(t, svc, content)
		if len(got) != 0 {
			t.Errorf("Schemas(%q) = %v, want empty", content, schemaKeys(got))
		}
	}
}

// TestSchemasSkipsTypesWithoutSchema covers a declared type this build
// knows nothing about: it is dropped rather than served empty.
func TestSchemasSkipsTypesWithoutSchema(t *testing.T) {
	svc := newWorkflowService(newFakeWorkflowRepo(), nil)
	content := `{
      "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
      "nodes": [
        {"id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", "type": "not_a_registered_type"}
      ]
    }`
	if got := schemasFor(t, svc, content); len(got) != 0 {
		t.Errorf("Schemas() = %v, want empty for an unknown type", schemaKeys(got))
	}
}

func TestNodeDefinitionServiceSchema(t *testing.T) {
	svc := service.NewNodeDefinitionService(newFakeNodeRepo(), testLimits, actorID)
	if raw := svc.Schema("script"); raw == nil {
		t.Error("Schema(script) = nil, want schema")
	}
	if raw := svc.Schema("not_a_registered_type"); raw != nil {
		t.Errorf("Schema(unknown) = %s, want nil", raw)
	}
}

func schemaKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
