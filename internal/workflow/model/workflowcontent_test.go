package model_test

import (
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const (
	topA   = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"
	topB   = "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
	topC   = "cccccccc-cccc-7ccc-8ccc-cccccccccccc"
	childD = "dddddddd-dddd-7ddd-8ddd-dddddddddddd"
	childE = "eeeeeeee-eeee-7eee-8eee-eeeeeeeeeeee"
)

func wfJSON(nodes string, start string) string {
	if start == "" {
		start = `""`
	}
	return `{"start_node_id": "` + start + `", "nodes": [` + nodes + `]}`
}

func scriptNode(id, next string) string {
	s := `{"id": "` + id + `", "type": "script", "script": "return 1;"`
	if next != "" {
		s += `, "next_node": "` + next + `"`
	}
	return s + `}`
}

func mustParseWorkflow(t *testing.T, raw string) *model.WorkflowContent {
	t.Helper()
	wc, err := model.ParseWorkflowContent([]byte(raw), testLimits)
	if err != nil {
		t.Fatalf("ParseWorkflowContent(%s) error = %v", raw, err)
	}
	return wc
}

func TestParseWorkflowContentBasic(t *testing.T) {
	wc := mustParseWorkflow(t, wfJSON(scriptNode(topA, topB)+","+scriptNode(topB, ""), topA))
	if wc.StartNodeID != topA {
		t.Errorf("start = %q, want %s", wc.StartNodeID, topA)
	}
	if len(wc.Nodes) != 2 {
		t.Fatalf("nodes len = %d, want 2", len(wc.Nodes))
	}
	if wc.Nodes[0].NextNode != topB {
		t.Errorf("nodes[0].next_node = %q", wc.Nodes[0].NextNode)
	}
}

func TestParseWorkflowContentRejectsInvalid(t *testing.T) {
	bad := []string{
		`{}`,                                // missing start and nodes
		`{"start_node_id": "` + topA + `"}`, // no nodes
		`{"start_node_id": "` + topA + `", "nodes": [` + scriptNode(topB, "") + `]}`,                                     // start not among nodes
		`{"start_node_id": "x", "nodes": [` + scriptNode(topB, "") + `]}`,                                                // bad start uuid
		`{"start_node_id": "` + topA + `", "nodes": [` + scriptNode(topA, "") + `, ` + scriptNode(topA, "") + `]}`,       // duplicate top-level ids
		`{"start_node_id": "` + topA + `", "nodes": [` + scriptNode(topA, "22222222-2222-7222-8222-222222222222") + `]}`, // next_node outside group
	}
	for _, raw := range bad {
		if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseWorkflowContent(%s) error = nil, want error", raw)
		}
	}
}

func TestParseWorkflowContentNestedGroup(t *testing.T) {
	groupNode := `{"id": "` + topB + `", "type": "group", "start_node_id": "` + childD + `",
		"nodes": [
			{"id": "` + childD + `", "type": "script", "script": "return 1;", "next_node": "` + childE + `"},
			{"id": "` + childE + `", "type": "script", "script": "return 2;"}
		], "next_node": "` + topC + `"}`
	raw := wfJSON(scriptNode(topA, topB)+","+groupNode+","+scriptNode(topC, ""), topA)
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[1].Group == nil {
		t.Fatal("group not parsed")
	}
	if len(wc.Nodes[1].Group.Nodes) != 2 {
		t.Fatalf("group nodes len = %d, want 2", len(wc.Nodes[1].Group.Nodes))
	}
}

func TestParseWorkflowContentRejectsCrossGroupLinks(t *testing.T) {
	// group child next_node pointing at a top-level node must fail
	groupNode := `{"id": "` + topB + `", "type": "group", "start_node_id": "` + childD + `",
		"nodes": [
			{"id": "` + childD + `", "type": "script", "script": "return 1;", "next_node": "` + topC + `"}
		]}`
	raw := wfJSON(scriptNode(topA, topB)+","+groupNode+","+scriptNode(topC, ""), topA)
	if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
		t.Error("cross-group next_node error = nil, want error")
	}
}

func TestParseWorkflowContentRejectsDuplicateNestedID(t *testing.T) {
	groupNode := `{"id": "` + topB + `", "type": "group", "start_node_id": "` + childD + `",
		"nodes": [
			{"id": "` + childD + `", "type": "script", "script": "return 1;"},
			{"id": "` + topA + `", "type": "script", "script": "return 2;"}
		]}`
	raw := wfJSON(scriptNode(topA, topB)+","+groupNode, topA)
	if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
		t.Error("duplicate id across nesting levels error = nil, want error")
	}
}

func TestParseWorkflowContentNodeDefinitionReference(t *testing.T) {
	raw := `{"start_node_id": "` + topA + `", "nodes": [
		{"id": "` + topA + `", "node_definition_id": "99999999-9999-7999-8999-999999999999", "next_node": "` + topB + `"},
		{"id": "` + topB + `", "type": "script", "script": "return 1;"}
	]}`
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[0].NodeDefinitionID != "99999999-9999-7999-8999-999999999999" {
		t.Errorf("node_definition_id = %q", wc.Nodes[0].NodeDefinitionID)
	}
	if wc.Nodes[0].Type != "" {
		t.Errorf("type = %q, want empty (resolved from definition)", wc.Nodes[0].Type)
	}
}

func TestParseWorkflowContentConditionKeys(t *testing.T) {
	raw := `{"start_node_id":"` + topA + `","keys":{
			"a":"` + topB + `",
			"exit":"",
			"null_exit":null,
			"future":"` + topC + `"
		},"nodes":[
		{"id":"` + topA + `","type":"conditions","conditions":[
			{"key":"a","condition":"return true;"},
			{"key":"exit","condition":"return false;"},
			{"key":"null_exit","condition":"return false;"},
			{"condition":"return false;"}
		]},
		` + scriptNode(topB, "") + `,
		` + scriptNode(topC, "") + `
	]}`
	wc := mustParseWorkflow(t, raw)
	if wc.Keys["a"] != topB {
		t.Errorf("key a = %q, want %s", wc.Keys["a"], topB)
	}
	for _, key := range []string{"exit", "null_exit"} {
		if target, ok := wc.Keys[key]; !ok || target != "" {
			t.Errorf("key %q = %q, present %v; want exit target", key, target, ok)
		}
	}
}

func TestParseWorkflowContentConditionRequiresDefinedKey(t *testing.T) {
	raw := `{"start_node_id":"` + topA + `","keys":{"a":"` + topB + `"},"nodes":[
		{"id":"` + topA + `","type":"conditions","conditions":[
			{"key":"a","condition":"return true;"},
			{"key":"missing","condition":"return false;"}
		]},
		` + scriptNode(topB, "") + `
	]}`
	if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
		t.Fatal("missing workflow key error = nil, want error")
	}
}

func TestParseWorkflowContentGroupUsesLocalKeys(t *testing.T) {
	groupNode := `{"id":"` + topB + `","type":"group","start_node_id":"` + childD + `","keys":{
		"inside":"` + childE + `",
		"exit":null
	},"nodes":[
		{"id":"` + childD + `","type":"conditions","conditions":[
			{"key":"inside","condition":"return true;"},
			{"key":"exit","condition":"return false;"}
		]},
		` + scriptNode(childE, "") + `
	]}`
	raw := wfJSON(scriptNode(topA, topB)+","+groupNode+","+scriptNode(topC, ""), topA)
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[1].Group.Keys["inside"] != childE {
		t.Errorf("group key inside = %q, want %s", wc.Nodes[1].Group.Keys["inside"], childE)
	}
}

func TestParseWorkflowContentRejectsCrossScopeKeyTarget(t *testing.T) {
	groupNode := `{"id":"` + topB + `","type":"group","start_node_id":"` + childD + `","keys":{
		"outside":"` + topC + `"
	},"nodes":[
		{"id":"` + childD + `","type":"conditions","conditions":[
			{"key":"outside","condition":"return true;"},
			{"condition":"return false;"}
		]}
	]}`
	raw := wfJSON(scriptNode(topA, topB)+","+groupNode+","+scriptNode(topC, ""), topA)
	if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
		t.Fatal("cross-scope key target error = nil, want error")
	}
}

func TestParseWorkflowContentRejectsInvalidKeys(t *testing.T) {
	bad := []string{
		`{"start_node_id":"` + topA + `","keys":{" ":"` + topA + `"},"nodes":[` + scriptNode(topA, "") + `]}`,
		`{"start_node_id":"` + topA + `","keys":{"bad":"x"},"nodes":[` + scriptNode(topA, "") + `]}`,
		`{"start_node_id":"` + topA + `","nodes":[{"id":"` + topA + `","type":"script","script":"return 1;","keys":{}}]}`,
		`{"start_node_id":"` + topA + `","nodes":[{"id":"` + topA + `","type":"conditions","conditions":[{"condition":"return true;"},{"condition":"return false;"}],"branches":{}}]}`,
	}
	for _, raw := range bad {
		if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseWorkflowContent(%s) error = nil, want error", raw)
		}
	}
}

func pollerNode(id, next string) string {
	s := `{"id": "` + id + `", "type": "poller", "http": {"url": "https://example.com/jobs", "until": "return response.status === 200;"}`
	if next != "" {
		s += `, "next_node": "` + next + `"`
	}
	return s + `}`
}

func TestParseWorkflowContentNodeWithHooks(t *testing.T) {
	raw := wfJSON(`{"id": "`+topA+`", "type": "script", "script": "return 1;", "pre_script": {"script": "context.p = 1;"}, "post_script": {"script": "context.q = 1;"}}`+","+scriptNode(topB, ""), topA)
	wc := mustParseWorkflow(t, raw)
	n := wc.Nodes[0]
	if n.PreScript == nil || n.PreScript.Script != "context.p = 1;" {
		t.Errorf("workflow node pre_script not parsed: %+v", n.PreScript)
	}
	if n.PostScript == nil || n.PostScript.Script != "context.q = 1;" {
		t.Errorf("workflow node post_script not parsed: %+v", n.PostScript)
	}
}

func TestParseWorkflowContentPollerNode(t *testing.T) {
	raw := wfJSON(pollerNode(topA, topB)+","+scriptNode(topB, ""), topA)
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[0].Type != model.NodeTypePoller {
		t.Errorf("type = %s, want poller", wc.Nodes[0].Type)
	}
	if wc.Nodes[0].PollerHTTP == nil {
		t.Error("poller http block not parsed in workflow context")
	}
}

func TestParseWorkflowContentPollerNodeDefinitionReference(t *testing.T) {
	raw := `{"start_node_id": "` + topA + `", "nodes": [
		{"id": "` + topA + `", "type": "poller", "node_definition_id": "99999999-9999-7999-8999-999999999999", "next_node": "` + topB + `"},
		{"id": "` + topB + `", "type": "poller", "http": {"url": "https://example.com/x", "until": "return true;"}}
	]}`
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[0].Type != model.NodeTypePoller {
		t.Errorf("referenced node type = %q, want poller", wc.Nodes[0].Type)
	}
}

func TestParseWorkflowContentPollerNodeDefinitionRejectsInline(t *testing.T) {
	raw := `{"start_node_id": "` + topA + `", "nodes": [
		{"id": "` + topA + `", "node_definition_id": "99999999-9999-7999-8999-999999999999", "http": {"url": "https://example.com/x", "until": "return true;"}},
		{"id": "` + topB + `", "type": "script", "script": "return 1;"}
	]}`
	if _, err := model.ParseWorkflowContent([]byte(raw), testLimits); err == nil {
		t.Fatal("node_definition_id with inline poller http block error = nil, want error")
	}
}

func TestParseWorkflowContentOnFailureRouting(t *testing.T) {
	// Valid same-scope on_failure target
	raw := `{"start_node_id":"` + topA + `","nodes":[
		{"id":"` + topA + `","type":"external_call","http_config":{"url":"https://example.com"},"next_node":"` + topB + `","on_failure":{"next_node":"` + topC + `","output_property":"err"}},
		{"id":"` + topB + `","type":"script","script":"return 1;"},
		{"id":"` + topC + `","type":"input","channel":"http","context_path":"fix"}
	]}`
	wc := mustParseWorkflow(t, raw)
	if wc.Nodes[0].OnFailure == nil || wc.Nodes[0].OnFailure.NextNode != topC {
		t.Fatalf("on_failure not parsed in workflow: %+v", wc.Nodes[0].OnFailure)
	}

	// Missing target in same scope
	badMissing := `{"start_node_id":"` + topA + `","nodes":[
		{"id":"` + topA + `","type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"next_node":"99999999-9999-7999-8999-999999999999","output_property":"err"}}
	]}`
	if _, err := model.ParseWorkflowContent([]byte(badMissing), testLimits); err == nil {
		t.Error("missing on_failure target error = nil, want error")
	}

	// Cross-scope target from group child to parent
	groupWithChildFailure := `{"id":"` + topB + `","type":"group","start_node_id":"` + childD + `","nodes":[
		{"id":"` + childD + `","type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"next_node":"` + topC + `","output_property":"err"}},
		{"id":"` + childE + `","type":"script","script":"return 1;"}
	],"keys":{}}`
	rawGroupCross := wfJSON(scriptNode(topA, topB)+","+groupWithChildFailure+","+scriptNode(topC, ""), topA)
	if _, err := model.ParseWorkflowContent([]byte(rawGroupCross), testLimits); err == nil {
		t.Error("cross-scope on_failure target error = nil, want error")
	}
}
