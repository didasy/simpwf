package model_test

import (
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func TestParseParallelStartNode(t *testing.T) {
	nc, err := model.ParseNodeContent([]byte(`{
		"type": "parallel_start",
		"id": "01950000-0000-7000-8000-000000000001",
		"branches": {"b": "01950000-0000-7000-8000-000000000002", "a": "01950000-0000-7000-8000-000000000003"},
		"parallel_end_node_id": "01950000-0000-7000-8000-000000000004"
	}`), model.NodeLimits{})
	if err != nil {
		t.Fatalf("parse parallel_start: %v", err)
	}
	if nc.Type != model.NodeTypeParallelStart {
		t.Fatalf("type = %q", nc.Type)
	}
	if len(nc.ParallelBranches) != 2 || nc.ParallelEndNodeID == "" {
		t.Fatalf("branches/end not parsed: %+v", nc)
	}
}

func TestParseParallelEndNode(t *testing.T) {
	nc, err := model.ParseNodeContent([]byte(`{
		"type": "parallel_end",
		"id": "01950000-0000-7000-8000-000000000004",
		"combining_script": "context.x = branch[\"a\"].context.x"
	}`), model.NodeLimits{})
	if err != nil {
		t.Fatalf("parse parallel_end: %v", err)
	}
	if nc.Type != model.NodeTypeParallelEnd || nc.CombiningScript == "" {
		t.Fatalf("combining script not parsed: %+v", nc)
	}
}

func TestParseParallelNodesRejectInvalid(t *testing.T) {
	cases := map[string]string{
		"start missing branches": `{"type": "parallel_start", "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"start one branch":       `{"type": "parallel_start", "branches": {"a": "01950000-0000-7000-8000-000000000002"}, "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"start blank name":       `{"type": "parallel_start", "branches": {"a": "01950000-0000-7000-8000-000000000002", " ": "01950000-0000-7000-8000-000000000003"}, "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"start bad target":       `{"type": "parallel_start", "branches": {"a": "nope", "b": "01950000-0000-7000-8000-000000000003"}, "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"start missing end":      `{"type": "parallel_start", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}}`,
		"start bad end":          `{"type": "parallel_start", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}, "parallel_end_node_id": "nope"}`,
		"start with next_node":   `{"type": "parallel_start", "next_node": "01950000-0000-7000-8000-000000000005", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}, "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"start with script":      `{"type": "parallel_start", "combining_script": "context.x = 1", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}, "parallel_end_node_id": "01950000-0000-7000-8000-000000000004"}`,
		"end missing script":     `{"type": "parallel_end"}`,
		"end blank script":       `{"type": "parallel_end", "combining_script": "  "}`,
		"end with branches":      `{"type": "parallel_end", "combining_script": "context.x = 1", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}}`,
		"script with branches":   `{"type": "script", "script": "context.x = 1", "branches": {"a": "01950000-0000-7000-8000-000000000002", "b": "01950000-0000-7000-8000-000000000003"}}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := model.ParseNodeContent([]byte(raw), model.NodeLimits{}); err == nil {
				t.Fatalf("expected error, got nil")
			} else if !strings.Contains(err.Error(), "parallel") && !strings.Contains(err.Error(), "branches") && !strings.Contains(err.Error(), "combining_script") && !strings.Contains(err.Error(), "next_node") {
				t.Fatalf("error %q does not name the offending field", err)
			}
		})
	}
}

func TestCanParallelExecutionTransition(t *testing.T) {
	cases := map[model.ParallelExecutionStatus][]model.ParallelExecutionStatus{
		model.ParallelWaitingForBranches: {model.ParallelReadyToJoin, model.ParallelExecutionFailed, model.ParallelExecutionCancelled},
		model.ParallelReadyToJoin:        {model.ParallelExecutionCompleted, model.ParallelExecutionFailed, model.ParallelExecutionCancelled},
	}
	all := []model.ParallelExecutionStatus{
		model.ParallelWaitingForBranches, model.ParallelReadyToJoin,
		model.ParallelExecutionCompleted, model.ParallelExecutionFailed, model.ParallelExecutionCancelled,
	}
	for _, from := range all {
		for _, to := range all {
			want := false
			for _, ok := range cases[from] {
				if ok == to {
					want = true
				}
			}
			if got := model.CanParallelExecutionTransition(from, to); got != want {
				t.Errorf("execution %s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestCanParallelBranchTransition(t *testing.T) {
	cases := map[model.ParallelBranchStatus][]model.ParallelBranchStatus{
		model.ParallelBranchPending: {model.ParallelBranchRunning, model.ParallelBranchFailed, model.ParallelBranchCancelled},
		model.ParallelBranchRunning: {model.ParallelBranchWaiting, model.ParallelBranchCompleted, model.ParallelBranchFailed, model.ParallelBranchCancelled},
		model.ParallelBranchWaiting: {model.ParallelBranchRunning, model.ParallelBranchFailed, model.ParallelBranchCancelled},
	}
	all := []model.ParallelBranchStatus{
		model.ParallelBranchPending, model.ParallelBranchRunning, model.ParallelBranchWaiting,
		model.ParallelBranchCompleted, model.ParallelBranchFailed, model.ParallelBranchCancelled,
	}
	for _, from := range all {
		for _, to := range all {
			want := false
			for _, ok := range cases[from] {
				if ok == to {
					want = true
				}
			}
			if got := model.CanParallelBranchTransition(from, to); got != want {
				t.Errorf("branch %s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestValidWaitingReasonParallel(t *testing.T) {
	if !model.ValidWaitingReason(model.WaitingReasonParallel) {
		t.Fatalf("parallel waiting reason is not valid")
	}
	if string(model.WaitingReasonParallel) != "parallel" {
		t.Fatalf("parallel waiting reason = %q", model.WaitingReasonParallel)
	}
	if !model.ValidWaitingReason(model.WaitingReasonPaused) {
		t.Fatalf("paused waiting reason is not valid")
	}
	if string(model.WaitingReasonPaused) != "paused" {
		t.Fatalf("paused waiting reason = %q", model.WaitingReasonPaused)
	}
}

func TestParallelLimitsDefault(t *testing.T) {
	lim := model.DefaultParallelLimits()
	if lim.MaxDepth != 4 || lim.MaxBranchesPerParallel != 32 {
		t.Fatalf("defaults = %+v", lim)
	}
}

func parallelWorkflowDoc(start string, nodes ...string) string {
	return parallelWorkflowDocWithKeys(start, "", nodes...)
}

func parallelWorkflowDocWithKeys(start, keys string, nodes ...string) string {
	doc := `{"start_node_id": "` + start + `"`
	if keys != "" {
		doc += `, "keys": ` + keys
	}
	doc += `, "nodes": [`
	for i, n := range nodes {
		if i > 0 {
			doc += ","
		}
		doc += n
	}
	return doc + `]}`
}

const (
	parallelStartID = "01950000-0000-7000-8000-000000000001"
	parallelAID     = "01950000-0000-7000-8000-000000000002"
	parallelBID     = "01950000-0000-7000-8000-000000000003"
	parallelEndID   = "01950000-0000-7000-8000-000000000004"
	parallelAfterID = "01950000-0000-7000-8000-000000000005"
)

func validParallelDoc() string {
	return parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		`{"id": "`+parallelAID+`", "type": "script", "script": "context.a = 1", "next_node": "`+parallelEndID+`"}`,
		`{"id": "`+parallelBID+`", "type": "script", "script": "context.b = 1", "next_node": "`+parallelEndID+`"}`,
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1", "next_node": "`+parallelAfterID+`"}`,
		`{"id": "`+parallelAfterID+`", "type": "script", "script": "context.done = true"}`,
	)
}

func TestParseWorkflowContentAcceptsValidParallel(t *testing.T) {
	wc, err := model.ParseWorkflowContent([]byte(validParallelDoc()), model.NodeLimits{})
	if err != nil {
		t.Fatalf("valid parallel workflow rejected: %v", err)
	}
	if len(wc.Nodes) != 5 {
		t.Fatalf("nodes = %d", len(wc.Nodes))
	}
}

func TestParseWorkflowContentRejectsInvalidParallel(t *testing.T) {
	unknown := "01950000-0000-7000-8000-000000000099"
	end2 := "01950000-0000-7000-8000-000000000098"
	start := func(branches, end string) string {
		return `{"id": "` + parallelStartID + `", "type": "parallel_start", "branches": ` + branches + `, "parallel_end_node_id": "` + end + `"}`
	}
	branches := `{"a": "` + parallelAID + `", "b": "` + parallelBID + `"}`
	scriptA := `{"id": "` + parallelAID + `", "type": "script", "script": "context.a = 1", "next_node": "` + parallelEndID + `"}`
	scriptB := `{"id": "` + parallelBID + `", "type": "script", "script": "context.b = 1", "next_node": "` + end2 + `"}`
	end := func(id, next string) string {
		return `{"id": "` + id + `", "type": "parallel_end", "combining_script": "context.x = 1", "next_node": "` + next + `"}`
	}
	after := `{"id": "` + parallelAfterID + `", "type": "script", "script": "context.done = true"}`
	cases := map[string][]string{
		"branch target unknown": {start(`{"a": "`+unknown+`", "b": "`+parallelBID+`"}`, parallelEndID), scriptA, scriptB, end(parallelEndID, parallelAfterID), after},
		"end unknown":           {start(branches, unknown), scriptA, scriptB, end(parallelEndID, parallelAfterID), after},
		"end wrong type":        {start(branches, parallelAID), scriptA, scriptB, end(parallelEndID, parallelAfterID), after},
		"end unreferenced":      {start(branches, end2), scriptA, scriptB, end(parallelEndID, parallelAfterID), end(end2, parallelAfterID), after},
	}
	for name, nodes := range cases {
		t.Run(name, func(t *testing.T) {
			doc := parallelWorkflowDoc(parallelStartID, nodes...)
			if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

const (
	parallelCID = "01950000-0000-7000-8000-000000000006"
	parallelDID = "01950000-0000-7000-8000-000000000007"
	parallelE2  = "01950000-0000-7000-8000-000000000008"
	parallelS2  = "01950000-0000-7000-8000-000000000009"
	parallelG0  = "01950000-0000-7000-8000-000000000010"
	parallelG1  = "01950000-0000-7000-8000-000000000011"
	parallelYID = "01950000-0000-7000-8000-000000000012"
)

func TestParseWorkflowContentAcceptsUnevenBranches(t *testing.T) {
	doc := parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelEndID),
		scriptNode(parallelBID, parallelCID),
		scriptNode(parallelCID, parallelDID),
		scriptNode(parallelDID, parallelEndID),
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err != nil {
		t.Fatalf("uneven branches rejected: %v", err)
	}
}

func TestParseWorkflowContentAcceptsBranchLoopWithExit(t *testing.T) {
	doc := parallelWorkflowDocWithKeys(parallelStartID,
		`{"retry": "`+parallelBID+`", "done": "`+parallelEndID+`"}`,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelEndID),
		scriptNode(parallelBID, parallelCID),
		`{"id": "`+parallelCID+`", "type": "conditions", "conditions": [{"condition": "context.x", "key": "retry"}, {"condition": "!context.x", "key": "done"}]}`,
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err != nil {
		t.Fatalf("branch loop with exit rejected: %v", err)
	}
}

func TestParseWorkflowContentRejectsBranchWithoutJoinPath(t *testing.T) {
	terminates := parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, ""),
		scriptNode(parallelBID, parallelEndID),
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	loops := parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelCID),
		scriptNode(parallelCID, parallelAID),
		scriptNode(parallelBID, parallelEndID),
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	selfFork := parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelStartID),
		scriptNode(parallelBID, parallelEndID),
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	for name, doc := range map[string]string{"terminates": terminates, "closed loop": loops, "self fork": selfFork} {
		t.Run(name, func(t *testing.T) {
			if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func nestedParallelDoc() string {
	return parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelBID+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelEndID),
		scriptNode(parallelBID, parallelS2),
		`{"id": "`+parallelS2+`", "type": "parallel_start", "branches": {"c": "`+parallelCID+`", "d": "`+parallelDID+`"}, "parallel_end_node_id": "`+parallelE2+`"}`,
		scriptNode(parallelCID, parallelE2),
		scriptNode(parallelDID, parallelE2),
		`{"id": "`+parallelE2+`", "type": "parallel_end", "combining_script": "context.y = 1", "next_node": "`+parallelEndID+`"}`,
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
}

func TestParseWorkflowContentAcceptsNestedParallel(t *testing.T) {
	if _, err := model.ParseWorkflowContent([]byte(nestedParallelDoc()), model.NodeLimits{}); err != nil {
		t.Fatalf("nested parallel rejected: %v", err)
	}
}

func TestParseWorkflowContentRejectsDepthOverLimit(t *testing.T) {
	limits := model.NodeLimits{Parallel: model.ParallelLimits{MaxDepth: 1}}
	if _, err := model.ParseWorkflowContent([]byte(nestedParallelDoc()), limits); err == nil {
		t.Fatalf("expected depth error, got nil")
	}
}

func TestParseWorkflowContentAcceptsGroupInsideBranch(t *testing.T) {
	doc := parallelWorkflowDoc(parallelStartID,
		`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelG1+`"}, "parallel_end_node_id": "`+parallelEndID+`"}`,
		scriptNode(parallelAID, parallelEndID),
		`{"id": "`+parallelG1+`", "type": "group", "start_node_id": "`+parallelYID+`", "next_node": "`+parallelEndID+`", "nodes": [`+scriptNode(parallelYID, "")+`]}`,
		`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`,
	)
	if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err != nil {
		t.Fatalf("group inside branch rejected: %v", err)
	}
}

func TestParseWorkflowContentRejectsBranchEscapingScope(t *testing.T) {
	// S lives in G0; branch b enters G1 whose exit pops below S's base
	// stack (G1 has no next, G0's next leaves the scope).
	doc := parallelWorkflowDoc(parallelG0,
		`{"id": "`+parallelG0+`", "type": "group", "start_node_id": "`+parallelStartID+`", "next_node": "`+parallelAfterID+`", "nodes": [`+
			`{"id": "`+parallelStartID+`", "type": "parallel_start", "branches": {"a": "`+parallelAID+`", "b": "`+parallelG1+`"}, "parallel_end_node_id": "`+parallelEndID+`"},`+
			scriptNode(parallelAID, parallelEndID)+`,`+
			`{"id": "`+parallelG1+`", "type": "group", "start_node_id": "`+parallelYID+`", "nodes": [`+scriptNode(parallelYID, "")+`]},`+
			`{"id": "`+parallelEndID+`", "type": "parallel_end", "combining_script": "context.x = 1"}`+
			`]}`,
		scriptNode(parallelAfterID, ""),
	)
	if _, err := model.ParseWorkflowContent([]byte(doc), model.NodeLimits{}); err == nil {
		t.Fatalf("expected scope-escape error, got nil")
	}
}

func TestParseParallelStartRejectsTooManyBranches(t *testing.T) {
	limits := model.NodeLimits{Parallel: model.ParallelLimits{MaxDepth: 4, MaxBranchesPerParallel: 2}}
	_, err := model.ParseNodeContent([]byte(`{
		"type": "parallel_start",
		"branches": {
			"a": "01950000-0000-7000-8000-000000000002",
			"b": "01950000-0000-7000-8000-000000000003",
			"c": "01950000-0000-7000-8000-000000000004"
		},
		"parallel_end_node_id": "01950000-0000-7000-8000-000000000005"
	}`), limits)
	if err == nil {
		t.Fatalf("expected too-many-branches error, got nil")
	}
}
