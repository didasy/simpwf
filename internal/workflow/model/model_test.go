package model_test

import (
	"errors"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func TestWorkflowStatusValidity(t *testing.T) {
	for _, s := range []model.WorkflowStatus{
		model.WorkflowWaiting,
		model.WorkflowRunning,
		model.WorkflowPaused,
		model.WorkflowFinished,
		model.WorkflowFailed,
		model.WorkflowStopped,
	} {
		if !model.ValidWorkflowStatus(s) {
			t.Errorf("ValidWorkflowStatus(%q) = false, want true", s)
		}
	}
	for _, s := range []model.WorkflowStatus{"", "done", "paused "} {
		if model.ValidWorkflowStatus(s) {
			t.Errorf("ValidWorkflowStatus(%q) = true, want false", s)
		}
	}
}

func TestWorkflowTransitions(t *testing.T) {
	allowed := map[[2]model.WorkflowStatus]bool{
		{model.WorkflowWaiting, model.WorkflowRunning}:  true, // claimed
		{model.WorkflowRunning, model.WorkflowWaiting}:  true, // checkpoint
		{model.WorkflowWaiting, model.WorkflowPaused}:   true, // pause
		{model.WorkflowRunning, model.WorkflowPaused}:   true, // pause after node
		{model.WorkflowPaused, model.WorkflowWaiting}:   true, // resume
		{model.WorkflowWaiting, model.WorkflowStopped}:  true, // stop
		{model.WorkflowRunning, model.WorkflowStopped}:  true, // force stop
		{model.WorkflowPaused, model.WorkflowStopped}:   true, // stop
		{model.WorkflowRunning, model.WorkflowFinished}: true,
		{model.WorkflowRunning, model.WorkflowFailed}:   true,
	}

	for from, transitions := range map[model.WorkflowStatus][]model.WorkflowStatus{
		model.WorkflowWaiting:  {model.WorkflowRunning, model.WorkflowPaused, model.WorkflowStopped},
		model.WorkflowRunning:  {model.WorkflowWaiting, model.WorkflowPaused, model.WorkflowStopped, model.WorkflowFinished, model.WorkflowFailed},
		model.WorkflowPaused:   {model.WorkflowWaiting, model.WorkflowStopped},
		model.WorkflowFinished: {},
		model.WorkflowFailed:   {},
		model.WorkflowStopped:  {},
	} {
		for _, to := range transitions {
			if !allowed[[2]model.WorkflowStatus{from, to}] {
				t.Fatalf("test table missing transition %s -> %s", from, to)
			}
		}
	}

	for _, pair := range [][2]model.WorkflowStatus{
		{model.WorkflowWaiting, model.WorkflowFinished},
		{model.WorkflowWaiting, model.WorkflowFailed},
		{model.WorkflowPaused, model.WorkflowFinished},
		{model.WorkflowFinished, model.WorkflowWaiting},
		{model.WorkflowFailed, model.WorkflowWaiting},
		{model.WorkflowStopped, model.WorkflowWaiting},
		{model.WorkflowPaused, model.WorkflowRunning},
		{model.WorkflowWaiting, model.WorkflowPaused}, // allowed; keep the table above honest
	} {
		pair := pair
		if allowed[pair] {
			continue
		}
		if model.CanWorkflowTransition(pair[0], pair[1]) {
			t.Errorf("CanWorkflowTransition(%s, %s) = true, want false", pair[0], pair[1])
		}
	}
}

func TestNodeStatusValidity(t *testing.T) {
	for _, s := range []model.NodeStatus{
		model.NodeWaiting,
		model.NodeRunning,
		model.NodeFinished,
		model.NodeFailed,
		model.NodeStopped,
	} {
		if !model.ValidNodeStatus(s) {
			t.Errorf("ValidNodeStatus(%q) = false, want true", s)
		}
	}
	if model.ValidNodeStatus("fired") {
		t.Error("ValidNodeStatus(\"fired\") = true, want false")
	}
}

func TestNodeTransitions(t *testing.T) {
	if !model.CanNodeTransition(model.NodeWaiting, model.NodeRunning) {
		t.Error("waiting -> running should be allowed")
	}
	if !model.CanNodeTransition(model.NodeRunning, model.NodeFinished) {
		t.Error("running -> finished should be allowed")
	}
	if !model.CanNodeTransition(model.NodeRunning, model.NodeFailed) {
		t.Error("running -> failed should be allowed")
	}
	if !model.CanNodeTransition(model.NodeRunning, model.NodeStopped) {
		t.Error("running -> stopped should be allowed")
	}
	if !model.CanNodeTransition(model.NodeWaiting, model.NodeStopped) {
		t.Error("waiting -> stopped should be allowed")
	}
	if model.CanNodeTransition(model.NodeFinished, model.NodeRunning) {
		t.Error("finished -> running should be forbidden")
	}
	if model.CanNodeTransition(model.NodeWaiting, model.NodeFinished) {
		t.Error("waiting -> finished should be forbidden")
	}
}

func TestWaitingReason(t *testing.T) {
	if model.WaitingReasonRunnable != "" {
		t.Errorf("runnable reason = %q, want empty string", model.WaitingReasonRunnable)
	}
	if model.WaitingReasonInput != "input" {
		t.Errorf("input reason = %q, want input", model.WaitingReasonInput)
	}
}

func TestErrorSentinels(t *testing.T) {
	if !errors.Is(model.ErrNotFound, model.ErrNotFound) {
		t.Fatal("ErrNotFound must classify itself")
	}
	wrapped := errors.Join(model.ErrConflict, model.ErrNotFound)
	if !errors.Is(wrapped, model.ErrNotFound) {
		t.Fatal("errors.Is must unwrap joined sentinels")
	}
}

func TestNodeInstanceID(t *testing.T) {
	nid := model.NodeInstanceID{WorkflowInstanceID: "inst-1", OccurrenceID: "occ-2"}
	if got := nid.String(); got != "inst-1:occ-2" {
		t.Errorf("String() = %q, want inst-1:occ-2", got)
	}

	parsed, err := model.ParseNodeInstanceID("inst-1:occ-2")
	if err != nil {
		t.Fatalf("ParseNodeInstanceID() error = %v", err)
	}
	if parsed != nid {
		t.Errorf("ParseNodeInstanceID() = %+v, want %+v", parsed, nid)
	}

	if _, err := model.ParseNodeInstanceID("no-colon"); err == nil {
		t.Error("ParseNodeInstanceID(no-colon) error = nil, want error")
	}
	if _, err := model.ParseNodeInstanceID("a:b:c"); err == nil {
		t.Error("ParseNodeInstanceID(a:b:c) error = nil, want error")
	}
}
