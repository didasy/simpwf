package engine_test

import (
	"fmt"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// mapGraph is a test graph over hardcoded node ids.
type mapGraph struct {
	types map[string]model.NodeType
	next  map[string]string
	start map[string]string
}

func (g mapGraph) TypeOf(id string) (model.NodeType, error) {
	t, ok := g.types[id]
	if !ok {
		return "", fmt.Errorf("unknown node %q", id)
	}
	return t, nil
}

func (g mapGraph) NextOf(id string) (string, error) {
	if _, ok := g.types[id]; !ok {
		return "", fmt.Errorf("unknown node %q", id)
	}
	return g.next[id], nil
}

func (g mapGraph) StartOf(id string) (string, error) {
	s, ok := g.start[id]
	if !ok {
		return "", fmt.Errorf("node %q is not a group", id)
	}
	return s, nil
}

func newGraph() mapGraph {
	return mapGraph{
		types: map[string]model.NodeType{
			"a": model.NodeTypeScript, "b": model.NodeTypeScript, "c": model.NodeTypeScript,
			"g1": model.NodeTypeGroup, "g2": model.NodeTypeGroup,
			"g1n1": model.NodeTypeScript, "g1n2": model.NodeTypeScript,
			"g2n1": model.NodeTypeScript,
		},
		next: map[string]string{
			"a": "b", "b": "c", "c": "",
			"g1": "", "g2": "",
			"g1n1": "g1n2", "g1n2": "", "g2n1": "",
		},
		start: map[string]string{"g1": "g1n1", "g2": "g2n1"},
	}
}

func TestEnterGroup(t *testing.T) {
	g := newGraph()
	frame := model.NewFrame("a")
	next, err := engine.EnterGroup(&frame, g, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if next != "g1n1" {
		t.Errorf("next = %q, want g1n1", next)
	}
	if frame.CurrentNodeID != "g1n1" || !frame.InGroup() || frame.CurrentGroupID() != "g1" {
		t.Errorf("frame = %+v", frame)
	}
}

func TestEnterGroupUnknownStart(t *testing.T) {
	g := newGraph()
	frame := model.NewFrame("g1")
	if _, err := engine.EnterGroup(&frame, g, "ghost-group"); err == nil {
		t.Error("EnterGroup() error = nil, want error for unknown group")
	}
}

func TestAdvanceChain(t *testing.T) {
	g := newGraph()
	frame := model.NewFrame("a")
	done, _, err := engine.Advance(&frame, g, "b")
	if err != nil || done {
		t.Fatalf("Advance() = done %v, err %v", done, err)
	}
	if frame.CurrentNodeID != "b" {
		t.Errorf("current = %q, want b", frame.CurrentNodeID)
	}
	done, _, err = engine.Advance(&frame, g, "c")
	if err != nil || done {
		t.Fatalf("Advance() = done %v, err %v", done, err)
	}
	if frame.CurrentNodeID != "c" {
		t.Errorf("current = %q, want c", frame.CurrentNodeID)
	}
}

func TestAdvanceFinishesAtTopLevel(t *testing.T) {
	g := newGraph()
	frame := model.NewFrame("c")
	done, _, err := engine.Advance(&frame, g, "")
	if err != nil {
		t.Fatal(err)
	}
	if !done || frame.CurrentNodeID != "" || frame.InGroup() {
		t.Errorf("frame = %+v, done = %v, want finished", frame, done)
	}
}

func TestAdvanceExitsGroupOnEmptyNext(t *testing.T) {
	g := newGraph()
	// g1.next = "" (top level link of the group); g1n2 has no next -> exit g1 -> finish.
	frame := model.NewFrame("g1n2")
	frame.GroupStack = []string{"g1"}
	done, exited, err := engine.Advance(&frame, g, "")
	if err != nil {
		t.Fatal(err)
	}
	if !done || frame.InGroup() {
		t.Errorf("frame = %+v, done = %v, want finished after popping g1", frame, done)
	}
	if len(exited) != 1 || exited[0] != "g1" {
		t.Errorf("exited = %v, want [g1]", exited)
	}
}

func TestAdvanceFollowsGroupLinkAfterExit(t *testing.T) {
	g := newGraph()
	g.next["g1"] = "b" // group link continues to b at top level
	frame := model.NewFrame("g1n2")
	frame.GroupStack = []string{"g1"}
	done, _, err := engine.Advance(&frame, g, "")
	if err != nil {
		t.Fatal(err)
	}
	if done || frame.CurrentNodeID != "b" {
		t.Errorf("frame = %+v, done = %v, want current b", frame, done)
	}
}

func TestAdvancePopsNestedGroups(t *testing.T) {
	g := newGraph()
	// top: g1 -> g2; g2.next = "" -> after g2 finishes, pop to g1, then pop to finish.
	g.next["g1"] = ""
	g.start["g1"] = "g1n1"
	g.next["g1n1"] = "g2"
	g.start["g2"] = "g2n1"
	frame := model.NewFrame("g2n1")
	frame.GroupStack = []string{"g1", "g2"}
	done, exited, err := engine.Advance(&frame, g, "")
	if err != nil {
		t.Fatal(err)
	}
	if !done || frame.InGroup() || frame.CurrentNodeID != "" {
		t.Errorf("frame = %+v, done = %v, want both groups popped and finished", frame, done)
	}
	if len(exited) != 2 || exited[0] != "g2" || exited[1] != "g1" {
		t.Errorf("exited = %v, want [g2 g1]", exited)
	}
}

func TestAdvanceConditionTrueEndsGroup(t *testing.T) {
	g := newGraph()
	// A condition inside g1 matched with no target -> same as empty next: exit g1.
	g.next["g1"] = "b"
	frame := model.NewFrame("g1n1")
	frame.GroupStack = []string{"g1"}
	done, _, err := engine.Advance(&frame, g, "")
	if err != nil {
		t.Fatal(err)
	}
	if done || frame.CurrentNodeID != "b" {
		t.Errorf("frame = %+v, done = %v, want continue at b after group exit", frame, done)
	}
}

func TestAdvanceUnknownGroupLink(t *testing.T) {
	g := newGraph()
	// A group id not present in the graph surfaces a NextOf error on exit.
	frame := model.NewFrame("g1n2")
	frame.GroupStack = []string{"ghost-group"}
	if _, _, err := engine.Advance(&frame, g, ""); err == nil {
		t.Error("Advance() error = nil, want error for unknown group")
	}
}
