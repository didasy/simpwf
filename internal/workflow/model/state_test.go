package model

import (
	"encoding/json"
	"testing"
)

func TestFrameJSONRoundTrip(t *testing.T) {
	f := Frame{CurrentNodeID: "node-1", GroupStack: []string{"g1", "g2"}}
	raw, err := f.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentNodeID != "node-1" || len(got.GroupStack) != 2 || got.GroupStack[1] != "g2" {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if !got.InGroup() || got.CurrentGroupID() != "g2" {
		t.Errorf("group helpers mismatch: %+v", got)
	}
}

func TestParseFrameEmptyAndInvalid(t *testing.T) {
	if _, err := ParseFrame(nil); err != nil {
		t.Errorf("ParseFrame(nil) error = %v, want nil", err)
	}
	if _, err := ParseFrame(json.RawMessage(`{}`)); err == nil {
		t.Error("ParseFrame({}) error = nil, want missing current_node_id error")
	}
	if _, err := ParseFrame(json.RawMessage(`not json`)); err == nil {
		t.Error("ParseFrame(bad) error = nil, want parse error")
	}
}

func TestCountersRecordAndLimits(t *testing.T) {
	limits := Limits{MaxPerNodeExecutions: 2, MaxTotalExecutions: 3}
	c := Counters{}
	if err := c.Record("a", limits); err != nil {
		t.Fatal(err)
	}
	if err := c.Record("a", limits); err != nil {
		t.Fatal(err)
	}
	if err := c.Record("a", limits); err == nil {
		t.Error("third execution of a: error = nil, want per-node limit error")
	}
	if c.Nodes["a"] != 2 || c.Total != 2 {
		t.Errorf("counters = %+v", c)
	}

	c = Counters{}
	if err := c.Record("a", limits); err != nil {
		t.Fatal(err)
	}
	if err := c.Record("b", limits); err != nil {
		t.Fatal(err)
	}
	if err := c.Record("c", limits); err != nil {
		t.Fatal(err)
	}
	if err := c.Record("d", limits); err == nil {
		t.Error("fourth total execution: error = nil, want total limit error")
	}
}

func TestCountersJSONRoundTrip(t *testing.T) {
	c := Counters{Total: 5, Nodes: map[string]int{"a": 3}}
	raw, err := c.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseCounters(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 5 || got.Nodes["a"] != 3 {
		t.Errorf("round trip mismatch: %+v", got)
	}
}

func TestParseCountersEmpty(t *testing.T) {
	c, err := ParseCounters(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Nodes != nil || c.Total != 0 {
		t.Errorf("empty counters = %+v", c)
	}
}

func TestDefaultLimitsSane(t *testing.T) {
	l := DefaultLimits()
	if l.MaxPerNodeExecutions <= 0 || l.MaxTotalExecutions <= 0 || l.LeaseDuration <= 0 || l.DefaultNodeTimeout <= 0 || l.ConditionTimeout <= 0 {
		t.Errorf("defaults not sane: %+v", l)
	}
}
