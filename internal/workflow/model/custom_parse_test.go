package model_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

type parseCustomConfig struct {
	City  string `json:"city"`
	Units string `json:"units"`
}

func parseCustomValidator(raw json.RawMessage) (any, error) {
	var cfg parseCustomConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.City) == "" {
		return nil, errParseCustomCity
	}
	if cfg.Units == "" {
		cfg.Units = "metric"
	}
	if cfg.Units != "metric" && cfg.Units != "imperial" {
		return nil, errParseCustomUnits
	}
	return cfg, nil
}

var (
	errParseCustomCity  = errNew("city is required")
	errParseCustomUnits = errNew("units must be metric or imperial")
)

func registerParseCustom(t *testing.T) {
	t.Helper()
	_ = model.RegisterCustomType("parsecustom", parseCustomValidator)
}

func TestParseCustomNodeOK(t *testing.T) {
	registerParseCustom(t)
	nc := mustParseContent(t, `{
		"id": "11111111-1111-7111-8111-111111111111",
		"type": "parsecustom",
		"name": "fetch",
		"config": {"city": "Berlin", "units": "metric"},
		"timeout": "10s",
		"output_property": "weather",
		"next_node": "22222222-2222-7222-8222-222222222222",
		"retry_on_recovery": true,
		"pre_script": {"script": "ctx.a = 1;", "timeout": "5s"},
		"post_script": {"script": "ctx.b = 2;", "timeout": "5s"},
		"metadata": {"k": "v"}
	}`)
	if nc.Type != model.NodeType("parsecustom") {
		t.Errorf("type = %q, want parsecustom", nc.Type)
	}
	if len(nc.CustomConfig) == 0 {
		t.Error("CustomConfig empty, want raw config bytes")
	}
	cfg, ok := nc.Custom.(parseCustomConfig)
	if !ok {
		t.Fatalf("Custom = %#v, want parseCustomConfig", nc.Custom)
	}
	if cfg.City != "Berlin" || cfg.Units != "metric" {
		t.Errorf("Custom = %+v, want Berlin/metric", cfg)
	}
	if nc.Timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", nc.Timeout)
	}
	if nc.OutputProperty != "weather" {
		t.Errorf("output_property = %q", nc.OutputProperty)
	}
	if !nc.RetryOnRecovery {
		t.Error("retry_on_recovery = false, want true")
	}
	if nc.PreScript == nil || nc.PostScript == nil {
		t.Error("hooks missing, want both parsed")
	}
}

func TestParseCustomNodeDefaultTimeout(t *testing.T) {
	registerParseCustom(t)
	nc := mustParseContent(t, `{"type":"parsecustom","config":{"city":"Berlin"}}`)
	if nc.Timeout != testLimits.DefaultTimeout {
		t.Errorf("timeout = %v, want default %v", nc.Timeout, testLimits.DefaultTimeout)
	}
	cfg := nc.Custom.(parseCustomConfig)
	if cfg.Units != "metric" {
		t.Errorf("units = %q, want default metric", cfg.Units)
	}
}

func TestParseCustomNodeTimeoutCap(t *testing.T) {
	registerParseCustom(t)
	_, err := model.ParseNodeContent([]byte(`{"type":"parsecustom","config":{"city":"x"},"timeout":"10h"}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("error = %v, want timeout cap error", err)
	}
}

func TestParseCustomNodeBadConfig(t *testing.T) {
	registerParseCustom(t)
	_, err := model.ParseNodeContent([]byte(`{"type":"parsecustom","config":{"units":"metric"}}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("error = %v, want invalid-config error", err)
	}
	_, err = model.ParseNodeContent([]byte(`{"type":"parsecustom","config":{"city":"x","units":"kelvin"}}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("error = %v, want invalid-config error", err)
	}
}

func TestParseCustomNodeMissingConfig(t *testing.T) {
	registerParseCustom(t)
	_, err := model.ParseNodeContent([]byte(`{"type":"parsecustom"}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "config is required") {
		t.Fatalf("error = %v, want config-required error", err)
	}
}

func TestParseCustomNodeOnFailureAllowed(t *testing.T) {
	registerParseCustom(t)
	nc := mustParseContent(t, `{
		"type": "parsecustom",
		"config": {"city": "Berlin"},
		"on_failure": {
			"next_node": "11111111-1111-7111-8111-111111111111",
			"output_property": "weather_err"
		}
	}`)
	if nc.OnFailure == nil || nc.OnFailure.OutputProperty != "weather_err" {
		t.Fatalf("on_failure = %+v, want route", nc.OnFailure)
	}
}

func TestParseCustomNodeRejectsBuiltinKeys(t *testing.T) {
	registerParseCustom(t)
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"script", `{"type":"parsecustom","config":{"city":"x"},"script":"return 1;"}`, "does not support"},
		{"http_config", `{"type":"parsecustom","config":{"city":"x"},"http_config":{"url":"https://example.com"}}`, "does not support"},
		{"keys", `{"type":"parsecustom","config":{"city":"x"},"keys":{"a":"11111111-1111-7111-8111-111111111111"}}`, "only valid for group"},
		{"branches", `{"type":"parsecustom","config":{"city":"x"},"branches":{"a":"b"}}`, "branches is not supported"},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %v, want mention of %q", c.name, err, c.want)
		}
	}
}

func TestParseUnregisteredCustomTypeFails(t *testing.T) {
	_, err := model.ParseNodeContent([]byte(`{"type":"parsecustom_missing","config":{}}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("error = %v, want not-supported error", err)
	}
}

func TestParseCustomNodeDefinitionRef(t *testing.T) {
	registerParseCustom(t)
	nc := mustParseContent(t, `{
		"id": "11111111-1111-7111-8111-111111111111",
		"type": "parsecustom",
		"node_definition_id": "22222222-2222-7222-8222-222222222222",
		"next_node": "33333333-3333-7333-8333-333333333333"
	}`)
	if nc.Type != model.NodeType("parsecustom") {
		t.Errorf("type = %q, want parsecustom", nc.Type)
	}
	if nc.NodeDefinitionID != "22222222-2222-7222-8222-222222222222" {
		t.Errorf("node_definition_id = %q", nc.NodeDefinitionID)
	}
}

func TestParseCustomNodeDefinitionRefRejectsInlineConfig(t *testing.T) {
	registerParseCustom(t)
	_, err := model.ParseNodeContent([]byte(`{
		"type": "parsecustom",
		"node_definition_id": "22222222-2222-7222-8222-222222222222",
		"config": {"city": "x"}
	}`), testLimits)
	if err == nil || !strings.Contains(err.Error(), "inline executable fields") {
		t.Fatalf("error = %v, want inline-fields error", err)
	}
}
