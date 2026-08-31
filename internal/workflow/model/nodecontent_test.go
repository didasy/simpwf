package model_test

import (
	"strings"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

var testLimits = model.NodeLimits{
	DefaultTimeout:   30 * time.Second,
	MaxTimeout:       5 * time.Minute,
	ConditionTimeout: 5 * time.Second,
}

func mustParseContent(t *testing.T, raw string) *model.NodeContent {
	t.Helper()
	nc, err := model.ParseNodeContent([]byte(raw), testLimits)
	if err != nil {
		t.Fatalf("ParseNodeContent(%s) error = %v", raw, err)
	}
	return nc
}

func TestNodeTypeValidity(t *testing.T) {
	for _, ty := range []string{"script", "conditions", "input", "group", "external_call", "output", "poller"} {
		if !model.ValidNodeType(ty) {
			t.Errorf("ValidNodeType(%q) = false, want true", ty)
		}
	}
	for _, ty := range []string{"", "redis", "rabbitmq", "magic"} {
		if model.ValidNodeType(ty) {
			t.Errorf("ValidNodeType(%q) = true, want false", ty)
		}
	}
}

func TestParseScriptNode(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "script",
		"script": "return 1;",
		"input_data": "user",
		"timeout": "45s",
		"output_property": "result",
		"next_node": "11111111-1111-7111-8111-111111111111"
	}`)
	if nc.Type != model.NodeTypeScript {
		t.Errorf("type = %s, want script", nc.Type)
	}
	if nc.Script != "return 1;" || nc.InputData == nil || *nc.InputData != "user" {
		t.Errorf("script fields mismatch: %+v", nc)
	}
	if nc.Timeout != 45*time.Second {
		t.Errorf("timeout = %v, want 45s", nc.Timeout)
	}
	if nc.OutputProperty != "result" || nc.NextNode != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("output/next mismatch: %+v", nc)
	}
}

func TestParseScriptNodeDefaultsTimeout(t *testing.T) {
	nc := mustParseContent(t, `{"type":"script","script":"return 1;"}`)
	if nc.Timeout != 30*time.Second {
		t.Errorf("timeout = %v, want default 30s", nc.Timeout)
	}
}

func TestParseScriptNodeRejectsBadTimeout(t *testing.T) {
	if _, err := model.ParseNodeContent([]byte(`{"type":"script","script":"x","timeout":"soon"}`), testLimits); err == nil {
		t.Error("invalid timeout error = nil, want error")
	}
	if _, err := model.ParseNodeContent([]byte(`{"type":"script","script":"x","timeout":"10m"}`), testLimits); err == nil {
		t.Error("timeout over cap error = nil, want error")
	}
}

func TestParseScriptNodeRequiresScript(t *testing.T) {
	if _, err := model.ParseNodeContent([]byte(`{"type":"script"}`), testLimits); err == nil {
		t.Error("missing script error = nil, want error")
	}
	if _, err := model.ParseNodeContent([]byte(`{"type":"script","script":"  "}`), testLimits); err == nil {
		t.Error("blank script error = nil, want error")
	}
}

func TestParseConditionsNode(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "conditions",
		"conditions": [
			{"key": "a", "condition": "return context.a === 1;"},
			{"key": "", "condition": "return context.exit1;"},
			{"key": null, "condition": "return context.exit2;"},
			{"condition": "return context.exit3;"},
			{"key": " ", "condition": "return context.exit4;"}
		]
	}`)
	if len(nc.Conditions) != 5 {
		t.Fatalf("conditions len = %d, want 5", len(nc.Conditions))
	}
	if nc.Conditions[0].Key != "a" || nc.Conditions[0].Condition == "" {
		t.Errorf("condition 0 mismatch: %+v", nc.Conditions[0])
	}
	for i := 1; i < len(nc.Conditions); i++ {
		if nc.Conditions[i].Key != "" {
			t.Errorf("condition %d key = %q, want exit key", i, nc.Conditions[i].Key)
		}
	}
}

func TestParseConditionsNodeRequiresAtLeastTwo(t *testing.T) {
	bad := []string{
		`{"type":"conditions"}`,
		`{"type":"conditions","conditions":[]}`,
		`{"type":"conditions","conditions":[{"key":"a","condition":"return true;"}]}`,
	}
	for _, raw := range bad {
		if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil || !strings.Contains(err.Error(), "at least two conditions") {
			t.Errorf("ParseNodeContent(%s) error = %v, want 'at least two conditions'", raw, err)
		}
	}
}

func TestParseConditionsNodeRejectsInvalid(t *testing.T) {
	bad := []string{
		`{"type":"conditions"}`,                 // no conditions
		`{"type":"conditions","conditions":[]}`, // empty conditions
		`{"type":"conditions","conditions":[{"key":"a","condition":" "},{"key":"b","condition":"return false;"}]}`,                                                        // blank condition
		`{"type":"conditions","conditions":[{"key":"a","condition":"return true;"},{"key":"a","condition":"x"}]}`,                                                         // duplicate key
		`{"type":"conditions","conditions":[{"key":"a","condition":"return true;","next_node":""},{"key":"b","condition":"return false;"}]}`,                              // legacy branch target
		`{"type":"conditions","conditions":[{"key":"a","condition":"a"},{"key":"b","condition":"return false;"}],"branches":{"":"11111111-1111-7111-8111-111111111111"}}`, // blank branch key
		`{"type":"conditions","conditions":[{"key":"a","condition":"a"},{"key":"b","condition":"return false;"}],"branches":null}`,                                        // legacy branches field
		`{"type":"conditions","conditions":[{"key":"a","condition":"a"},{"key":"b","condition":"return false;"}],"next_node":"x"}`,                                        // node-level next_node forbidden
		`{"type":"conditions","conditions":[{"key":"a","condition":"a"},{"key":"b","condition":"return false;"}],"output_property":"x"}`,                                  // output_property forbidden
	}
	for _, raw := range bad {
		if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseNodeContent(%s) error = nil, want error", raw)
		}
	}
}

func TestParseInputNode(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "input",
		"channel": "http",
		"context_path": "webhook.data",
		"validation": {"script": "input = JSON.parse(input); if (!input.success) { return 'failed'; };"},
		"next_node": "11111111-1111-7111-8111-111111111111"
	}`)
	if nc.Channel != "http" || nc.ContextPath != "webhook.data" {
		t.Errorf("channel/context_path mismatch: %+v", nc)
	}
	if nc.Validation == nil || nc.Validation.Script == "" {
		t.Errorf("validation mismatch: %+v", nc.Validation)
	}
}

func TestParseInputNodeAcceptsBrokerChannels(t *testing.T) {
	for _, channel := range []string{"http", "redis", "rabbitmq"} {
		nc := mustParseContent(t, `{"type":"input","channel":"`+channel+`","context_path":"x"}`)
		if nc.Channel != channel {
			t.Errorf("channel = %q, want %q", nc.Channel, channel)
		}
	}
	for _, channel := range []string{"kafka", "ftp", "amqp"} {
		raw := `{"type":"input","channel":"` + channel + `","context_path":"x"}`
		if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseNodeContent(channel=%s) error = nil, want error", channel)
		}
	}
	if _, err := model.ParseNodeContent([]byte(`{"type":"input","context_path":"x"}`), testLimits); err == nil {
		t.Error("input without channel error = nil, want error")
	}
	if _, err := model.ParseNodeContent([]byte(`{"type":"input","channel":"http"}`), testLimits); err == nil {
		t.Error("input without context_path error = nil, want error")
	}
}

func TestParseExternalCallHTTP(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "external_call",
		"http_config": {
			"url": "https://example.com/api",
			"method": "POST",
			"headers": {"Authorization": "Bearer x"},
			"body": {"a": 1}
		},
		"timeout": "30s"
	}`)
	if nc.HTTP == nil {
		t.Fatal("http_config not parsed")
	}
	if nc.HTTP.URL != "https://example.com/api" || nc.HTTP.Method != "POST" {
		t.Errorf("http config mismatch: %+v", nc.HTTP)
	}
	if nc.HTTP.Headers["Authorization"] != "Bearer x" {
		t.Errorf("headers mismatch: %+v", nc.HTTP.Headers)
	}
	if string(nc.HTTP.Body) != `{"a": 1}` {
		t.Errorf("body = %s", nc.HTTP.Body)
	}
	if nc.Execution != nil {
		t.Error("execution_config should be nil for http node")
	}
}

func TestParseExternalCallHTTPDynamic(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "external_call",
		"http_config": {
			"url": "{{ notification.url }}",
			"method": "{{ notification.method }}",
			"headers": {"Authorization": "Bearer {{ user.token }}"},
			"body": {"name": "{{ user.name }}"}
		}
	}`)
	if nc.HTTP == nil {
		t.Fatal("http_config not parsed")
	}
	if nc.HTTP.URL != "{{ notification.url }}" {
		t.Errorf("url = %q, want template preserved", nc.HTTP.URL)
	}
	if nc.HTTP.Method != "{{ notification.method }}" {
		t.Errorf("method = %q, want template preserved (not uppercased)", nc.HTTP.Method)
	}
	if nc.HTTP.Headers["Authorization"] != "Bearer {{ user.token }}" {
		t.Errorf("headers mismatch: %+v", nc.HTTP.Headers)
	}
	if string(nc.HTTP.Body) != `{"name": "{{ user.name }}"}` {
		t.Errorf("body = %s", nc.HTTP.Body)
	}
}

func TestParseExternalCallCommand(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "external_call",
		"execution_config": {"command": ["ls", "-al"], "stdin": "data"}
	}`)
	if nc.Execution == nil || len(nc.Execution.Command) != 2 || nc.Execution.Command[0] != "ls" {
		t.Fatalf("execution config mismatch: %+v", nc.Execution)
	}
	if nc.Execution.Stdin != "data" {
		t.Errorf("stdin = %q", nc.Execution.Stdin)
	}
}

func TestParseExternalCallRejectsInvalid(t *testing.T) {
	bad := []string{
		`{"type":"external_call"}`, // neither config
		`{"type":"external_call","http_config":{},"execution_config":{"command":["ls"]}}`, // both configs
		`{"type":"external_call","http_config":{"url":""}}`,                               // empty url
		`{"type":"external_call","http_config":{"url":"no-scheme"}}`,                      // missing scheme
		`{"type":"external_call","execution_config":{"command":[]}}`,                      // empty command
	}
	for _, raw := range bad {
		if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseNodeContent(%s) error = nil, want error", raw)
		}
	}
}

func TestParseGroupNode(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "group",
		"start_node_id": "11111111-1111-7111-8111-111111111111",
		"nodes": [
			{"id": "11111111-1111-7111-8111-111111111111", "type": "script", "script": "return 1;"}
		]
	}`)
	if nc.Group == nil {
		t.Fatal("group not parsed")
	}
	if nc.Group.StartNodeID != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("start_node_id = %q", nc.Group.StartNodeID)
	}
	if len(nc.Group.Nodes) != 1 {
		t.Fatalf("nested nodes len = %d, want 1", len(nc.Group.Nodes))
	}
}

func TestParseGroupNodeRejectsInvalid(t *testing.T) {
	bad := []string{
		`{"type":"group"}`, // no start_node_id
		`{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111"}`,                                                                                                                                                                 // no nodes
		`{"type":"group","start_node_id":"x","nodes":[{"id":"11111111-1111-7111-8111-111111111111","type":"script","script":"x"}]}`,                                                                                                               // bad start id
		`{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111","nodes":[{"id":"x","type":"script","script":"x"}]}`,                                                                                                               // bad node id
		`{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111","nodes":[{"id":"22222222-2222-7222-8222-222222222222","type":"script","script":"x"}]}`,                                                                            // start not among nodes
		`{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111","nodes":[{"id":"11111111-1111-7111-8111-111111111111","type":"script","script":"x"},{"id":"11111111-1111-7111-8111-111111111111","type":"script","script":"y"}]}`, // duplicate ids
	}
	for _, raw := range bad {
		if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
			t.Errorf("ParseNodeContent(%s) error = nil, want error", raw)
		}
	}
}

func TestParseOutputNode(t *testing.T) {
	for _, channel := range []string{"redis", "rabbitmq"} {
		nc := mustParseContent(t, `{"type":"output","channel":"`+channel+`","context_path":"result.payload","output_property":"out","next_node":"11111111-1111-7111-8111-111111111111"}`)
		if nc.Type != model.NodeTypeOutput || nc.Channel != channel || nc.ContextPath != "result.payload" {
			t.Errorf("output node mismatch: %+v", nc)
		}
		if nc.OutputProperty != "out" {
			t.Errorf("output_property = %q, want out", nc.OutputProperty)
		}
	}
}

func TestParseOutputNodeRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"missing channel", `{"type":"output","context_path":"x"}`, "channel"},
		{"http channel", `{"type":"output","channel":"http","context_path":"x"}`, "channel"},
		{"empty channel", `{"type":"output","channel":"","context_path":"x"}`, "channel"},
		{"missing context_path", `{"type":"output","channel":"redis"}`, "context_path"},
		{"empty context_path", `{"type":"output","channel":"redis","context_path":"  "}`, "context_path"},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil {
			t.Errorf("%s: error = nil, want error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %q, want mention of %q", c.name, err, c.want)
		}
	}
}
func TestParseUnknownType(t *testing.T) {
	if _, err := model.ParseNodeContent([]byte(`{"type":"magic"}`), testLimits); err == nil {
		t.Error("unknown type error = nil, want error")
	}
}

func TestParsePollerHTTP(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "poller",
		"http": {
			"url": "https://example.com/jobs/{{ job.id }}",
			"method": "POST",
			"headers": {"Authorization": "Bearer {{ auth.token }}"},
			"body": {"job_id": "{{ job.id }}"},
			"delay": "5s",
			"request_timeout": "30s",
			"max_attempts": 10,
			"until": "return response.status === 200 && response.body.status === \"completed\";"
		},
		"output_property": "polled_data",
		"retry_on_recovery": true,
		"next_node": "11111111-1111-7111-8111-111111111111"
	}`)
	if nc.Type != model.NodeTypePoller {
		t.Fatalf("type = %s, want poller", nc.Type)
	}
	h := nc.PollerHTTP
	if h == nil {
		t.Fatal("http block not parsed")
	}
	if h.URL != "https://example.com/jobs/{{ job.id }}" {
		t.Errorf("url = %q", h.URL)
	}
	if h.Method != "POST" {
		t.Errorf("method = %q, want POST", h.Method)
	}
	if h.Headers["Authorization"] != "Bearer {{ auth.token }}" {
		t.Errorf("headers mismatch: %+v", h.Headers)
	}
	if string(h.Body) != `{"job_id": "{{ job.id }}"}` {
		t.Errorf("body = %s", h.Body)
	}
	if h.Delay != 5*time.Second || h.RequestTimeout != 30*time.Second || h.MaxAttempts != 10 {
		t.Errorf("timing fields mismatch: %+v", h)
	}
	if h.Until != `return response.status === 200 && response.body.status === "completed";` {
		t.Errorf("until = %q", h.Until)
	}
	if nc.OutputProperty != "polled_data" || !nc.RetryOnRecovery {
		t.Errorf("node fields mismatch: %+v", nc)
	}
	if nc.NextNode != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("next_node = %q", nc.NextNode)
	}
	if nc.PredicateTimeout != testLimits.ConditionTimeout {
		t.Errorf("predicate timeout = %v, want %v", nc.PredicateTimeout, testLimits.ConditionTimeout)
	}
}

func TestParsePollerHTTPDefaults(t *testing.T) {
	nc := mustParseContent(t, `{"type":"poller","http":{"url":"https://example.com/x","until":"return true;"}}`)
	h := nc.PollerHTTP
	if h == nil {
		t.Fatal("http block not parsed")
	}
	if h.Method != "GET" {
		t.Errorf("method = %q, want default GET", h.Method)
	}
	if h.Delay != 5*time.Second {
		t.Errorf("delay = %v, want default 5s", h.Delay)
	}
	if h.RequestTimeout != 30*time.Second {
		t.Errorf("request_timeout = %v, want default 30s", h.RequestTimeout)
	}
	if h.MaxAttempts != 10 {
		t.Errorf("max_attempts = %d, want default 10", h.MaxAttempts)
	}
	if !nc.RetryOnRecovery {
		t.Error("retry_on_recovery = false, want default true")
	}
}

func TestParsePollerHTTPNoTimeoutCap(t *testing.T) {
	nc := mustParseContent(t, `{"type":"poller","http":{"url":"https://example.com/x","delay":"1m","request_timeout":"1h","until":"return true;"}}`)
	if nc.PollerHTTP.Delay != time.Minute {
		t.Errorf("delay = %v, want 1m", nc.PollerHTTP.Delay)
	}
	if nc.PollerHTTP.RequestTimeout != time.Hour {
		t.Errorf("request_timeout = %v, want 1h (no global cap)", nc.PollerHTTP.RequestTimeout)
	}
}

func TestParsePollerRedisGET(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "poller",
		"redis": {
			"method": "GET",
			"key": "jobs:{{ job.id }}",
			"delay": "5s",
			"request_timeout": "30s",
			"max_attempts": 10,
			"until": "return response.body !== null && response.body.status === \"completed\";"
		}
	}`)
	r := nc.PollerRedis
	if r == nil {
		t.Fatal("redis block not parsed")
	}
	if r.Method != "GET" || r.Key != "jobs:{{ job.id }}" {
		t.Errorf("method/key mismatch: %+v", r)
	}
	if r.Delay != 5*time.Second || r.RequestTimeout != 30*time.Second || r.MaxAttempts != 10 {
		t.Errorf("timing fields mismatch: %+v", r)
	}
	if r.MaxWaitTime != 0 {
		t.Errorf("max_wait_time = %v, want unset for GET", r.MaxWaitTime)
	}
	if r.Channel != "" {
		t.Errorf("channel = %q, want unset for GET", r.Channel)
	}
}

func TestParsePollerRedisGETNormalizesMethod(t *testing.T) {
	nc := mustParseContent(t, `{"type":"poller","redis":{"method":"get","key":"k","until":"return true;"}}`)
	if nc.PollerRedis.Method != "GET" {
		t.Errorf("method = %q, want GET (case-normalized)", nc.PollerRedis.Method)
	}
}

func TestParsePollerRedisSUB(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "poller",
		"redis": {
			"method": "SUB",
			"channel": "jobs:{{ job.id }}",
			"max_wait_time": "5m",
			"until": "return response.body.status === \"completed\";"
		}
	}`)
	r := nc.PollerRedis
	if r == nil {
		t.Fatal("redis block not parsed")
	}
	if r.Method != "SUB" || r.Channel != "jobs:{{ job.id }}" {
		t.Errorf("method/channel mismatch: %+v", r)
	}
	if r.MaxWaitTime != 5*time.Minute {
		t.Errorf("max_wait_time = %v, want 5m", r.MaxWaitTime)
	}
	if r.Key != "" || r.Delay != 0 || r.RequestTimeout != 0 || r.MaxAttempts != 0 {
		t.Errorf("GET-only fields must be unset for SUB: %+v", r)
	}
	if !nc.RetryOnRecovery {
		t.Error("retry_on_recovery = false, want default true")
	}
}

func TestParsePollerRabbitMQ(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "poller",
		"rabbitmq": {
			"queue": "jobs.{{ workflow_instance_id }}",
			"max_wait_time": "5m",
			"until": "return response.body.status === \"completed\";"
		}
	}`)
	r := nc.PollerRabbitMQ
	if r == nil {
		t.Fatal("rabbitmq block not parsed")
	}
	if r.Queue != "jobs.{{ workflow_instance_id }}" {
		t.Errorf("queue = %q", r.Queue)
	}
	if r.MaxWaitTime != 5*time.Minute {
		t.Errorf("max_wait_time = %v, want 5m", r.MaxWaitTime)
	}
	if nc.PollerHTTP != nil || nc.PollerRedis != nil {
		t.Error("other transport blocks should be nil")
	}
}

// hookObject renders a pre/post_script hook object for tests.
func hookObject(script, timeout string) string {
	s := `{"script":"` + script + `"`
	if timeout != "" {
		s += `,"timeout":"` + timeout + `"`
	}
	return s + `}`
}

func TestParseHooksOnEveryNodeType(t *testing.T) {
	pre := hookObject("context.p = 1;", "")
	post := hookObject("context.q = 1;", "")
	cases := []struct {
		name string
		raw  string
	}{
		{"script", `{"type":"script","script":"return 1;","pre_script":` + pre + `,"post_script":` + post + `}`},
		{"conditions", `{"type":"conditions","conditions":[{"key":"a","condition":"return true;"},{"key":"b","condition":"return false;"}],"pre_script":` + pre + `,"post_script":` + post + `}`},
		{"input", `{"type":"input","channel":"http","context_path":"x","pre_script":` + pre + `,"post_script":` + post + `}`},
		{"external_call", `{"type":"external_call","http_config":{"url":"https://example.com/x"},"pre_script":` + pre + `,"post_script":` + post + `}`},
		{"output", `{"type":"output","channel":"redis","context_path":"x","pre_script":` + pre + `,"post_script":` + post + `}`},
		{"poller", `{"type":"poller","http":{"url":"https://example.com/x","until":"return true;"},"pre_script":` + pre + `,"post_script":` + post + `}`},
		{"group", `{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111","nodes":[{"id":"11111111-1111-7111-8111-111111111111","type":"script","script":"return 1;","pre_script":` + pre + `}],"pre_script":` + pre + `,"post_script":` + post + `}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nc := mustParseContent(t, c.raw)
			if nc.PreScript == nil || nc.PreScript.Script != "context.p = 1;" {
				t.Errorf("pre_script not parsed: %+v", nc.PreScript)
			}
			if nc.PreScript.Timeout != testLimits.DefaultTimeout {
				t.Errorf("pre_script timeout = %v, want default %v", nc.PreScript.Timeout, testLimits.DefaultTimeout)
			}
			if nc.PostScript == nil || nc.PostScript.Script != "context.q = 1;" {
				t.Errorf("post_script not parsed: %+v", nc.PostScript)
			}
			if c.name == "group" && nc.Group.Nodes[0].PreScript == nil {
				t.Error("group child pre_script not parsed")
			}
		})
	}
}

func TestParseHookExplicitTimeout(t *testing.T) {
	nc := mustParseContent(t, `{"type":"script","script":"return 1;",
		"pre_script":`+hookObject("context.p = 1;", "45s")+`,
		"post_script":`+hookObject("context.q = 1;", "1m")+`}`)
	if nc.PreScript.Timeout != 45*time.Second {
		t.Errorf("pre_script timeout = %v, want 45s", nc.PreScript.Timeout)
	}
	if nc.PostScript.Timeout != time.Minute {
		t.Errorf("post_script timeout = %v, want 1m", nc.PostScript.Timeout)
	}
}

func TestParseHooksExplicitNull(t *testing.T) {
	nc := mustParseContent(t, `{"type":"script","script":"return 1;","pre_script":null,"post_script":null}`)
	if nc.PreScript != nil || nc.PostScript != nil {
		t.Errorf("hooks = %+v / %+v, want nil for explicit null", nc.PreScript, nc.PostScript)
	}
	if !nc.PreScriptSet || !nc.PostScriptSet {
		t.Errorf("presence flags = %v/%v, want true for explicit null", nc.PreScriptSet, nc.PostScriptSet)
	}
}

func TestParseHooksRejectInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"missing script", `{"type":"script","script":"return 1;","pre_script":{"timeout":"5s"}}`, "pre_script.script"},
		{"blank script", `{"type":"script","script":"return 1;","post_script":{"script":"  "}}`, "post_script.script"},
		{"not an object", `{"type":"script","script":"return 1;","pre_script":"context.x=1;"}`, "pre_script"},
		{"empty object", `{"type":"script","script":"return 1;","pre_script":{}}`, "pre_script.script"},
		{"invalid timeout", `{"type":"script","script":"return 1;","pre_script":{"script":"x;","timeout":"soon"}}`, "timeout"},
		{"timeout over cap", `{"type":"script","script":"return 1;","post_script":{"script":"x;","timeout":"10m"}}`, "cap"},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil {
			t.Errorf("%s: error = nil, want error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %q, want mention of %q", c.name, err, c.want)
		}
	}
}

func TestParseReferencedNodeCarriesHooks(t *testing.T) {
	nc := mustParseContent(t, `{"node_definition_id":"11111111-1111-7111-8111-111111111111","pre_script":`+hookObject("context.p = 1;", "")+`}`)
	if nc.PreScript == nil || !nc.PreScriptSet {
		t.Errorf("referenced node pre_script not parsed: %+v (set %v)", nc.PreScript, nc.PreScriptSet)
	}
	// Hooks are lifecycle fields, not inline executable fields: a referenced
	// node carrying both hooks and an inline script must still be rejected.
	raw := `{"node_definition_id":"11111111-1111-7111-8111-111111111111","pre_script":` + hookObject("context.p = 1;", "") + `,"script":"return 1;"}`
	if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
		t.Error("referenced node with inline script error = nil, want error")
	}
}

func TestParsePollerRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"no transport", `{"type":"poller"}`, "exactly one"},
		{"two transports", `{"type":"poller","http":{"url":"https://example.com/x","until":"return true;"},"redis":{"method":"GET","key":"k","until":"return true;"}}`, "exactly one"},
		{"missing until", `{"type":"poller","http":{"url":"https://example.com/x"}}`, "until"},
		{"blank until", `{"type":"poller","http":{"url":"https://example.com/x","until":"  "}}`, "until"},
		{"blank url", `{"type":"poller","http":{"url":"","until":"return true;"}}`, "url"},
		{"no-scheme url", `{"type":"poller","http":{"url":"example.com/x","until":"return true;"}}`, "url"},
		{"redis get with channel", `{"type":"poller","redis":{"method":"GET","key":"k","channel":"c","until":"return true;"}}`, "channel"},
		{"redis get with max_wait_time", `{"type":"poller","redis":{"method":"GET","key":"k","max_wait_time":"5m","until":"return true;"}}`, "max_wait_time"},
		{"redis sub with key", `{"type":"poller","redis":{"method":"SUB","channel":"c","key":"k","until":"return true;"}}`, "key"},
		{"redis sub with delay", `{"type":"poller","redis":{"method":"SUB","channel":"c","delay":"5s","until":"return true;"}}`, "delay"},
		{"redis sub with request_timeout", `{"type":"poller","redis":{"method":"SUB","channel":"c","request_timeout":"30s","until":"return true;"}}`, "request_timeout"},
		{"redis sub with max_attempts", `{"type":"poller","redis":{"method":"SUB","channel":"c","max_attempts":10,"until":"return true;"}}`, "max_attempts"},
		{"redis bad method", `{"type":"poller","redis":{"method":"SET","key":"k","until":"return true;"}}`, "GET"},
		{"redis missing method", `{"type":"poller","redis":{"key":"k","until":"return true;"}}`, "GET"},
		{"redis get missing key", `{"type":"poller","redis":{"method":"GET","until":"return true;"}}`, "key"},
		{"redis get blank key", `{"type":"poller","redis":{"method":"GET","key":"  ","until":"return true;"}}`, "key"},
		{"redis sub missing channel", `{"type":"poller","redis":{"method":"SUB","until":"return true;"}}`, "channel"},
		{"rabbit missing queue", `{"type":"poller","rabbitmq":{"max_wait_time":"5m","until":"return true;"}}`, "queue"},
		{"rabbit blank queue", `{"type":"poller","rabbitmq":{"queue":"  ","max_wait_time":"5m","until":"return true;"}}`, "queue"},
		{"zero delay", `{"type":"poller","http":{"url":"https://example.com/x","delay":"0s","until":"return true;"}}`, "delay"},
		{"negative delay", `{"type":"poller","http":{"url":"https://example.com/x","delay":"-5s","until":"return true;"}}`, "delay"},
		{"invalid delay", `{"type":"poller","http":{"url":"https://example.com/x","delay":"soon","until":"return true;"}}`, "delay"},
		{"zero request_timeout", `{"type":"poller","http":{"url":"https://example.com/x","request_timeout":"0s","until":"return true;"}}`, "request_timeout"},
		{"invalid request_timeout", `{"type":"poller","http":{"url":"https://example.com/x","request_timeout":"soon","until":"return true;"}}`, "request_timeout"},
		{"zero max_attempts", `{"type":"poller","http":{"url":"https://example.com/x","max_attempts":0,"until":"return true;"}}`, "max_attempts"},
		{"negative max_attempts", `{"type":"poller","http":{"url":"https://example.com/x","max_attempts":-1,"until":"return true;"}}`, "max_attempts"},
		{"zero max_wait_time", `{"type":"poller","redis":{"method":"SUB","channel":"c","max_wait_time":"0s","until":"return true;"}}`, "max_wait_time"},
		{"invalid max_wait_time", `{"type":"poller","rabbitmq":{"queue":"q","max_wait_time":"soon","until":"return true;"}}`, "max_wait_time"},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil {
			t.Errorf("%s: error = nil, want error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %q, want mention of %q", c.name, err, c.want)
		}
	}
}

func TestParseNodeContentOnFailureExternalCall(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "external_call",
		"http_config": {"url": "https://example.com/api"},
		"on_failure": {
			"next_node": "11111111-1111-7111-8111-111111111111",
			"output_property": "ext_err"
		}
	}`)
	if nc.OnFailure == nil {
		t.Fatal("on_failure not parsed")
	}
	if nc.OnFailure.NextNode != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("on_failure.next_node = %q", nc.OnFailure.NextNode)
	}
	if nc.OnFailure.OutputProperty != "ext_err" {
		t.Errorf("on_failure.output_property = %q", nc.OnFailure.OutputProperty)
	}
}

func TestParseNodeContentOnFailurePoller(t *testing.T) {
	nc := mustParseContent(t, `{
		"type": "poller",
		"http": {"url": "https://example.com/status", "until": "return true;"},
		"on_failure": {
			"next_node": "11111111-1111-7111-8111-111111111111",
			"output_property": "poller_err"
		}
	}`)
	if nc.OnFailure == nil {
		t.Fatal("on_failure not parsed")
	}
	if nc.OnFailure.NextNode != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("on_failure.next_node = %q", nc.OnFailure.NextNode)
	}
	if nc.OnFailure.OutputProperty != "poller_err" {
		t.Errorf("on_failure.output_property = %q", nc.OnFailure.OutputProperty)
	}
}

func TestParseNodeContentOnFailureRejectsUnsupportedTypes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"script", `{"type":"script","script":"return 1;","on_failure":{"next_node":"11111111-1111-7111-8111-111111111111","output_property":"err"}}`},
		{"conditions", `{"type":"conditions","conditions":[{"key":"a","condition":"return true;"},{"key":"b","condition":"return false;"}],"on_failure":{"next_node":"11111111-1111-7111-8111-111111111111","output_property":"err"}}`},
		{"input", `{"type":"input","channel":"http","context_path":"x","on_failure":{"next_node":"11111111-1111-7111-8111-111111111111","output_property":"err"}}`},
		{"output", `{"type":"output","channel":"redis","context_path":"x","on_failure":{"next_node":"11111111-1111-7111-8111-111111111111","output_property":"err"}}`},
		{"group", `{"type":"group","start_node_id":"11111111-1111-7111-8111-111111111111","nodes":[{"id":"11111111-1111-7111-8111-111111111111","type":"script","script":"return 1;"}],"on_failure":{"next_node":"22222222-2222-7222-8222-222222222222","output_property":"err"}}`},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil {
			t.Errorf("%s with on_failure: error = nil, want error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), "on_failure") {
			t.Errorf("%s with on_failure: error = %q, want mention of on_failure", c.name, err)
		}
	}
}

func TestParseNodeContentOnFailureRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"not an object", `{"type":"external_call","http_config":{"url":"https://example.com"},"on_failure":"bad"}`, "on_failure"},
		{"missing next_node", `{"type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"output_property":"err"}}`, "next_node"},
		{"invalid next_node uuid", `{"type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"next_node":"invalid-uuid","output_property":"err"}}`, "next_node"},
		{"missing output_property", `{"type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"next_node":"11111111-1111-7111-8111-111111111111"}}`, "output_property"},
		{"blank output_property", `{"type":"external_call","http_config":{"url":"https://example.com"},"on_failure":{"next_node":"11111111-1111-7111-8111-111111111111","output_property":"  "}}`, "output_property"},
	}
	for _, c := range cases {
		_, err := model.ParseNodeContent([]byte(c.raw), testLimits)
		if err == nil {
			t.Errorf("%s: error = nil, want error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %q, want mention of %q", c.name, err, c.want)
		}
	}
}

func TestParseNodeContentOnFailureReferencedNode(t *testing.T) {
	nc := mustParseContent(t, `{
		"node_definition_id": "11111111-1111-7111-8111-111111111111",
		"on_failure": {
			"next_node": "22222222-2222-7222-8222-222222222222",
			"output_property": "err"
		}
	}`)
	if nc.OnFailure == nil || nc.OnFailure.NextNode != "22222222-2222-7222-8222-222222222222" {
		t.Errorf("referenced node on_failure not parsed: %+v", nc.OnFailure)
	}

	// Explicit unsupported type on reference should be rejected during parse
	raw := `{"node_definition_id":"11111111-1111-7111-8111-111111111111","type":"script","on_failure":{"next_node":"22222222-2222-7222-8222-222222222222","output_property":"err"}}`
	if _, err := model.ParseNodeContent([]byte(raw), testLimits); err == nil {
		t.Error("referenced script with on_failure error = nil, want error")
	}
}
