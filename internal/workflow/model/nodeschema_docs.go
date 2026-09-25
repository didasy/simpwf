package model

// The per-type schema documents below are authored by hand, one per builtin
// node type. They mirror the Go parser in nodecontent.go: required fields,
// enums, and nested objects follow the parser rules, and nothing else is
// accepted. They are documentation only — the parser stays authoritative —
// and are served verbatim on node and workflow definition reads so a
// generic frontend can render a form per type.
//
// Every document describes the inline workflow-occurrence shape. A node
// referencing node_definition_id carries only graph fields instead; that
// reference shape is documented once in docs/integration/fields.md.

// commonNodeSchemaFields is the JSON fragment of the graph and lifecycle
// fields every node type accepts. It is a comma-separated property list
// pasted into the "properties" object of each hand-authored document below;
// the parser decides which types additionally allow on_failure.
const commonNodeSchemaFields = `
    "id": { "$ref": "#/$defs/nodeId" },
    "name": { "type": "string" },
    "retry_on_recovery": { "type": "boolean" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }`

const scriptNodeSchemaJSON = `{
  "title": "simpwf script node",
  "description": "Runs a script against the workflow context. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "script" },
    "script": { "type": "string", "minLength": 1, "description": "Non-blank script; its return value is stored at output_property." },
    "input_data": { "type": "string", "description": "Context path selecting the value exposed to the script as input." },
    "timeout": { "$ref": "#/$defs/duration" },
    "output_property": { "$ref": "#/$defs/contextKey" },
    "next_node": { "$ref": "#/$defs/nodeId" },
    ` + commonNodeSchemaFields + `
  },
  "required": ["type", "script"],
  "additionalProperties": false
}`

const conditionsNodeSchemaJSON = `{
  "title": "simpwf conditions node",
  "description": "Routes execution to a workflow or group key. Carries neither next_node nor output_property. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "conditions" },
    "conditions": {
      "type": "array",
      "minItems": 2,
      "description": "Each script must return a boolean. A blank key exits the scope; a non-blank key must be defined in the workflow or group keys.",
      "items": {
        "type": "object",
        "properties": {
          "condition": { "type": "string", "minLength": 1 },
          "key": { "type": "string" }
        },
        "required": ["condition"],
        "additionalProperties": false
      }
    },
    "id": { "$ref": "#/$defs/nodeId" },
    "name": { "type": "string" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }
  },
  "required": ["type", "conditions"],
  "additionalProperties": false
}`

const inputNodeSchemaJSON = `{
  "title": "simpwf input node",
  "description": "Parks the instance until a payload arrives on a channel. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "input" },
    "channel": { "type": "string", "enum": ["http", "redis", "rabbitmq"] },
    "output_property": { "type": "string", "pattern": "^[A-Za-z0-9_]+$", "description": "Bare key receiving the accepted payload; blank means the graph node id." },
    "validation": {
      "type": "object",
      "description": "Script rejecting non-blank string returns, evaluated with the raw payload as input.",
      "properties": { "script": { "type": "string", "minLength": 1 } },
      "required": ["script"],
      "additionalProperties": false
    },
    "form": {
      "type": "object",
      "description": "Dynamic-form contract. schema is a non-empty draft 2020-12 JSON Schema the payload is validated against before the script runs; ui carries opaque render hints.",
      "properties": {
        "schema": { "type": "object", "minProperties": 1 },
        "ui": { "type": "object" }
      },
      "required": ["schema"],
      "additionalProperties": false
    },
    "next_node": { "$ref": "#/$defs/nodeId" },
    "retry_on_recovery": { "type": "boolean" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }
  },
  "required": ["type", "channel"],
  "additionalProperties": false
}`

const groupNodeSchemaJSON = `{
  "title": "simpwf group node",
  "description": "Runs a nested node graph starting at start_node_id. Nested children follow the same per-type rules; go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "group" },
    "start_node_id": { "$ref": "#/$defs/nodeId" },
    "keys": { "$ref": "#/$defs/keys" },
    "nodes": {
      "type": "array",
      "minItems": 1,
      "description": "Nested child nodes with uuids unique across the whole definition.",
      "items": { "type": "object" }
    },
    "output_property": { "$ref": "#/$defs/contextKey" },
    "next_node": { "$ref": "#/$defs/nodeId" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }
  },
  "required": ["type", "start_node_id", "nodes"],
  "additionalProperties": false
}`

const externalCallNodeSchemaJSON = `{
  "title": "simpwf external_call node",
  "description": "Makes exactly one outbound HTTP call or executes an allowlisted command. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "external_call" },
    "http_config": {
      "type": "object",
      "description": "Outbound HTTP call. Templated values validate at execution time after rendering.",
      "properties": {
        "url": { "type": "string", "minLength": 1, "description": "Absolute http(s) url, or a {{ path }} template." },
        "method": { "type": "string", "description": "Blank defaults to GET; templated values resolve at execution time." },
        "headers": { "type": "object", "additionalProperties": { "type": "string" } },
        "body": {}
      },
      "required": ["url"],
      "additionalProperties": false
    },
    "execution_config": {
      "type": "object",
      "description": "Allowlisted command execution. argv is literal and never passed through a shell.",
      "properties": {
        "command": { "type": "array", "minItems": 1, "items": { "type": "string", "minLength": 1 } },
        "stdin": { "type": "string" }
      },
      "required": ["command"],
      "additionalProperties": false
    },
    "timeout": { "$ref": "#/$defs/duration" },
    "output_property": { "$ref": "#/$defs/contextKey" },
    "next_node": { "$ref": "#/$defs/nodeId" },
    "on_failure": { "$ref": "#/$defs/failureRoute" },
    ` + commonNodeSchemaFields + `
  },
  "required": ["type"],
  "oneOf": [{ "required": ["http_config"] }, { "required": ["execution_config"] }],
  "additionalProperties": false
}`

const outputNodeSchemaJSON = `{
  "title": "simpwf output node",
  "description": "Publishes the exact JSON at context_path to a broker channel. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "output" },
    "channel": { "type": "string", "enum": ["redis", "rabbitmq"] },
    "context_path": { "type": "string", "minLength": 1, "description": "Context path whose value is published." },
    "timeout": { "$ref": "#/$defs/duration" },
    "next_node": { "$ref": "#/$defs/nodeId" },
    "output_property": { "$ref": "#/$defs/contextKey" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }
  },
  "required": ["type", "channel", "context_path"],
  "additionalProperties": false
}`

const pollerNodeSchemaJSON = `{
  "title": "simpwf poller node",
  "description": "Repeatedly calls a transport or waits on one until the until predicate returns true. Exactly one transport block is required. Go parsers stay authoritative.",
  "type": "object",
  "properties": {
    "type": { "const": "poller" },
    "http": {
      "type": "object",
      "description": "Repeated HTTP calls. Defaults: GET / 5s delay / 30s request_timeout / 10 attempts.",
      "properties": {
        "url": { "type": "string", "minLength": 1, "description": "Absolute http(s) url, or a {{ path }} template." },
        "method": { "type": "string" },
        "headers": { "type": "object", "additionalProperties": { "type": "string" } },
        "body": {},
        "delay": { "$ref": "#/$defs/duration" },
        "request_timeout": { "$ref": "#/$defs/duration" },
        "max_attempts": { "type": "integer", "minimum": 1, "default": 10 },
        "until": { "type": "string", "minLength": 1, "description": "Predicate evaluated against the normalized response; must return a boolean." }
      },
      "required": ["url", "until"],
      "additionalProperties": false
    },
    "redis": {
      "type": "object",
      "description": "GET requires key; SUB requires channel and forbids key, delay, request_timeout, and max_attempts. MaxWaitTime default 5m.",
      "properties": {
        "method": { "type": "string", "enum": ["GET", "SUB"] },
        "key": { "type": "string" },
        "channel": { "type": "string" },
        "delay": { "$ref": "#/$defs/duration" },
        "request_timeout": { "$ref": "#/$defs/duration" },
        "max_attempts": { "type": "integer", "minimum": 1, "default": 10 },
        "max_wait_time": { "$ref": "#/$defs/duration" },
        "until": { "type": "string", "minLength": 1, "description": "Predicate evaluated against the normalized response; must return a boolean." }
      },
      "required": ["until"],
      "additionalProperties": false
    },
    "rabbitmq": {
      "type": "object",
      "description": "Waits for the first message on a pre-provisioned exclusive queue. MaxWaitTime default 5m.",
      "properties": {
        "queue": { "type": "string", "minLength": 1 },
        "max_wait_time": { "$ref": "#/$defs/duration" },
        "until": { "type": "string", "minLength": 1, "description": "Predicate evaluated against the normalized response; must return a boolean." }
      },
      "required": ["queue", "until"],
      "additionalProperties": false
    },
    "timeout": { "$ref": "#/$defs/duration" },
    "on_failure": { "$ref": "#/$defs/failureRoute" },
    "retry_on_recovery": { "type": "boolean", "default": true, "description": "Pollers are active waits and requeue by default when the worker lease expires." },
    "id": { "$ref": "#/$defs/nodeId" },
    "name": { "type": "string" },
    "output_property": { "$ref": "#/$defs/contextKey" },
    "next_node": { "$ref": "#/$defs/nodeId" },
    "metadata": { "type": "object", "description": "Opaque metadata; never read by the engine." },
    "pre_script": { "$ref": "#/$defs/hook" },
    "post_script": { "$ref": "#/$defs/hook" }
  },
  "required": ["type"],
  "oneOf": [{ "required": ["http"] }, { "required": ["redis"] }, { "required": ["rabbitmq"] }],
  "additionalProperties": false
}`
