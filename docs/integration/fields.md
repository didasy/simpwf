# Fields, enums, and limits

Value rules for everything FE sends. Endpoint shapes live in `endpoints.md`.

## Max characters

There are none. Every FE-supplied string lands in a `text`, `uuid`, or `jsonb` column (see `migrations/versions/*.sql`);
no `varchar(n)` exists anywhere. `name`, `reason`, `Idempotency-Key`, header values, scripts, urls: unbounded. Practical
caps are payload size and the per-field rules below, not length.

## Ids

All ids are canonical lowercase UUIDv7 (`xxxxxxxx-xxxx-7xxx-8xxx-xxxxxxxxxxxx`). Uppercase, braced, or non-canonical
forms are rejected wherever validated. Applies to: definition/instance/occurrence/lineage ids, `previous_version_id`,
`start_node_id`, every node `id`, `next_node`, `on_failure.next_node`, `keys` targets, `current_group_id`,
`current_node_id`, list `id`/`lineage_id`/`workflow_definition_id` filters, `target_occurrence_id`. Exception: instance
path ids on GET/controls are not format-checked; only send back ids the API gave you. A garbage id normally returns
`404`, but the database can reject the literal first and surface `500`.

## Enums

### Node type (`type`)

`script`, `conditions`, `input`, `group`, `external_call`, `output`, `poller`. Case-sensitive. Unknown → `422`. Drift
warning: `api/openapi.yaml` lists only five (omits `output`, `poller`); code accepts all seven.

### Workflow status (`status`)

`waiting`, `running`, `paused`, `finished`, `failed`, `stopped`. Used in instance responses and the list `status`
filter. Unknown filter → `400`.

### Node occurrence status

Executed occurrences: `waiting`, `running`, `finished`, `failed`, `stopped` (no `paused`). The status-map and debug
views add `not_started` for graph nodes that never ran (`occurrence_id: null`, `attempt: null`).

### Waiting reason (`waiting_reason`)

`null` = runnable, `"input"` = parked on an input node. No other value exists.

### Channels

Input node `channel`: `http`, `redis`, `rabbitmq`. Output node `channel`: `redis`, `rabbitmq` only (`http` → `422`).
Case-sensitive.

### Context mode (`context_mode`)

`full`, `lean`, or omitted (inherit server default). Parsed case-insensitively and trimmed (`" Lean "` works). Anything
else → `422`. Snapshot at instance creation; later edits never affect running instances.

### Poller redis `method`

`GET` or `SUB`, case-insensitive on input, stored uppercase. Anything else → `422`.

### Status-update transports and events

Transports: `http`, `redis`, `rabbitmq` (any combination, at least one). Event names on the wire: `waiting_for_input`,
`input_received`, `paused`, `resumed`, `finished`, `failed`, `stopped`. Payload discriminator is always
`"type": "workflow.status_changed"`. Webhook body carries no workflow context. HTTP methods: `GET`, `POST`, `PUT`,
`PATCH`, `DELETE`, `HEAD` (default `POST`). External-call/poller HTTP `method` has no allowlist; blank defaults to
`GET`; templated values validate after rendering (absolute http(s), host, allowlist, DNS).

## Numeric and pagination limits

| Field                            | Rule                                                                                                                                                                                                                                                                               |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `page`                           | Integer `>= 1`, default `1`.                                                                                                                                                                                                                                                       |
| `per_page`                       | Integer `1`–`200`, default `50`. `total_pages = ceil(total / per_page)`.                                                                                                                                                                                                           |
| `id` filter                      | Repeatable, max 100 values, each a valid UUID.                                                                                                                                                                                                                                     |
| `version` filter                 | Integer `>= 1`.                                                                                                                                                                                                                                                                    |
| `latest_only`                    | Boolean (`true`/`false`, `ParseBool` forms).                                                                                                                                                                                                                                       |
| `order`                          | Allowlisted field + optional single leading `-`. Default `-created_at`. `--x` rejected. Definitions: `id name version lineage_id [type] created_at updated_at` (`type` node-only; rejected on workflow list). Instances: `id workflow_definition_id status created_at updated_at`. |
| Node debug `attempt`             | Omitted = latest. Positive integer = exact loop execution. `0`/negative/garbage → `400`. Beyond latest → `404`.                                                                                                                                                                    |
| `max_attempts` (poller http/get) | `> 0`, default `10`.                                                                                                                                                                                                                                                               |
| `max_retry` (status_update)      | `>= 0`, default `3`.                                                                                                                                                                                                                                                               |

## Durations

Go duration strings (`"30s"`, `"5m"`, `"2s"`). Must parse and be positive. Node `timeout` is additionally capped by
`engine.max_node_timeout` (defaults: default `30s`, cap `5m`, conditions/inputs fixed at `condition_timeout` `5s`, not
settable). Poller `delay` (default `5s`), `request_timeout` (default `30s`), `max_wait_time` (default `5m`), and
status-update `retry_delay` (default `5s`) are never capped by the engine max.

## Workflow content reference

Top level: `start_node_id` (required UUID, must match a top-level node), `nodes` (required, non-empty), `keys`
(optional), `context_mode` (optional), `status_update` (optional). Node ids unique across the whole tree including
nested group children. `next_node`, `on_failure.next_node`, and condition `key` targets must resolve inside the same
scope (workflow or group).

`keys`: `{name: target}`. Names non-empty. Target `null`/empty = exit the scope when selected. Otherwise a UUID sibling
in the same scope. A condition entry with missing/blank `key`, or a key mapped to null/empty, exits the scope.

Per node:

| Type            | Required                                                                           | Optional                                                                                                      | Notes                                                                                                                                                      |
| --------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `script`        | `script` (non-blank)                                                               | `timeout`, `input_data` (context path), `output_property` (default: node id), `next_node`, hooks, `metadata`  | Return value lands on `output_property`.                                                                                                                   |
| `conditions`    | `conditions` array, min 2 entries, each with non-blank `condition` script          | `key` per entry (blank = exit branch)                                                                         | No `next_node`, no `output_property`, no per-condition `next_node`. Every non-blank key must exist in scope keys. Writes no output.                        |
| `input`         | `channel`, `context_path`                                                          | `validation.script` (non-blank), `next_node`, hooks                                                           | Parks with `waiting_reason: "input"`. Accepted payload written to `context_path`, then optional `post_script` runs. Writes no direct output.               |
| `group`         | `start_node_id` (UUID matching a child), `nodes` (non-empty, UUID unique children) | `keys` (group-local), `next_node`                                                                             | Children recurse with the same rules.                                                                                                                      |
| `external_call` | Exactly one of `http_config` / `execution_config`                                  | `timeout`, `output_property`, `next_node`, `on_failure`, `retry_on_recovery`                                  | `http_config.url` required; static urls must be absolute http(s). `execution_config.command` non-empty, args non-empty. Result object → `output_property`. |
| `output`        | `channel` (redis/rabbitmq), `context_path`                                         | `timeout`, `output_property`, `next_node`                                                                     | Publishes exact JSON at path; receipt `{channel, destination, message_id}` → `output_property`.                                                            |
| `poller`        | Exactly one of `http` / `redis` / `rabbitmq` + non-blank `until` predicate         | `output_property` (default: node attempt id), `next_node`, `on_failure`, `retry_on_recovery` (default `true`) | Normalized response `{body, headers?, status?}` → `output_property`. Occupies a worker slot while waiting.                                                 |

Cross-type fields:

- `on_failure`: `{next_node (required UUID sibling), output_property (required non-blank)}`. Only `external_call` and
  `poller`; anything else → `422`.
- `pre_script` / `post_script`: `{script (required, non-blank), timeout?}`. Any node type. Explicit `null` disables an
  inherited definition hook on `node_definition_id` references; omitted inherits.
- `node_definition_id` reference: carries only graph fields (`id`, `name` override, `next_node`, `output_property`,
  `on_failure`, `keys` for groups, hooks, `metadata`, `retry_on_recovery`). Inline executable fields (`script`,
  `conditions`, `channel`, `http_config`, `nodes`, poller blocks) → `422`. Optional `type` must match the referenced
  definition.
- `timeout`: omitted/null = engine default. `branches` anywhere → `422` (define `keys` on the workflow/group instead).

`status_update` block: at least one of `http`/`redis`/`rabbitmq` → else `422`. `http.url` required, absolute http(s).
Header names non-empty, no CR/LF in names or values. Redis publishes to `workflow:status:<instance>`, rabbitmq to the
configured status queue; channel/queue names are fixed, only retry policy is configurable. Input/output broker
addresses: input redis channel `workflow:input:<instance>`, output redis channel `workflow:output:<instance>`, rabbitmq
input/output/status queues from server config.

## Context paths and templates

Paths: dot-separated segments, `name`, `user.name`, `items[0]`. Each key matches `[A-Za-z_][A-Za-z0-9_]*`; indexes
non-negative. Empty path, empty segments, and out-of-range array writes are rejected. Used by `input_data`, input/output
`context_path`, and `{{ path }}` templates.

Templates `{{ context.path }}` allowed in: external-call url/method/header values/body string values; poller
url/method/key/channel/queue. Header names and body property names never render. Reserved read-only roots:
`workflow_instance_id`, `node_instance_id` (usable in templates, never persisted).

## Response-only shapes

- `counters`: `{"total": N, "nodes": {"<graph-node-id>": N}}`.
- `current_node_instance_id`: `"<instance-id>:<occurrence-id>"`, `null` when unresolvable.
- Rollback `rollbackable` hint: true only when the instance is paused/failed without `termination_pending`, the node is
  not a group, the occurrence is `finished`/`failed`/`stopped`, and its `context_before` parses as a JSON object.
- Node debug `duration_ms`: set only when both `started_at` and `finished_at` exist.
  `recovery_policy`/`recovery_result`: `null` unless a recovery happened.
