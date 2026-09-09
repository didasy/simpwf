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

| Field                          | Rule                                                                                                                                                                                                                                                                   |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `page`                           | Integer `>= 1`, default `1`.                                                                                                                                                                                                                                               |
| `per_page`                       | Integer `1`–`200`, default `50`. `total_pages = ceil(total / per_page)`.                                                                                                                                                                                                       |
| `id` filter                      | Repeatable, max 100 values, each a valid UUID.                                                                                                                                                                                                                         |
| `version` filter                 | Integer `>= 1`.                                                                                                                                                                                                                                                          |
| `latest_only`                    | Boolean (`true`/`false`, `ParseBool` forms).                                                                                                                                                                                                                                 |
| `order`                          | Allowlisted field + optional single leading `-`. Default `-created_at`. `--x` rejected. Definitions: `id name version lineage_id [type] created_at updated_at` (`type` node-only; rejected on workflow list). Instances: `id workflow_definition_id status created_at updated_at`. |
| Node debug `attempt`             | Omitted = latest. Positive integer = exact loop execution. `0`/negative/garbage → `400`. Beyond latest → `404`.                                                                                                                                                              |
| `max_attempts` (poller http/get) | `> 0`, default `10`.                                                                                                                                                                                                                                                       |
| `max_retry` (status_update)      | `>= 0`, default `3`.                                                                                                                                                                                                                                                       |

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

| Type          | Configuration (accepted `content` fields)                                                                                                                                             | Input — what the node consumes at runtime                                                                                                                                                                                                                                                                                                                                                                  | Output — what lands in `attempt.output` and `output_property`                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `script`        | `script` (required, non-blank). `timeout`, `input_data` (context path), `output_property`, `next_node`, hooks, `metadata`, `retry_on_recovery`, `id`, `name`                                          | Workflow context as mutable `context` global. `input_data` selects one context-path value, exposed as `input` var. Optional `pre_script` runs before, on a clone.                                                                                                                                                                                                                                                  | Script return value (any JSON: number, string, boolean, object, array, `null`, `undefined` → `null`). Stored verbatim at `output_property` (blank `output_property` = the graph node id). Full mutated context replaces workflow context. `post_script` then runs with native output as frozen `output` global; hook return ignored.                                                                                                                                             |
| `conditions`    | `conditions` array (required, min 2 entries, each with non-blank `condition` script). `key` per entry (blank = exit branch). No `next_node`, no `output_property`.                            | Workflow context as frozen read-only `context` global; writes are discarded. No external payload. Each `condition` script must `return` a boolean. Exactly one must match, else the node fails.                                                                                                                                                                                                                  | `{Matched: boolean, Index: number, Key: string}` (`Index: -1` when nothing matched, which fails the workflow; multiple matches fail). No context write: routing only. Matched non-blank `key` resolves via scope `keys` to the next node; blank `key` exits the scope. Post-hook sees `{matched, index, key}` as frozen `output`.                                                                                                                                                |
| `input`         | `channel` (required: `http`, `redis`, `rabbitmq`). `context_path` (required). `validation.script` (optional, non-blank). `next_node`, hooks. `output_property` accepted but ignored.                | External payload: any valid JSON (object, array, scalar) delivered on `channel` (`PUT .../input` for `http`; broker feed for `redis`/`rabbitmq`). Validation script sees frozen `context` plus the raw payload bytes as string `input` var (so `JSON.parse(input)` first for objects). Non-empty string return = reject with that message (`accepted: false`).                                                               | Accepted payload itself, written verbatim to `context_path` (context write is the effect). `attempt.output` = delivered payload JSON. No separate output object; downstream nodes read the value at `context_path`. Rejected payload: `attempt.output` = `null`, error = rejection message.                                                                                                                                                                                  |
| `group`         | `start_node_id` (required UUID matching a child), `nodes` (required, non-empty, UUID-unique children). `keys` (group-local), `next_node`. Children recurse with the same per-type rules.    | Delegates to child graph starting at `start_node_id`. Child nodes consume context per their own input rows; `pre_script` on the group transforms context once on entry.                                                                                                                                                                                                                                        | No native output of its own. Child outputs accumulate in context under each child's `output_property`. Exiting the group runs group `post_script` chain (innermost-first) with `output: null`.                                                                                                                                                                                                                                                                           |
| `external_call` | Exactly one of `http_config` / `execution_config` (required). `timeout`, `output_property`, `next_node`, `on_failure`, `retry_on_recovery`, hooks, `metadata`                                       | Workflow context for templating: `http_config` renders `url`, `method`, header values, and body string values against context (`{{ path }}`). `execution_config.stdin` renders the same way; `command` argv is literal (never a shell). No reserved roots here (unlike poller).                                                                                                                                        | `http_config` → `{Status: number, Headers: {name: string[]}, Body: any}` (`Body` = parsed JSON when the response parses, else raw string). Non-2xx with `on_failure` set still records this object as `result` inside the failure payload and routes instead of failing. `execution_config` → `{ExitCode: number, Stdout: string, Stderr: string, TimedOut: boolean, Truncated: boolean}` (non-zero exit fails the node). Stored at `output_property` (blank = the graph node id). |
| `output`        | `channel` (required: `redis`/`rabbitmq`; `http` → `422`). `context_path` (required). `timeout`, `output_property`, `next_node`.                                                                       | Reads the value at `context_path` from the live context and publishes its exact JSON. No external request, no script. Missing path fails the node. Redis destination is derived: `workflow:output:<instance-id>`. RabbitMQ destination is the server-configured output queue.                                                                                                                                  | `{Channel: string, Destination: string, MessageID: string}` (`MessageID` = `"<instance-id>:<occurrence-id>"` stable execution id). Stored at `output_property` (blank = the graph node id). Original payload stays in context untouched.                                                                                                                                                                                                                                   |
| `poller`        | Exactly one of `http` / `redis` / `rabbitmq` (required) + non-blank `until` predicate (required). `output_property`, `next_node`, `on_failure`, `retry_on_recovery` (default `true`), hooks, `metadata` | Workflow context for templating (unlike `external_call`, poller templates also get reserved roots `workflow_instance_id` / `node_instance_id`, which win over same-named context values): poller `url`/`method`/`key`/`channel`/`queue` render against context. `until` predicate sees frozen `context` plus frozen `response` var (the normalized shape below) and must `return` a boolean. Occupies a worker slot while waiting. | Normalized `PollerResponse` (lowercase keys via explicit `json` tags): HTTP → `{body: any, headers: {name: string[]}, status: number}`; redis GET/SUB → `{body: any}` (missing GET key = `body: null`); rabbitmq → `{body: any, headers: {name: string}}`. `body` = parsed JSON when it parses, else raw string. Stored at `output_property` (blank = the graph node id). Exhausted budget without a match fails the node.                                                         |

Cross-type fields:

- `output_property`: context key receiving the native node output (any non-group node except `conditions`, which rejects it;
  `input` accepts but ignores it). Blank/omitted/whitespace-only = the graph node id (the node's UUID in the workflow definition; deterministic but still a UUID, so prefer an explicit name for anything downstream must read; UUID keys need bracket access context["<uuid>"]). Always a plain top-level key, not a dotted path; `post_script` runs after
  the write and sees the native output as frozen `output`.
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

## Node outputs (stored shapes)

Every finished non-group, non-conditions attempt stores its native result twice: as `attempt.output` (node debug
`output`) and merged into the workflow context at `output_property` (blank = graph node id). `conditions` writes
nothing; `group` has no native output (children write their own). Field-for-field shapes:

- `script`: the script return value verbatim (any JSON). Context mutations also persist as the new workflow context.
- `conditions`: `{Matched: boolean, Index: number, Key: string}`. Routing only, never written to context.
- `input`: the accepted payload verbatim. Rejected delivery: `attempt.output` = `null`, `error` = rejection message.
- `external_call` http: `{Status: number, Headers: {name: string[]}, Body: any}` (`Body` = parsed JSON when the
  response parses, else raw string).
- `external_call` command: `{ExitCode: number, Stdout: string, Stderr: string, TimedOut: boolean,
  Truncated: boolean}`.
- `output`: `{Channel: string, Destination: string, MessageID: string}` (publish receipt; the published payload
  itself stays at `context_path`).
- `poller` http: `{body: any, headers: {name: string[]}, status: number}` (lowercase keys: `PollerResponse` carries
  explicit `json` tags, unlike the executor result structs above).
- `poller` redis GET/SUB: `{body: any}` (missing GET key = `body: null`).
- `poller` rabbitmq: `{body: any, headers: {name: string}}`.
- `on_failure` route (external_call/poller only): `{message: string, reason: string, result: any}` at
  `on_failure.output_property` (not `output_property`). `result` is the partial native output when one exists (e.g.
  the non-2xx HTTP object), else `null`. The failed attempt keeps `status: "failed"` with `output` = `result`
  (`null` when absent); the workflow itself stays runnable and advances to `on_failure.next_node`.
