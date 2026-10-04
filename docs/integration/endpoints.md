# Endpoints

Every route, what it requires, what it returns. Field value rules live in `fields.md`. Auth: `X-Api-Token` on all
`/v1/*` when `auth.enabled=true`. Health never needs auth.

## Health

### `GET /health/live`

Liveness. No params. `200 {"status":"ok"}`.

### `GET /health/ready`

Readiness, pings DB (2s timeout). `200 {"status":"ok"}` or `503 {"status":"unavailable"}`.

## Node definitions

Reusable immutable steps. New versions point at `previous_version_id`; one child per parent (duplicate
`previous_version_id` → `409`).

### `POST /v1/node/definition` → `201`

| Body field          | Required | Rule                                                                                         |
| ------------------- | -------- | -------------------------------------------------------------------------------------------- |
| `name`                | yes      | Non-blank string. No max length.                                                             |
| `type`                | yes      | Enum, see `fields.md`. Must match content shape.                                               |
| `content`             | yes      | Object. Per-type rules, see `fields.md`. Must not carry `keys`.                                  |
| `previous_version_id` | no       | UUID of existing definition. Sets `version = prev + 1`, inherits `lineage_id`. Unknown id → `404`. |

Malformed JSON → `400`. Blank name/type/content → `422`. Bad type or content → `422`. Response is the full definition
(`id`, `name`, `version`, `previous_version_id`, `lineage_id`, `type`, `content`, `created_by`, `updated_by`,
`created_at`, `updated_at`).

FE note: `content` for a `group` definition must not contain `keys`; keys are supplied at the workflow occurrence.
Referencing occurrence may add `keys`, `next_node`, `output_property`, `on_failure` (external_call/poller only), hooks,
metadata. Executable string fields may template the frozen instance roots (`{{ env.SIMPWF_* }}`, `{{ secret.KEY }}`);
definitions store the template text as-is, values resolve at instance runtime (see `fields.md` reserved roots).

### `GET /v1/node/definition` → `200`

| Query       | Rule                                                                                                             |
| ----------- | ---------------------------------------------------------------------------------------------------------------- |
| `page`        | Integer `>= 1`, default `1`.                                                                                         |
| `per_page`    | Integer `1`–`200`, default `50`.                                                                                       |
| `order`       | Allowlist: `id`, `name`, `version`, `lineage_id`, `type`, `created_at`, `updated_at`, optional leading `-`. Default `-created_at`. |
| `id`          | Repeatable UUID, max 100.                                                                                        |
| `name`        | Exact match, case-sensitive.                                                                                     |
| `lineage_id`  | Single UUID.                                                                                                     |
| `version`     | Integer `>= 1`.                                                                                                    |
| `latest_only` | Boolean.                                                                                                         |
| `type`        | Node-type string filter.                                                                                         |

Bad param → `400`. Envelope `{items, page, per_page, total, total_pages}`.

### `GET /v1/node/definition/{id}` → `200`

Malformed UUID → `400`. Unknown → `404`. Returns one definition.

### `DELETE /v1/node/definition/{id}` → `204`

Malformed UUID → `400`. Referenced by any workflow definition or node instance → `409`. Unknown → `404`. Empty body.

## Workflow definitions

Immutable graphs. Same versioning semantics as node definitions.

### `POST /v1/workflow/definition` → `201`

| Body field          | Required | Rule                                                                                                                                                                         |
| ------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                | yes      | Non-blank string. No max length.                                                                                                                                             |
| `content`             | yes      | Object: `start_node_id` (UUID, required), `nodes` (non-empty array, required), `keys` (optional), `context_mode` (optional enum), `status_update` (optional). Full rules in `fields.md`. |
| `previous_version_id` | no       | UUID of existing definition. Unknown id → `404`.                                                                                                                               |

Malformed JSON → `400`. Blank name/content → `422`. Bad content (bad links, unknown `node_definition_id`, type mismatch
on references) → `422`. Response is the full definition (same shape as node definition minus `type`).

FE note: every `node_definition_id` used must already exist; the create call resolves and validates all references
before storing. Authored `content` is stored as-is.

### `GET /v1/workflow/definition` → `200`

Same params as node list except `order` allowlist drops `type` (`id`, `name`, `version`, `lineage_id`, `created_at`,
`updated_at`), and `type` param is rejected with `400` here. Bad param → `400`.

### `GET /v1/workflow/definition/{id}` → `200`

Malformed UUID → `400`. Unknown → `404`.

### `DELETE /v1/workflow/definition/{id}` → `204`

Malformed UUID → `400`. Referenced by any request or instance → `409`. Unknown → `404`. Empty body.

## Secrets

Credential table frozen into each instance's `secret` root at creation. Plaintext never returns over HTTP; every read
is masked.

### `POST /v1/secrets` → `201`

| Body field | Required | Rule                                             |
| ---------- | -------- | ------------------------------------------------ |
| `key`        | yes      | `^[A-Za-z0-9_]{1,128}$`. Blank/pattern miss → `422`. |
| `value`      | yes      | 1–8192 characters. Blank/out of range → `422`.     |

Malformed JSON → `400`. Duplicate key → `409`. Response `{key, value_masked: "********", created_at, updated_at}`.

### `GET /v1/secrets` → `200`

`page` / `per_page` same rules as definitions; bad → `400`. No `order` param; fixed `key` ascending. Envelope
`{items, page, per_page, total, total_pages}`; every item masked.

### `GET /v1/secrets/{key}` → `200`

Key outside `^[A-Za-z0-9_]{1,128}$` → `400`. Unknown → `404`. Returns one masked secret.

### `DELETE /v1/secrets/{key}` → `204`

Key outside `^[A-Za-z0-9_]{1,128}$` → `400`. Unknown → `404`. Empty body. Running instances keep their frozen copy;
only new instances see the deletion.

## Instances

### `POST /v1/workflow/instance` → `202`

Starts a run. Normal runs start `waiting`; step-through runs (`"debug": true`) start `paused`.

| Body field             | Required | Rule                                                                                                                                                                                                                                       |
| ---------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `workflow_definition_id` | yes      | Non-blank string. Blank → `422`. Unknown id → `404` (no `400` here; a malformed UUID can surface as `500`, same caveat as status).                                                                                                                 |
| `context`                | no       | JSON object. Omitted/`null` → `{}`. Arrays, scalars, malformed JSON → `422`.                                                                                                                                                                     |
| `debug`                  | no       | Boolean. Omitted/`false` → normal run. `true` → step-through run: starts `paused` (runnable, `waiting_reason: null`), and each `POST .../resume` advances exactly one engine transition before re-pausing until termination. Immutable after create. |

Definition with unparsable content → `422`. Response `{id, status}` (`"waiting"`, or `"paused"` for a debug run).

Debug stepping: input parks stay `waiting` / `"input"` (never force-paused), so `PUT .../input` works unchanged; an
accepted input delivery that advances the cursor lands `paused` instead of `waiting`, while a terminal delivery stays
terminal. Debug pauses carry `pause_requested: false`, unlike a deferred operator pause (`pause_requested: true`).
All other controls behave as usual: context replace and rollback apply wherever `paused` allows them, and pause is
idempotent on a step-paused instance.

FE note: the instance snapshots `context_mode` at creation (definition value wins, else server default). Later
definition edits never affect running instances.

### `GET /v1/workflow/instance` → `200`

| Query                  | Rule                                                                                                            |
| ---------------------- | --------------------------------------------------------------------------------------------------------------- |
| `page` / `per_page`        | Same as definitions (`>= 1` / `1`–`200`, defaults `1` / `50`).                                                            |
| `order`                  | Allowlist: `id`, `workflow_definition_id`, `status`, `created_at`, `updated_at`, optional leading `-`. Default `-created_at`. |
| `id`                     | Repeatable UUID, max 100.                                                                                       |
| `workflow_definition_id` | Single UUID.                                                                                                    |
| `status`                 | Repeatable enum: `waiting`, `running`, `paused`, `finished`, `failed`, `stopped`.                                           |

Bad param → `400`. Items are compact summaries: no `context`, frame, counters, or node detail. Each item carries
the `debug` step-through flag. Nullables
(`waiting_reason`, `error`, `started_at`, `finished_at`) render `null` when empty.

### `GET /v1/workflow/instance/{id}/status` → `200`

Unknown id → `404`. No UUID format check on this route: any shape that matches no row is `404` (a malformed UUID can
surface as `500` if the database rejects the literal instead of returning zero rows, so only send ids the API gave you).
Response:

- Identity, `status` enum, `context_mode` (`full`/`lean`, snapshotted at creation), `debug` (step-through flag,
  immutable after create), `waiting_reason` (`null` = runnable, `"input"` = parked, `"parallel"` = parked on a join).
- `pause_requested`, `termination_pending`, `current_group_id`, `current_node_id`, `current_node_instance_id`
  (`"<instance>:<occurrence>"`, `null` when unresolvable).
- `attempt`, `counters` (`{"total":N,"nodes":{...}}`), `error` (`null` when empty), `started_at`/`finished_at`, audit
  fields.
- `nodes`: map of graph node id → `{occurrence_id, status, attempt, rollbackable}`. Never-ran nodes carry
  `occurrence_id: null`, `attempt: null`, `status: "not_started"`. `rollbackable` is advisory; the rollback endpoint is
  the source of truth. Whole map is `null`/omitted when the definition cannot be loaded.
- `parallel`: list of fork/join executions with their branches, `null`/omitted when the instance never forked. Each
  execution carries `{id, parent_branch_id, depth, start_node_id, end_node_id, status, branch_count,
  completed_count, branches[]}`; each branch `{id, name, branch_index, start_node_id, status, waiting_reason, error,
  updated_at}`. A branch parked on an input node shows `status: "waiting"`, `waiting_reason: "input"`: deliver to it
  with `PUT .../input?branch_id=<branch id>`. Full enum lists are in `fields.md`.

### `GET /v1/workflow/instance/{id}/context` → `200`

Unknown → `404`. Same caveat as status: no UUID format check here, so a malformed id can surface as `500` instead of
`404`. Response `{id, context}` with the full live context object.

### `PUT /v1/workflow/instance/{id}/context` → `200`

Full replacement on paused instances only. Keys absent from the body are dropped.

| Item                           | Rule                                                                                                            |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| Body                           | Required. Must be a JSON object. Empty body or invalid JSON → `400`. Valid non-object JSON (array, scalar) → `422`. |
| `X-Context-Update-Reason` header | Optional string. Audit annotation only; context values never enter the audit trail.                             |

Unknown id → `404`. Not paused (including concurrent resume/stop winning the race) → `409`. Response `{id, context}`
with the replaced context.

### `GET /v1/workflow/instance/{id}/status/node/{node_id}` → `200`

`node_id` accepts a graph node id or an occurrence id. `attempt` query selects a loop execution: omitted → latest;
positive integer → exact attempt; `0`/negative/garbage → `400`; beyond latest → `404`. Unknown node or occurrence →
`404`.

- Never-ran graph node → `200` with `status: "not_started"`, `occurrence_id` echoing the graph node id (not a real
  occurrence; not usable as a rollback target), `selected_attempt`/`latest_attempt` `null`, `attempt_count: 0`,
  snapshots `null`.
- Executed node → `context_before`, `context_after`, `input`, `output`, `error`, `recovery_policy`, `recovery_result`,
  `cancelled`, `started_at`, `finished_at`, `stopped_at`, `duration_ms`, audit timestamps. Per-type `output` shapes
  are listed under "Node outputs" in `fields.md`.

### `GET /v1/workflow/instance/{id}/debug/context` → `200`

Debug instances only: `debug: false` → `409`. `node_id` query omitted selects the debug cursor (falling back to the
live redacted instance context when the cursor is empty or the node never ran); an explicit `node_id` accepts a graph
node id or an occurrence id. `attempt` follows the node-debug rule: omitted → latest; `0`/negative/garbage → `400`;
beyond latest → `404`. Malformed `node_id` → `400`; unknown instance/node → `404`.

Response `{instance_id, node_id, occurrence_id, attempt, is_debug_paused, typescript}`.
`occurrence_id`/`attempt` are `null` when the position has no occurrence. `is_debug_paused` is `true` when the status
is `paused`. `typescript` is an inline structural `declare const context` declaration for Monaco `addExtraLib()`,
rendered from the secret-redacted snapshot and never carrying literal values.

### `PUT /v1/workflow/instance/{id}/input` → `202`

Delivers a payload to the parked input node. Only works when the instance is `waiting` with `waiting_reason == "input"`
and the node channel is `http` (this endpoint always delivers as source `http`; `redis`/`rabbitmq` channels are fed by
brokers, never by FE).

| Item                   | Rule                                                                                                                                                     |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Idempotency-Key` header | Required, non-blank → else `422`. Replays return the originally recorded delivery regardless of current state. Use a fresh key per genuinely new delivery. |
| Body                   | Required. Any valid JSON (object, array, scalar). Empty body → `422`. Invalid JSON → `422`.                                                                  |
| `branch_id` query        | Optional branch id (from the status `parallel` tree). Targets a branch-parked input node instead of the parent park; omit for the parent park. Unknown branch → `404`; branch of another instance → `409`. |

Validation-script rejection → `422` with the rejection message in the problem detail (delivery recorded as
`accepted: false`). Wrong state (terminal, not parked, parked on non-input node, channel mismatch, no live attempt) →
`409`. Success → `202 {"accepted":true}`. A post-hook failure after acceptance still returns `202` but fails the
workflow.

A branch delivery requires the parent parked on its join (`waiting` or `paused` with `waiting_reason == "parallel"`)
and the branch itself `waiting`/`"input"` on an `http` input node; anything else → `409`. The delivery advances only
the branch (its attempt finishes, the branch wakes runnable); the parent never moves. Idempotency keys are
instance-wide: a key already used on any park replays the original delivery.

#### Anonymous delivery to a `public` node

This is the only endpoint in `/v1` that serves a caller with no credential, and only when the parked input node sets
`public: true`:

| Request                               | Result                                                                             |
| ------------------------------------- | ---------------------------------------------------------------------------------- |
| No `X-Api-Token`, node has `public: true` | `202` — accepted, written as the bare payload, audited with `delivered_by: anonymous`  |
| No `X-Api-Token`, node is not `public`    | `403` — decided after the parked node is loaded, before any delivery row is written  |
| Wrong `X-Api-Token` sent, any node      | `401` — a refused credential never degrades to an accepted anonymous delivery        |
| Valid `X-Api-Token`, any node           | Normal path: the two input gates run, and the delivery is attributed to the caller |

A blank or whitespace-only header counts as no credential, since no client means anything by it; a header with any
content in it is a credential and is checked.

The other instance routes are unaffected: `GET .../status` and every other read still require a credential, so an
anonymous caller cannot fetch the form schema or the context to discover what to send. Share the instance id and the
expected shape out-of-band, the way a webhook URL is shared. The authenticated status response reports
`pending_input.public` so a frontend can tell in advance.

## Controls

No request bodies. Path ids are never format-validated on instance routes: unknown shape → `404`, with the same
malformed-UUID caveat as status (only send back ids the API gave you).

### `POST /v1/workflow/instance/{id}/pause` → `200` or `202`

- Waiting → `200 {status:"paused", pause_requested:false}` (immediate).
- Running → `202 {status:"running", pause_requested:true}` (deferred; settles to paused).
- Already paused → `200` idempotent. Unknown id → `404`. Terminal → `409`. (A missing row at the repository layer
  surfaces as a conflict, so an unknown id here can return `409` instead of `404`; treat `409` on pause/resume/stop as
  "gone or terminal" and refresh.)

Pausing mid-parallel parks only the parent: live branches keep running, and the barrier still flips the paused parent
runnable when the last branch arrives, so a later resume lands claimable and runs the join.

### `POST /v1/workflow/instance/{id}/resume` → `200`

Paused → `waiting`. Already waiting → `200` idempotent. Running with a pending pause clears the request. Unknown id →
`404` (same `409` caveat as pause). Terminal → `409`. Response `{status}`. On debug runs each resume advances
exactly one step before the instance re-pauses, so keep offering resume while `debug` is true and status returns to
`paused`. A debug resume with paused branches wakes exactly one branch — shallowest execution first, then
alphabetically — while the parent stays `paused`; the parent wakes once no paused branch remains. A branch with a
step already in flight → `409`, so wait and retry.

### `POST /v1/workflow/instance/{id}/stop` → `200`

Waiting/running/paused → `stopped` (stop reason recorded as `"operator"`). Already stopped → `200` idempotent.
Finished/failed → `409`. Unknown id → `404` (same `409` caveat as pause). Response `{status, termination_pending}`;
`true` means a node attempt is still being cancelled and cleaned up. On instances with live parallel branches, stop
cancels the whole tree (live siblings, nested subtrees) in the same transaction; `termination_pending` covers
in-flight branch workers too.

### `POST /v1/workflow/instance/{id}/rollback` → `200`

Moves a paused or failed instance back to an already-executed occurrence. Instance is always `paused` afterwards (failed
becomes paused, error cleared).

| Body field           | Required | Rule                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| -------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `target_occurrence_id` | yes      | Real executed occurrence id (from the status `nodes` map or an executed debug response). Never-ran `not_started` ids are rejected (`404`, no occurrence row). Blank/missing/malformed body → `400`. Unknown occurrence or occurrence of another instance → `404`. Group node or occurrence whose node left the definition → `422`. Occurrence that ran inside a parallel branch → `422` (roll back to a node before or after the block). Input occurrence that is not finished → `409`. Unrestorable context or history gap/overflow on lean instances → `422`/`409` (history failures map to `409`). |
| `reason`               | no       | Optional audit annotation on the rollback event only.                                                                                                                                                                                                                                                                                                                                                                                                                       |

Guards: instance must be paused/failed → else `409`; `termination_pending` → `409`; live parallel branches (any
execution still waiting for branches or for its join) → `409`. Rolling back onto the currently
parked input occurrence is a no-op `200` (no writes). Any other target atomically supersedes the live park (closed as
stopped/cancelled, `"superseded by rollback"`). Context is restored from the target's `context_before`; input targets
re-arm as a fresh attempt (`waiting_reason: "input"`, fresh `Idempotency-Key` accepted); other targets become runnable.
Targets before or after a completed parallel block are accepted: re-execution forks a fresh execution and old rows stay
as history. Response `{status:"paused", current_node_id}`. Resume afterwards to re-execute.

## Statistics

Read-only aggregate for dashboards. Same auth as every other `/v1/*` route. No writes, no state guards,
no `409`/`422` paths: bad query → `400`, service failure → `500`.

### `GET /v1/statistics` → `200`

Aggregate run counts over an inclusive creation window. No pagination.

| Query        | Rule                                                                                           |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `created_from` | Optional RFC3339 timestamp, inclusive lower bound on instance `created_at`. Garbage → `400`.       |
| `created_to`   | Optional RFC3339 timestamp, inclusive upper bound on instance `created_at`. Garbage → `400`.       |
| `order`        | `date` or `-date` only, default `-date`. Selects `runs_per_day` bucket direction. Anything else → `400`. |

`created_from` after `created_to` → `400`. Response:

```json
{
  "total_runs": 5,
  "finished_runs": 1,
  "failed_runs": 1,
  "stopped_runs": 1,
  "terminal_runs": 3,
  "active_runs": 2,
  "success_rate": 0.3333333333333333,
  "average_duration_ms": 90000,
  "runs_per_day": [
    {"date": "2026-09-21", "total_runs": 2, "finished_runs": 0, "failed_runs": 0, "stopped_runs": 1},
    {"date": "2026-09-20", "total_runs": 3, "finished_runs": 1, "failed_runs": 1, "stopped_runs": 0}
  ]
}
```

- `total_runs` counts every status; `terminal_runs` = finished + failed + stopped. `active_runs` = waiting + running
  + paused in the same creation window. Narrow windows can report `active_runs: 0` while workers are busy globally.
- `success_rate` = finished / terminal, `null` when terminal is 0. `average_duration_ms` = mean
  `finished_at - started_at` in ms over terminal runs with both timestamps set, `null` when none qualify.
- `runs_per_day` buckets group by calendar day of `created_at` (`date` is `YYYY-MM-DD`), ordered by `order`. Empty
  window returns zeros with `runs_per_day: []` (empty array, never `null`).
