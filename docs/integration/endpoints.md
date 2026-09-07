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

| Body field            | Required | Rule                                                                                               |
| --------------------- | -------- | -------------------------------------------------------------------------------------------------- |
| `name`                | yes      | Non-blank string. No max length.                                                                   |
| `type`                | yes      | Enum, see `fields.md`. Must match content shape.                                                   |
| `content`             | yes      | Object. Per-type rules, see `fields.md`. Must not carry `keys`.                                    |
| `previous_version_id` | no       | UUID of existing definition. Sets `version = prev + 1`, inherits `lineage_id`. Unknown id → `404`. |

Malformed JSON → `400`. Blank name/type/content → `422`. Bad type or content → `422`. Response is the full definition
(`id`, `name`, `version`, `previous_version_id`, `lineage_id`, `type`, `content`, `created_by`, `updated_by`,
`created_at`, `updated_at`).

FE note: `content` for a `group` definition must not contain `keys`; keys are supplied at the workflow occurrence.
Referencing occurrence may add `keys`, `next_node`, `output_property`, `on_failure` (external_call/poller only), hooks,
metadata.

### `GET /v1/node/definition` → `200`

| Query         | Rule                                                                                                                               |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `page`        | Integer `>= 1`, default `1`.                                                                                                       |
| `per_page`    | Integer `1`–`200`, default `50`.                                                                                                   |
| `order`       | Allowlist: `id`, `name`, `version`, `lineage_id`, `type`, `created_at`, `updated_at`, optional leading `-`. Default `-created_at`. |
| `id`          | Repeatable UUID, max 100.                                                                                                          |
| `name`        | Exact match, case-sensitive.                                                                                                       |
| `lineage_id`  | Single UUID.                                                                                                                       |
| `version`     | Integer `>= 1`.                                                                                                                    |
| `latest_only` | Boolean.                                                                                                                           |
| `type`        | Node-type string filter.                                                                                                           |

Bad param → `400`. Envelope `{items, page, per_page, total, total_pages}`.

### `GET /v1/node/definition/{id}` → `200`

Malformed UUID → `400`. Unknown → `404`. Returns one definition.

### `DELETE /v1/node/definition/{id}` → `204`

Malformed UUID → `400`. Referenced by any workflow definition or node instance → `409`. Unknown → `404`. Empty body.

## Workflow definitions

Immutable graphs. Same versioning semantics as node definitions.

### `POST /v1/workflow/definition` → `201`

| Body field            | Required | Rule                                                                                                                                                                                     |
| --------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                | yes      | Non-blank string. No max length.                                                                                                                                                         |
| `content`             | yes      | Object: `start_node_id` (UUID, required), `nodes` (non-empty array, required), `keys` (optional), `context_mode` (optional enum), `status_update` (optional). Full rules in `fields.md`. |
| `previous_version_id` | no       | UUID of existing definition. Unknown id → `404`.                                                                                                                                         |

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

## Instances

### `POST /v1/workflow/instance` → `202`

Starts a run. Status is always `waiting` at creation.

| Body field               | Required | Rule                                                                                                                               |
| ------------------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `workflow_definition_id` | yes      | Non-blank string. Blank → `422`. Unknown id → `404` (no `400` here; a malformed UUID can surface as `500`, same caveat as status). |
| `context`                | no       | JSON object. Omitted/`null` → `{}`. Arrays, scalars, malformed JSON → `422`.                                                       |

Definition with unparsable content → `422`. Response `{id, status}` (`status` is `"waiting"`).

FE note: the instance snapshots `context_mode` at creation (definition value wins, else server default). Later
definition edits never affect running instances.

### `GET /v1/workflow/instance` → `200`

| Query                    | Rule                                                                                                                          |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `page` / `per_page`      | Same as definitions (`>= 1` / `1`–`200`, defaults `1` / `50`).                                                                |
| `order`                  | Allowlist: `id`, `workflow_definition_id`, `status`, `created_at`, `updated_at`, optional leading `-`. Default `-created_at`. |
| `id`                     | Repeatable UUID, max 100.                                                                                                     |
| `workflow_definition_id` | Single UUID.                                                                                                                  |
| `status`                 | Repeatable enum: `waiting`, `running`, `paused`, `finished`, `failed`, `stopped`.                                             |

Bad param → `400`. Items are compact summaries: no `context`, frame, counters, or node detail. Nullables
(`waiting_reason`, `error`, `started_at`, `finished_at`) render `null` when empty.

### `GET /v1/workflow/instance/{id}/status` → `200`

Unknown id → `404`. No UUID format check on this route: any shape that matches no row is `404` (a malformed UUID can
surface as `500` if the database rejects the literal instead of returning zero rows, so only send ids the API gave you).
Response:

- Identity, `status` enum, `context_mode` (`full`/`lean`, snapshotted at creation), `waiting_reason` (`null` = runnable,
  `"input"` = parked).
- `pause_requested`, `termination_pending`, `current_group_id`, `current_node_id`, `current_node_instance_id`
  (`"<instance>:<occurrence>"`, `null` when unresolvable).
- `attempt`, `counters` (`{"total":N,"nodes":{...}}`), `error` (`null` when empty), `started_at`/`finished_at`, audit
  fields.
- `nodes`: map of graph node id → `{occurrence_id, status, attempt, rollbackable}`. Never-ran nodes carry
  `occurrence_id: null`, `attempt: null`, `status: "not_started"`. `rollbackable` is advisory; the rollback endpoint is
  the source of truth. Whole map is `null`/omitted when the definition cannot be loaded.

### `GET /v1/workflow/instance/{id}/context` → `200`

Unknown → `404`. Same caveat as status: no UUID format check here, so a malformed id can surface as `500` instead of
`404`. Response `{id, context}` with the full live context object.

### `PUT /v1/workflow/instance/{id}/context` → `200`

Full replacement on paused instances only. Keys absent from the body are dropped.

| Item                             | Rule                                                                                                                |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Body                             | Required. Must be a JSON object. Empty body or invalid JSON → `400`. Valid non-object JSON (array, scalar) → `422`. |
| `X-Context-Update-Reason` header | Optional string. Audit annotation only; context values never enter the audit trail.                                 |

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
  `cancelled`, `started_at`, `finished_at`, `stopped_at`, `duration_ms`, audit timestamps.

### `PUT /v1/workflow/instance/{id}/input` → `202`

Delivers a payload to the parked input node. Only works when the instance is `waiting` with `waiting_reason == "input"`
and the node channel is `http` (this endpoint always delivers as source `http`; `redis`/`rabbitmq` channels are fed by
brokers, never by FE).

| Item                     | Rule                                                                                                                                                       |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Idempotency-Key` header | Required, non-blank → else `422`. Replays return the originally recorded delivery regardless of current state. Use a fresh key per genuinely new delivery. |
| Body                     | Required. Any valid JSON (object, array, scalar). Empty body → `422`. Invalid JSON → `422`.                                                                |

Validation-script rejection → `422` with the rejection message in the problem detail (delivery recorded as
`accepted: false`). Wrong state (terminal, not parked, parked on non-input node, channel mismatch, no live attempt) →
`409`. Success → `202 {"accepted":true}`. A post-hook failure after acceptance still returns `202` but fails the
workflow.

## Controls

No request bodies. Path ids are never format-validated on instance routes: unknown shape → `404`, with the same
malformed-UUID caveat as status (only send back ids the API gave you).

### `POST /v1/workflow/instance/{id}/pause` → `200` or `202`

- Waiting → `200 {status:"paused", pause_requested:false}` (immediate).
- Running → `202 {status:"running", pause_requested:true}` (deferred; settles to paused).
- Already paused → `200` idempotent. Unknown id → `404`. Terminal → `409`. (A missing row at the repository layer
  surfaces as a conflict, so an unknown id here can return `409` instead of `404`; treat `409` on pause/resume/stop as
  "gone or terminal" and refresh.)

### `POST /v1/workflow/instance/{id}/resume` → `200`

Paused → `waiting`. Already waiting → `200` idempotent. Running with a pending pause clears the request. Unknown id →
`404` (same `409` caveat as pause). Terminal → `409`. Response `{status}`.

### `POST /v1/workflow/instance/{id}/stop` → `200`

Waiting/running/paused → `stopped` (stop reason recorded as `"operator"`). Already stopped → `200` idempotent.
Finished/failed → `409`. Unknown id → `404` (same `409` caveat as pause). Response `{status, termination_pending}`;
`true` means a node attempt is still being cancelled and cleaned up.

### `POST /v1/workflow/instance/{id}/rollback` → `200`

Moves a paused or failed instance back to an already-executed occurrence. Instance is always `paused` afterwards (failed
becomes paused, error cleared).

| Body field             | Required | Rule                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ---------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `target_occurrence_id` | yes      | Real executed occurrence id (from the status `nodes` map or an executed debug response). Never-ran `not_started` ids are rejected (`404`, no occurrence row). Blank/missing/malformed body → `400`. Unknown occurrence or occurrence of another instance → `404`. Group node or occurrence whose node left the definition → `422`. Input occurrence that is not finished → `409`. Unrestorable context or history gap/overflow on lean instances → `422`/`409` (history failures map to `409`). |
| `reason`               | no       | Optional audit annotation on the rollback event only.                                                                                                                                                                                                                                                                                                                                                                                                                                           |

Guards: instance must be paused/failed → else `409`; `termination_pending` → `409`. Rolling back onto the currently
parked input occurrence is a no-op `200` (no writes). Any other target atomically supersedes the live park (closed as
stopped/cancelled, `"superseded by rollback"`). Context is restored from the target's `context_before`; input targets
re-arm as a fresh attempt (`waiting_reason: "input"`, fresh `Idempotency-Key` accepted); other targets become runnable.
Response `{status:"paused", current_node_id}`. Resume afterwards to re-execute.
