# FE integration guide

FE entry point for SimpWF REST API. Three files:

- `README.md` (this file): base URL, auth, conventions, errors, pagination, happy-path flow.
- `endpoints.md`: every endpoint, what it requires, what it returns, status codes.
- `fields.md`: every enum, every limit, per-node-type content reference.

Source of truth is code (`internal/workflow/handler/`, `internal/workflow/model/`, `internal/workflow/service/`).
`api/openapi.yaml` is stale in one place (`NodeDefinitionRequest.type` omits `output` and `poller`, both accepted by
code). Where this guide and `openapi.yaml` disagree, this guide follows code.

## Base URL and auth

- API base path: `/`. All v1 routes live under `/v1`. Health probes under `/health`.
- Auth: header `X-Api-Token: <token>`, required on all `/v1/*` routes only when server runs with `auth.enabled=true`.
  Header name is case-insensitive (standard HTTP semantics); comparison is constant-time, so send the token exactly.
- Missing/invalid token → `401` with `application/problem+json` body.
- Health (`GET /health/live`, `GET /health/ready`) never requires auth.
- Interactive explorer: `GET /swagger/*any` (e.g. `/swagger/index.html`, `/swagger/doc.json`, Swagger 2.0) when server
  runs with swagger enabled (disabled → `404`).

## Conventions

- Request and response bodies are JSON. Success content type is `application/json`.
- All ids (definition, instance, occurrence, lineage, user) are canonical lowercase UUIDv7 strings, e.g.
  `11111111-1111-7111-8111-111111111101`. Uppercase or non-canonical forms are rejected wherever an id is validated.
- Timestamps are RFC 3339 date-time strings (`created_at`, `updated_at`, `started_at`, `finished_at`, `stopped_at`).
- Nullable fields render as JSON `null` when empty (`waiting_reason`, `error`, `started_at`, `finished_at`,
  `previous_version_id`, `occurrence_id`, `attempt`, snapshots, `input`/`output`, `duration_ms`). `waiting_reason: null`
  means runnable.
- `PUT /v1/workflow/instance/{id}/context` is full replacement, not merge. Keys absent from the body are dropped.
- `PUT /v1/workflow/instance/{id}/input` accepts any valid JSON body (object, array, scalar). Every other JSON-object
  body in the API must be an object; arrays, strings, numbers, and bare `null` are rejected there.

## Errors

Error content type is `application/problem+json`:

```json
{
  "type": "about:blank",
  "title": "Unprocessable Entity",
  "status": 422,
  "detail": "name, type, and content are required",
  "instance": "/v1/node/definition"
}
```

Status mapping:

| Status | Meaning here                                                                                                       |
| ------ | ------------------------------------------------------------------------------------------------------------------ |
| 200    | OK (includes pause-immediate, resume, stop, rollback, node debug)                                                  |
| 201    | Definition created                                                                                                 |
| 202    | Accepted: instance created, input accepted, pause deferred (node still running)                                    |
| 204    | Deleted, empty body                                                                                                |
| 400    | Malformed JSON, malformed query param, invalid path uuid on definition routes (`GET`/`DELETE .../definition/{id}`) |
| 401    | Missing/invalid `X-Api-Token` (auth enabled)                                                                       |
| 404    | Unknown id (definition, instance, occurrence, attempt beyond latest)                                               |
| 409    | State conflict: delete while referenced, control on terminal instance, context/input/rollback guard failed         |
| 422    | Semantic validation failure (bad enum, bad content, bad payload, missing required field)                           |
| 500    | Server error (includes malformed instance path ids the database rejects instead of returning zero rows)            |
| 503    | `GET /health/ready` only: database unreachable                                                                     |

Rule of thumb for FE: `400` = fix the request shape, `422` = fix the values, `409` = refresh state first (instance moved
on), `404` on a just-created id = treat as gone and refresh the list.

## Pagination and list filters

Every `GET` list returns the same envelope:

```json
{
  "items": [],
  "page": 1,
  "per_page": 50,
  "total": 0,
  "total_pages": 0
}
```

- `page`: integer `>= 1`, default `1`.
- `per_page`: integer `1`–`200`, default `50`.
- `order`: allowlisted field with optional single leading `-` for descending (e.g. `order=-created_at`). Default
  `-created_at`. `--created_at` rejected. Allowlist differs per endpoint, see `endpoints.md`.
- `id`: repeatable uuid filter (`?id=A&id=B`), max 100 values. Every value must be a valid uuid.
- `name`: exact-match string filter (definitions only), case-sensitive.
- `lineage_id`: single uuid. `version`: integer `>= 1`. `latest_only`: boolean (`true`/`false`; `strconv.ParseBool`
  forms accepted).
- `total_pages` is `ceil(total / per_page)`.

## Happy-path flow

1. Create node definitions (`POST /v1/node/definition`) for reusable steps, or skip them and author nodes inline in the
   workflow content.
2. Create a workflow definition (`POST /v1/workflow/definition`) with `start_node_id`, `nodes`, optional `keys`,
   optional `context_mode`, optional `status_update`. Full sample: `workflow.yaml` at repo root.
3. Start an instance (`POST /v1/workflow/instance`) with `workflow_definition_id` and optional `context` object.
4. Poll `GET .../status` until `status` is `paused` (input wait), `finished`, `failed`, or `stopped`. `nodes` map shows
   per-graph-node progress.
5. If `waiting_reason == "input"`, find the parked input node (status entry with occurrence in `waiting`), then
   `PUT .../input` with the payload and a fresh `Idempotency-Key`.
6. Paused instance with wrong data: `PUT .../context` (full replacement), then `POST .../resume`.
7. Failed/paused instance that must redo work: pick `occurrence_id` from the `nodes` map or node debug API,
   `POST .../rollback`, then `POST .../resume`.
8. Inspect any step: `GET .../status/node/{node_id}` (`context_before`, `context_after`, `input`, `output`, `error`,
   attempts).

## Instance lifecycle (for button state)

```
waiting --claim--> running --checkpoint--> waiting
waiting --pause--> paused --resume--> waiting
running --pause--> running (pause_requested=true, 202) --settles--> paused
waiting|running|paused --stop--> stopped (terminal)
running --finish--> finished (terminal)
running --fail--> failed (terminal)
failed --rollback--> paused (only exception to terminal immutability)
```

Enable controls by status: pause on `waiting`/`running`; resume on `paused`; stop on `waiting`/`running`/`paused`;
context replace and rollback on `paused` only (rollback also on `failed`); input delivery only when `waiting` with
`waiting_reason == "input"`. `finished`/`failed`/`stopped` accept no controls except rollback on `failed`.
`termination_pending == true` blocks rollback.
