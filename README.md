# SimpWF — Durable workflow engine on PostgreSQL

[![CI](https://github.com/didasy/simpwf/actions/workflows/ci.yml/badge.svg)](https://github.com/didasy/simpwf/actions/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/Coverage-78.3%25-brightgreen)
[![Go](https://img.shields.io/badge/Go-1.26-blue)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Define a workflow once, run it durably. SimpWF executes script, approval,
> HTTP, and broker steps on top of PostgreSQL, so crashed workers never lose
> your place.

SimpWF is a Go workflow engine with **immutable definitions**, a **leased
state machine** over PostgreSQL (`FOR UPDATE SKIP LOCKED` claims, leases,
heartbeats), and **HTTP APIs** for input, control, and debugging. Redis
pub/sub and RabbitMQ queues are optional transports for broker input/output
nodes and status notifications. They stay off when their DSN is empty.

## Contents

- [Why SimpWF](#why-simpwf)
- [Features](#features)
- [Quickstart](#quickstart)
- [Your first workflow](#your-first-workflow)
- [How it works](#how-it-works)
- [Node types](#node-types)
- [Waiting, polling, and failure handling](#waiting-polling-and-failure-handling)
- [Transports](#transports)
- [Status notifications](#status-notifications)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Why SimpWF

- **Durable by default.** Every transition checkpoints cursor, context, and
  counters. A dead worker requeues; the next claim resumes.
- **No broker required.** HTTP-only mode works out of the box. Add Redis or
  RabbitMQ later without rewriting workflows.
- **Human steps fit naturally.** Park on an `input` node, collect a webhook
  or broker payload with idempotent delivery, resume.
- **Observable.** Per-node attempts, context before/after snapshots,
  append-only events, and per-occurrence debug views.
- **Safe by construction.** Sandboxed scripts, HTTP/exec allowlists, fencing
  on every checkpoint so stale workers always lose.

## Features

- Immutable node and workflow definitions with linear versioning
  (`previous_version_id`, `lineage_id`), recursive materialization, and graph
  validation.
- Node types: `script` (Goja ES5.1 sandbox, no `eval`), `conditions`
  (exactly-one-match branching), `input` (HTTP / Redis / RabbitMQ with
  validation script and `Idempotency-Key` dedupe), `output` (publish a
  `context_path` to Redis or RabbitMQ, receipt returned), nested `group`,
  `external_call` (outbound HTTP or allowlisted command), `poller` (repeat
  HTTP, Redis `GET`/`SUB`, or RabbitMQ until an `until` predicate matches).
- Durable cursor (`frame`), per-node/total execution counters, full
  `context_before`/`context_after` snapshots, append-only event log.
- Dispatcher claims via `SELECT ... FOR UPDATE SKIP LOCKED`, fenced
  checkpoints (lease + revision), heartbeat renewal, crash recovery with
  per-type retry policy.
- Controls: `pause` (immediate or deferred), `resume`, `stop` (terminal,
  fences workers, cancels in-flight Goja/HTTP/command work locally and
  across replicas), and `rollback` to a prior node occurrence (lands paused,
  restores `context_before`).
- Node debug: latest/exact attempts, running snapshots, `not_started` views.
- Status notifications: per-definition `status_update` fan-out over HTTP,
  Redis, and RabbitMQ from a PostgreSQL transactional outbox, ordered per
  instance and per transport.

## Quickstart

**Requirements:** Go 1.26+, Docker, PostgreSQL 16. Optional: Atlas CLI,
[`task`](https://taskfile.dev), `curl`, `jq`.

### Option A: Docker Compose (fastest, everything wired)

```bash
docker compose up --build
curl http://localhost:8080/health/ready
# Swagger UI: http://localhost:8080/swagger/index.html
# RabbitMQ UI:  http://localhost:15672 (simpwf/simpwf)
```

Stack ports: app `8080`, PostgreSQL `9921`, Redis `6379`, RabbitMQ `5672`.
To run broker-free, drop `SIMPWF_INFRA_REDIS_DSN` and
`SIMPWF_INFRA_RABBITMQ_DSN` from the `app` service.

> Auth note: if `SIMPWF_AUTH_ENABLED=true`, every `/v1` call needs
> `-H "X-Api-Token: <token>"`. `/health/*` stays public.

### Option B: Local Go (PostgreSQL only)

```bash
# PostgreSQL dev container (matches config.yaml)
docker run -d --name simpwf-pg -p 9921:5432 \
  -e POSTGRES_USER=gorm -e POSTGRES_PASSWORD=gorm -e POSTGRES_DB=gorm \
  postgres:16-alpine

# Migrate (Atlas owns the schema; the app never migrates)
atlas migrate apply --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"

# Run API + dispatcher
go run ./cmd/app -config config.yaml
# -> http://localhost:9999 (health: /health/live, /health/ready)
```

Seed a sample workflow, or run black-box checks (pass explicit workflow
JSON; `workflow.yaml` is the annotated sample definition):

```bash
bash scripts/seed.sh [BASE_URL]
bash scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]
```

## Your first workflow

Create a definition (script → approval input → script), run it, deliver the
approval, read the result. Save as `hello.json`:

```json
{
  "name": "hello-approval",
  "content": {
    "start_node_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
    "nodes": [
      {
        "id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        "type": "script",
        "name": "Greet",
        "script": "return { greeting: 'hello ' + context.user.name };",
        "output_property": "greeting",
        "next_node": "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
      },
      {
        "id": "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
        "type": "input",
        "name": "Approval",
        "channel": "http",
        "context_path": "approval",
        "next_node": "cccccccc-cccc-7ccc-8ccc-cccccccccccc"
      },
      {
        "id": "cccccccc-cccc-7ccc-8ccc-cccccccccccc",
        "type": "script",
        "name": "Finish",
        "script": "return { done: true, by: context.approval.approved };",
        "output_property": "result"
      }
    ]
  }
}
```

```bash
BASE=http://localhost:9999  # compose: http://localhost:8080

# 1. Create definition
WF_ID=$(curl -fsS -X POST $BASE/v1/workflow/definition \
  -H 'Content-Type: application/json' --data-binary @hello.json | jq -er .id)
echo "definition: $WF_ID"

# 2. Start instance
INST_ID=$(curl -fsS -X POST $BASE/v1/workflow/instance \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"context\":{\"user\":{\"name\":\"Jono\"}}}" \
  | jq -er .id)
echo "instance: $INST_ID"

# 3. Wait until parked on input, then approve (idempotent)
curl -fsS $BASE/v1/workflow/instance/$INST_ID/status | jq '{status, waiting_reason}'
curl -fsS -X PUT $BASE/v1/workflow/instance/$INST_ID/input \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: approve-1' \
  -d '{"approved":true}' | jq .

# 4. Read result
curl -fsS $BASE/v1/workflow/instance/$INST_ID/status | jq '{status, error}'
curl -fsS $BASE/v1/workflow/instance/$INST_ID/context | jq .
```

Controls when you need them:

```bash
curl -X POST $BASE/v1/workflow/instance/$INST_ID/pause   # 200 now, 202 deferred
curl -X POST $BASE/v1/workflow/instance/$INST_ID/resume
curl -X POST $BASE/v1/workflow/instance/$INST_ID/stop
```

## How it works

One claim executes exactly one node transition, then checkpoints:

1. Dispatcher claims a runnable instance (`FOR UPDATE SKIP LOCKED`), marks
   it `running`, leases it to a worker.
2. Engine runs the node (script sandbox, HTTP/command, broker publish, or
   park on input), then checkpoints cursor + context with fencing
   (`WHERE id = ? AND revision = ? AND leased_by = ?`). Stale workers get
   `ErrLeaseLost` and abort silently. A `stop` always wins.
3. Heartbeats renew leases and propagate termination across replicas.
4. Recovery: scripts requeue as a new attempt, input nodes re-enter waiting.
   `conditions`, `external_call`, `output`, and `poller` nodes requeue only
   with `retry_on_recovery: true` (pollers default true); otherwise node and
   workflow fail.

Deep dive: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Node types

| Type          | Does                                                                                                                                                            | Waits?  |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `script`        | Goja ES5.1 snippet. Reads frozen `context` (+ `input` from `input_data`). Return value lands on `output_property` (default: node id).                                   | No      |
| `conditions`    | Evaluates all branches, routes on the single match via workflow/group `keys`. Zero or multiple matches fail. Needs ≥2 conditions. No `next_node` / `output_property`. | No      |
| `input`         | Parks with `waiting_reason: input`. Accepts payload on its `channel` (`http` / `redis` / `rabbitmq`), validates, writes to `context_path`. `Idempotency-Key` dedupes.         | Yes     |
| `group`         | Nested nodes with own `start_node_id` and local `keys`.                                                                                                             | Depends |
| `external_call` | Outbound HTTP (`http_config`, `{{ context.path }}` templates) or allowlisted command (`execution_config`, argv only, no shell).                                       | No      |
| `output`        | Publishes exact JSON of `context_path` to Redis (`workflow:output:<id>`) or RabbitMQ (`output_queue`). Returns `{channel, destination, message_id}` receipt.            | No      |
| `poller`        | Actively waits in a worker slot until `until` returns `true`. Exactly one of `http` / `redis` / `rabbitmq`. Full normalized response is the node output.                  | Yes     |

Links and scoping rules: `script`/`input`/`output`/`group`/`external_call`/`poller`
may carry `next_node` (empty ends the group/workflow). `conditions` select
`keys` names; missing/null/blank key exits the group/workflow. Group children
link only inside the group. See [`workflow.yaml`](workflow.yaml).

## Waiting, polling, and failure handling

**Pollers.** First HTTP/`GET` attempt starts immediately; `delay` applies
between attempts; every attempt (including transport errors) counts toward
`max_attempts`. HTTP statuses are responses for `until`, not errors. `until`
must return a real boolean; non-boolean, error, or timeout fails the node.
Redis `SUB` / RabbitMQ waits discard non-matching messages up to
`max_wait_time`. Missing Redis key is a normal `body: null` response.
`workflow_instance_id` / `node_instance_id` template roots are read-only and
never persisted. Redis/RabbitMQ pollers need the matching broker DSN.

| Transport | Response shape            | Defaults                                            |
| --------- | ------------------------- | --------------------------------------------------- |
| `http`      | `{ body, headers, status }` | `GET`, `delay 5s`, `request_timeout 30s`, `max_attempts 10` |
| `redis` GET | `{ body }`                  | `delay 5s`, `request_timeout 30s`, `max_attempts 10`      |
| `redis` SUB | `{ body }`                  | `max_wait_time 5m`                                    |
| `rabbitmq`  | `{ body, headers }`         | `max_wait_time 5m`                                    |

**Lifecycle hooks.** Every node accepts `pre_script` / `post_script`
context transforms (same sandbox, hard timeout). Return values ignored; only
context mutations persist. `post_script` also sees frozen `output`. Group
pre runs before first child, post after last child (innermost first). Input
pre runs once before parking; post runs once per accepted delivery. Hook
failure fails node and workflow. Handled `on_failure` executions skip
`post_script`. Node definitions supply hook defaults; occurrences override
with an object, disable with explicit `null`, inherit by omitting.

**Failure routing.** `external_call` and `poller` nodes accept `on_failure:
{ next_node, output_property }`. Handled failures mark the attempt `failed`,
store `{ message, reason, result }`, skip `post_script`, emit
`node_failed` + `node_failure_routed`, and continue at the fallback without
failing the workflow. With `on_failure`, HTTP status ≥ 300 routes with reason
`http-status`; without it, ≥ 300 is normal output. Exhausted pollers route;
interrupted nodes with `retry_on_recovery: false` route with reason
`recovery`.

## Transports

Redis and RabbitMQ are optional. Empty DSN disables the transport; a
configured-but-unreachable broker fails startup.

| Transport | Input node                                                                         | Output node                                  | Status fan-out                                                      |
| --------- | ---------------------------------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------- |
| Redis     | `SUB workflow:input:<instance_id>`, envelope `{idempotency_key, payload}`              | publish `workflow:output:<instance_id>`        | publish `workflow:status:<instance_id>` (best-effort)                 |
| RabbitMQ  | consume `input_queue`, headers `NodeInstanceId` + `IdempotencyKey` (`message_id` fallback) | publish `output_queue`, confirmed + persistent | publish `status_queue`, confirmed + persistent, `message_id` = event id |

Delivering transport must match the input node's `channel`.

## Status notifications

Add a top-level `status_update` block to opt in (any mix of `http`, `redis`,
`rabbitmq`, each with own `max_retry` / `retry_delay`):

```yaml
status_update:
  http:
    url: "https://example.com/workflow-status"
    method: "POST"
    headers:
      Authorization: "Bearer token"
    max_retry: 3
    retry_delay: "5s"
  redis:
    max_retry: 2
    retry_delay: "2s"
  rabbitmq:
    max_retry: 1
    retry_delay: "10s"
```

Events (`waiting_for_input`, `input_received`, `paused`, `resumed`,
`finished`, `failed`, `stopped`) enqueue one outbox row per transport in the
same transaction, sharing one logical event id, delivered strictly in order
per instance and per transport with independent retry/dead-letter. Payloads
carry `id`, `type: workflow.status_changed`, instance/definition ids, event,
`from/to_status`, waiting reasons, `revision`, `occurred_at`, `error` (never
workflow context). Receivers dedupe via `Idempotency-Key` /
`X-SimpWF-Event-ID` (HTTP), `message_id` + `IdempotencyKey` header
(RabbitMQ), or embedded `id` (Redis). Scheduler churn emits nothing.

## Configuration

`config.yaml` or `SIMPWF_*` env vars (see [`.env.example`](.env.example)).
Without a config file, env vars are the source.

| Area            | Key knobs                                                                                            | Defaults                                   |
| --------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| Engine timeouts | `engine.default_node_timeout` / `max_node_timeout` (script, `external_call`, `output`)                       | `30s` / `5m`                                   |
| Script budgets  | `engine.condition_timeout` (conditions, input validation, poller `until`)                                | `5s`                                         |
| HTTP targets    | `engine.http_allowlist` (scheme, host:port, DNS checked per request + redirect; `"*"` = dev only, warns) | allowlisted hosts                          |
| Commands        | `engine.exec_allowlist` (argv only, no shell, process-group kill)                                      | `echo`, `ls`                                   |
| Redis           | `infra.redis.dsn` (`redis://host:6379/0`)                                                                | empty = disabled                           |
| RabbitMQ        | `infra.rabbitmq.dsn` + `input_queue` / `output_queue` / `status_queue`                                       | `simpwf.input`, `simpwf.output`, `simpwf.status` |
| Auth            | `auth.enabled` / `api_token` (`X-Api-Token` on `/v1`; `/health/*` public)                                      | disabled                                   |
| Swagger         | `infra.http.swagger_enabled`                                                                           | enabled                                    |
| Audit           | `system.*` actor for `created_by`/`updated_by` and audit events                                            | `system`                                     |

List values accept comma-separated env, e.g.
`SIMPWF_ENGINE_HTTP_ALLOWLIST="api.example.com,jsonplaceholder.typicode.com"`.

## API reference

Full contract: [`api/openapi.yaml`](api/openapi.yaml). Interactive docs:
`/swagger/index.html` (regen with `task swagger` after annotation changes).

| Method     | Path                                             | Purpose                                                             |
| ---------- | ------------------------------------------------ | ------------------------------------------------------------------- |
| GET        | `/health/live`, `/health/ready`                      | Liveness / readiness (public)                                       |
| POST       | `/v1/node/definition`                              | Create node definition (immutable)                                  |
| GET        | `/v1/node/definition`                              | List (paged, `latest_only`, `type`, …)                                  |
| GET/DELETE | `/v1/node/definition/{id}`                         | Get / delete                                                        |
| POST       | `/v1/workflow/definition`                          | Create workflow definition (immutable)                              |
| GET        | `/v1/workflow/definition`                          | List (paged, `latest_only`, …)                                        |
| GET/DELETE | `/v1/workflow/definition/{id}`                     | Get / delete                                                        |
| POST       | `/v1/workflow/instance`                            | Create instance (202)                                               |
| GET        | `/v1/workflow/instance`                            | List (`id` / `workflow_definition_id` / `status`)                         |
| GET        | `/v1/workflow/instance/{id}/status`                | Status, counters, cursor, per-node occurrence map, audit actors     |
| GET        | `/v1/workflow/instance/{id}/context`               | Full context JSON                                                   |
| PUT        | `/v1/workflow/instance/{id}/context`               | Replace context (paused only, `X-Context-Update-Reason` optional)     |
| GET        | `/v1/workflow/instance/{id}/status/node/{node_id}` | Node debug (`?attempt=N`)                                             |
| PUT        | `/v1/workflow/instance/{id}/input`                 | Deliver input (`Idempotency-Key`; 202 accepted, 422 rejected)         |
| POST       | `/v1/workflow/instance/{id}/pause`                 | Pause (200 immediate / 202 deferred)                                |
| POST       | `/v1/workflow/instance/{id}/resume`                | Resume                                                              |
| POST       | `/v1/workflow/instance/{id}/stop`                  | Force stop (terminal)                                               |
| POST       | `/v1/workflow/instance/{id}/rollback`              | Roll back paused/failed instance to prior occurrence (lands paused) |

Errors use RFC 7807 `problem+json`.

## Development

```bash
task test        # full suite (needs scratch DBs on :9921, see Taskfile)
task test-race   # with race detector
task lint        # golangci-lint
task swagger     # regenerate docs/ from annotations
gofmt -l .                     # must print nothing
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
golangci-lint run ./...
atlas migrate validate --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"
```

Layout:

```
cmd/app                  composition root (config, repos, engine, dispatcher, brokers, router)
cmd/atlas-loader         Atlas schema loader (no AutoMigrate in prod)
internal/workflow/model       domain entities, state machine, validation
internal/workflow/repository  GORM models, fenced claims/checkpoints, outbox
internal/workflow/executor    Goja sandbox, script/conditions/input/output/HTTP/command/poller
internal/workflow/engine      cursor machine, dispatcher, cancellation, recovery
internal/workflow/transport   Redis/RabbitMQ adapters behind narrow interfaces
internal/workflow/inputtransport  broker input consumers
internal/workflow/statusupdate    outbox dispatcher + publishers
internal/workflow/service     use-case orchestration (instances, controls, debug, rollback)
internal/workflow/handler     Gin routes, DTOs, problem+json
pkg/*                    config, database, ids, context paths (no internal imports)
api/openapi.yaml         authoritative API contract
docs/                    generated Swagger docs
migrations/              Atlas config + versioned SQL
workflow.yaml            annotated sample definition
scripts/                 seed.sh + e2e.sh black-box checks
```

## Contributing

1. Fork and branch off `main`.
2. Keep changes surgical: touch only what the task needs, match surrounding
   style, no drive-by refactors.
3. Verify before you push: `gofmt -l .` clean, `go vet ./...`,
   `task test`, and `atlas migrate validate` if migrations changed.
4. Open a PR describing what changed and how you verified it.

## License

MIT © 2026 didasy. See [LICENSE](LICENSE).
