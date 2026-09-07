# SimpWF — Durable Workflow Engine on PostgreSQL

[![CI](https://github.com/didasy/simpwf/actions/workflows/ci.yml/badge.svg)](https://github.com/didasy/simpwf/actions/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/Coverage-78.3%25-brightgreen)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SimpWF is a durable workflow engine in Go. Workflow definitions are
immutable and versioned, execution state lives in PostgreSQL, and workers
claim work with `SELECT ... FOR UPDATE SKIP LOCKED` under leases and
heartbeats. An HTTP API covers definitions, instances, input delivery,
pause/resume/stop/rollback controls, and per-node debugging. Optional Redis
and RabbitMQ transports add broker input nodes, broker output nodes, and
status notifications. Both stay fully disabled when their DSN is absent.

## Contents

- [Why SimpWF](#why-simpwf)
- [Features](#features)
- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Quickstart](#quickstart)
- [Your first workflow](#your-first-workflow)
- [Node types](#node-types)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Status notifications](#status-notifications)
- [Development](#development)
- [Project layout](#project-layout)
- [Further reading](#further-reading)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

## Why SimpWF

- **Durable by default.** Every state transition is checkpointed in
  PostgreSQL with lease plus revision fencing, so crashed workers never
  corrupt a run. Recovery rules per node type decide between retry and fail.
- **Definitions are immutable.** New versions link to the previous one
  (`previous_version_id`, `lineage_id`). Running instances always execute
  the exact definition they started with.
- **Postgres first, brokers optional.** The engine needs only PostgreSQL.
  Redis pub/sub and RabbitMQ durable queues plug in for event-driven input,
  output publishing, and status fan-out, and the app still starts
  HTTP-only when both DSNs are empty.
- **Debuggable.** Full `context_before`/`context_after` snapshots per
  attempt, an append-only event log, per-occurrence node debug, and
  rollback of paused or failed instances to a prior occurrence.

## Features

- Immutable node and workflow definitions with linear versioning and graph
  validation.
- Node types: `script` (Goja ES5.1 sandbox, no `eval`), `conditions`
  (exactly-one-match routing), `input` (HTTP webhook, Redis pub/sub, or
  RabbitMQ queue with validation script and `Idempotency-Key` dedupe),
  `output` (publish a context value to Redis or RabbitMQ, returns a
  receipt), nested `group`, `external_call` (outbound HTTP or allowlisted
  command), and `poller` (active wait over HTTP, Redis, or RabbitMQ until
  an `until` predicate matches).
- Lifecycle hooks: optional `pre_script`/`post_script` context transforms
  on every node type, run in the same sandbox.
- Failure routing: `external_call` and `poller` nodes can route failures
  to a fallback node via `on_failure` instead of failing the instance.
- Controls: `pause` (immediate, or deferred after the current node),
  `resume`, terminal `stop` (fences workers, cancels in-flight execution),
  and `rollback` to a prior node occurrence (always lands paused).
- Status notifications: per-definition `status_update` webhooks and broker
  messages, delivered from a PostgreSQL transactional outbox, ordered per
  instance and per transport, at-least-once with shared idempotency keys.
- Auth: optional `X-Api-Token` protection for all `/v1` endpoints
  (`/health/*` stays public); Swagger UI at `/swagger/index.html`.

## How it works

```mermaid
flowchart LR
    API[HTTP API] --> DB[(PostgreSQL)]
    DISP[Dispatch loop] -->|ClaimNext FOR UPDATE SKIP LOCKED| DB
    DISP --> ENG[Engine: one node transition per claim]
    ENG --> EX[Executors: script / conditions / input / output / HTTP / command / poller]
    ENG -->|fenced checkpoint + outbox| DB
    HB[Heartbeat] -->|renew leases, poll stops| DB
```

1. A dispatcher claims a runnable instance and leases it to a worker.
2. The engine runs exactly one node transition, then checkpoints the new
   cursor (status `waiting`, `paused`, `finished`, or `failed`).
3. Checkpoints are fenced writes (`WHERE id AND revision AND leased_by`).
   A stale worker loses and aborts silently; a stop always wins.
4. Expired leases trigger recovery: scripts requeue, input nodes re-park,
   other node types retry only with `retry_on_recovery=true` (pollers
   default it to true).
5. Status changes enqueue outbox rows in the same transaction; a dedicated
   dispatcher delivers them per transport, in order, with retries.

See [ARCHITECTURE.md](ARCHITECTURE.md) for package boundaries, the full
state machines, and recovery semantics.

## Requirements

- Go 1.26+
- Docker and Docker Compose (fastest path, or a local PostgreSQL 16)
- [Atlas CLI](https://atlasgo.io/) (schema migrations; the app never migrates)
- `curl` and `jq` (for `scripts/seed.sh` and `scripts/e2e.sh`)
- Optional: Redis 7, RabbitMQ 4 (only for broker transports)

## Quickstart

### Option A: Docker Compose (fastest)

Starts PostgreSQL, Redis, RabbitMQ, migrations, and the app with all
transports enabled.

```bash
docker compose up --build
curl http://localhost:8080/health/ready
```

- App: `http://localhost:8080` (PostgreSQL `9921`, Redis `6379`,
  RabbitMQ `5672`, management UI `http://localhost:15672` with
  `simpwf` / `simpwf`).
- Swagger UI: `http://localhost:8080/swagger/index.html`.
- Auth is disabled in the compose defaults. To require tokens, set
  `SIMPWF_AUTH_ENABLED=true` and pass `X-Api-Token: wadidaw` on `/v1` calls.
- Broker-free mode: unset `SIMPWF_INFRA_REDIS_DSN` and
  `SIMPWF_INFRA_RABBITMQ_DSN` on the `app` service.

### Option B: Local Go

```bash
# PostgreSQL (matches config.yaml)
docker run -d --name simpwf-pg -p 9921:5432 \
  -e POSTGRES_USER=gorm -e POSTGRES_PASSWORD=gorm -e POSTGRES_DB=gorm \
  postgres:16-alpine

# Atlas owns the schema; the app never migrates
atlas migrate apply --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"

# Run the API + dispatcher
go run ./cmd/app -config config.yaml
# -> http://localhost:9999 (health: /health/live, /health/ready)
```

Without a config file, the same keys are read from `SIMPWF_*` environment
variables (see [.env.example](.env.example)).

## Your first workflow

The annotated sample definition is [workflow.yaml](workflow.yaml).
`scripts/seed.sh` creates reusable node definitions plus a workflow from
them and prints their ids (defaults to the compose URL):

```bash
bash scripts/seed.sh http://localhost:8080
# -> {"node_definition_ids": {...}, "workflow_definition_id": "<WF_ID>"}

# Start an instance (202 Accepted)
curl -s -X POST http://localhost:8080/v1/workflow/instance \
  -H 'Content-Type: application/json' \
  -d '{"workflow_definition_id":"<WF_ID>","context":{"user":{"name":"Jono","title":"Manager"}}}'

# Poll status until it parks on input
curl -s http://localhost:8080/v1/workflow/instance/<INSTANCE_ID>/status

# Deliver input (202 Accepted; Idempotency-Key required)
curl -s -X PUT http://localhost:8080/v1/workflow/instance/<INSTANCE_ID>/input \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: first-try' \
  -d '{"title":"Approved"}'

# Inspect results
curl -s http://localhost:8080/v1/workflow/instance/<INSTANCE_ID>/status
curl -s http://localhost:8080/v1/workflow/instance/<INSTANCE_ID>/context
```

`scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]` runs black-box API checks
against a running app (pass an explicit workflow JSON file).

## Node types

| Type          | Does                                                                             | Details                           |
| ------------- | -------------------------------------------------------------------------------- | --------------------------------- |
| `script`        | Goja ES5.1 transform, return value written to `output_property`                    | [workflow.yaml](workflow.yaml)     |
| `conditions`    | Evaluates all conditions, routes on the single match (0 or 2+ fail)              | [ARCHITECTURE.md](ARCHITECTURE.md) |
| `input`         | Parks with `waiting_reason: input` until a payload arrives                         | [API reference](#api-reference)    |
| `output`        | Publishes `context_path` JSON to Redis or RabbitMQ, returns receipt                | [workflow.yaml](workflow.yaml)     |
| `group`         | Nested sub-graph with its own key routing and hooks                              | [ARCHITECTURE.md](ARCHITECTURE.md) |
| `external_call` | Outbound HTTP (allowlisted) or allowlisted command, optional `on_failure` fallback | [Configuration](#configuration)    |
| `poller`        | Repeats HTTP/Redis/RabbitMQ reads until `until` returns `true`                       | [ARCHITECTURE.md](ARCHITECTURE.md) |

Every node type accepts `pre_script`/`post_script` hooks. Hook return
values are ignored; only context mutations persist. See
[ARCHITECTURE.md](ARCHITECTURE.md) for hook ordering, poller transports
and defaults, and `on_failure` payload shape.

## Configuration

`config.yaml` holds infra, worker pool, engine limits, auth, and the
system audit user. Key settings:

| Key                                            | Default             | Notes                                                                                                   |
| ---------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------- |
| `infra.http.host`                                | `localhost:9999`      | Compose overrides to `0.0.0.0:8080`                                                                       |
| `infra.http.swagger_enabled`                     | `true`                | Serves UI at `/swagger/index.html`                                                                        |
| `infra.postgresql.dsn`                           | local `gorm` DSN      | pgx/postgres wire format                                                                                |
| `infra.redis.dsn`                                | `""` (disabled)       | e.g. `redis://localhost:6379/0`; unreachable broker fails startup                                         |
| `infra.rabbitmq.dsn`                             | `""` (disabled)       | e.g. `amqp://simpwf:simpwf@localhost:5672/`; queues default to `simpwf.input`, `simpwf.output`, `simpwf.status` |
| `engine.default_node_timeout` / `max_node_timeout` | `30s` / `5m`            | Caps script, `external_call`, and `output` nodes                                                            |
| `engine.condition_timeout`                       | `5s`                  | Fixed budget for conditions, input validation, poller predicates                                        |
| `engine.http_allowlist`                          | loopback + examples | `"*"` allows any target (development only, logs a warning)                                                |
| `engine.exec_allowlist`                          | `echo`, `ls`            | Direct argv only, never a shell                                                                         |
| `auth.enabled` / `api_token`                       | `false`               | When enabled, `/v1` requires `X-Api-Token`                                                                  |

List-valued keys accept comma-separated env values, e.g.
`SIMPWF_ENGINE_HTTP_ALLOWLIST="api.example.com,jsonplaceholder.typicode.com"`.

## API reference

The authoritative contract is [api/openapi.yaml](api/openapi.yaml)
(Swagger UI served from the app; regenerate committed `docs/` with
`task swagger` after annotation changes).

| Method     | Path                                             | Purpose                                                                  |
| ---------- | ------------------------------------------------ | ------------------------------------------------------------------------ |
| GET        | `/health/live`, `/health/ready`                      | Liveness / readiness (always public)                                     |
| POST       | `/v1/node/definition`                              | Create immutable node definition (201)                                   |
| GET        | `/v1/node/definition`                              | List (paged, `latest_only`, `type`, ...)                                     |
| GET/DELETE | `/v1/node/definition/{id}`                         | Get / delete (fails if referenced)                                       |
| POST       | `/v1/workflow/definition`                          | Create immutable workflow definition (201)                               |
| GET        | `/v1/workflow/definition`                          | List (paged, `latest_only`, ...)                                           |
| GET/DELETE | `/v1/workflow/definition/{id}`                     | Get / delete (fails if referenced)                                       |
| POST       | `/v1/workflow/instance`                            | Start instance (202)                                                     |
| GET        | `/v1/workflow/instance`                            | List compact summaries (paged, `id`/`workflow_definition_id`/`status` filters) |
| GET        | `/v1/workflow/instance/{id}/status`                | Status, counters, cursor, per-node `nodes` map, audit actors               |
| GET/PUT    | `/v1/workflow/instance/{id}/context`               | Get full context / replace it (paused only, 409 on race)                 |
| GET        | `/v1/workflow/instance/{id}/status/node/{node_id}` | Node debug (`?attempt=N`; `not_started` for never-run nodes)                 |
| PUT        | `/v1/workflow/instance/{id}/input`                 | Deliver input (`Idempotency-Key` required, 202)                            |
| POST       | `/v1/workflow/instance/{id}/pause`                 | Pause (200 immediate / 202 deferred)                                     |
| POST       | `/v1/workflow/instance/{id}/resume`                | Resume                                                                   |
| POST       | `/v1/workflow/instance/{id}/stop`                  | Terminal stop (fences workers, cancels in-flight execution)              |
| POST       | `/v1/workflow/instance/{id}/rollback`              | Roll back paused/failed instance to a prior occurrence (lands paused)    |

Instance statuses: `waiting`, `running`, `paused`, `finished`, `failed`,
`stopped`. Errors follow RFC 7807 `problem+json`.

## Status notifications

A definition opts in with a top-level `status_update` block (any
combination of transports, each with its own retry policy):

```yaml
status_update:
  http:
    url: "https://example.com/workflow-status"
    max_retry: 3
    retry_delay: "5s"
  redis:
    max_retry: 2
  rabbitmq:
    max_retry: 1
```

Covered transitions: `waiting_for_input`, `input_received`, `paused`,
`resumed`, `finished`, `failed`, `stopped`. Each event fans out to one
outbox row per configured transport sharing one logical event id, delivered
in per-instance/per-transport order, at-least-once. Receivers dedupe on the
id (`Idempotency-Key` / `X-SimpWF-Event-ID` on HTTP, AMQP `message_id` on
RabbitMQ, embedded `id` on Redis). A transport configured without its
broker DSN dead-letters. Full payload and ordering guarantees are in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Development

```bash
task test        # full suite (needs PostgreSQL on :9921 + scratch DBs, see [Taskfile.yaml](Taskfile.yaml))
task test-race   # full suite under the race detector
task lint        # golangci-lint
task vet         # go vet
task e2e         # black-box API checks against a running app
task swagger     # regenerate docs/ from API annotations
task migrate-up      # atlas migrate apply
task migrate-diff    # new migration from GORM models
task migrate-lint    # validate migration directory
```

Validation before opening a PR:

```bash
gofmt -l .
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
golangci-lint run ./...
atlas migrate validate --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"
```

CI (`.github/workflows/`) runs gofmt, vet, tests, race tests, lint, and
Atlas validate plus apply on pushes to `main`/`master` and on pull requests. A
coverage workflow tracks a 75% floor, and the badge at the top of this
file is refreshed automatically; leave that badge line in place.

## Project layout

```
cmd/app                    composition root (config, repos, engine, dispatcher, router)
cmd/atlas-loader           Atlas schema loader (app never migrates)
internal/workflow/model       domain entities, statuses, state machine invariants
internal/workflow/repository  GORM models, fenced claims, checkpoints, outbox
internal/workflow/executor    Goja sandbox, node executors, lifecycle hooks
internal/workflow/engine      cursor machine, dispatcher, cancellation registry
internal/workflow/transport   optional Redis/RabbitMQ adapters
internal/workflow/inputtransport  broker input consumers
internal/workflow/statusupdate    outbox dispatcher + status publishers
internal/workflow/service     use-case orchestration (instances, controls, debug)
internal/workflow/handler     Gin routes, DTOs, problem+json
pkg/*                      config, database, ids, context paths (no internal imports)
api/openapi.yaml           authoritative API contract
docs/                      generated Swagger docs (via task swagger)
migrations/                Atlas config + versioned SQL
workflow.yaml              annotated sample workflow definition
scripts/                   seed.sh (sample seed) + e2e.sh (black-box checks)
```

## Further reading

- [ARCHITECTURE.md](ARCHITECTURE.md): package boundaries, runtime model,
  state machines, recovery, hooks, failure routing, notifications.
- [api/openapi.yaml](api/openapi.yaml): authoritative REST contract.
- [workflow.yaml](workflow.yaml): annotated sample definition covering all
  node types.
- [.env.example](.env.example): every setting as environment variables.
- [docker-compose.yml](docker-compose.yml): full local stack wiring.

## Contributing

1. Fork the repo and create a feature branch from `main`.
2. Keep changes surgical: touch only what the task needs, match existing
   style, and add tests for new behavior.
3. Run the validation block above (`gofmt`, `vet`, tests, `lint`; Atlas
   validate if migrations changed; `task swagger` if API annotations
   changed).
4. Open a pull request against `main` describing what changed and how it
   was verified. CI must pass.

Good first checks on any PR: `gofmt -l .` prints nothing, `go vet ./...`
is clean, and `task test` passes.

## Security

- Never use `engine.http_allowlist: ["*"]` outside local development; every
  request and redirect is validated against the allowlist (scheme,
  host:port, DNS).
- Keep `engine.exec_allowlist` minimal. Commands run directly (no shell)
  and are killed as a process group on timeout or cancel, but each entry
  is still arbitrary code execution by design.
- Enable `auth.enabled` and set a strong `api_token` for any shared or
  production deployment. Treat tokens like passwords.
- Found a vulnerability? Please use GitHub's private vulnerability
  reporting on this repository instead of opening a public issue, so a fix
  can land before disclosure.

## License

MIT. See [LICENSE](LICENSE).

Copyright (c) 2026 didasy.
