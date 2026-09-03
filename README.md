# SimpWF — PostgreSQL-backed durable workflow engine
![Coverage](https://img.shields.io/badge/Coverage-78.3%25-brightgreen)

SimpWF is a Go workflow engine with **immutable workflow definitions**, a
**leased state machine** over PostgreSQL, and **HTTP input/control/debug
APIs**. `FOR UPDATE SKIP LOCKED` claims, leases, and heartbeats are the
durable dispatch mechanism. Optional **Redis (pub/sub)** and **RabbitMQ
(durable queues)** transports add broker input nodes, broker output nodes,
and multi-transport status notifications; both stay disabled when their DSN
is absent. Nodes run in a Goja sandbox (scripts/conditions/validation), as
outbound HTTP calls, as allowlisted subprocesses, or as broker publishers.

## Features

- Immutable node and workflow definitions with linear versioning
  (`previous_version_id`, `lineage_id`), recursive materialization, and graph
  validation.
- Node types: `script` (Goja ES5.1 sandbox, no eval), `conditions`
  (at least two conditions; all are evaluated and exactly one must match, zero
  or multiple matches fail the workflow; workflow/group-scoped key routing),
  `input` (HTTP webhook, Redis pub/sub, or RabbitMQ queue with validation
  script and `Idempotency-Key` dedupe), `output` (publish a selected
  `context_path` to Redis or RabbitMQ and return a receipt), nested `group`,
  `external_call` (outbound HTTP or allowlisted command), and `poller`
  (active waits: repeated HTTP, Redis GET, Redis pub/sub, or a RabbitMQ
  queue, until an `until` predicate matches).
- Durable cursor (`frame`), per-node/total execution counters, full
  `context_before`/`context_after` snapshots, and an append-only event log.
- Worker claims via `SELECT ... FOR UPDATE SKIP LOCKED`, fenced checkpoints
  (lease + revision), heartbeat renewal, and recovery: scripts requeue (new
  attempt) and input nodes re-enter waiting; `conditions`, `external_call`,
  `output`, and `poller` nodes requeue only with `retry_on_recovery=true` (pollers
  default it to true), otherwise the node and workflow fail.
- Controls: `pause` (immediate or deferred after the current node), `resume`,
  and `stop` (terminal, fences workers, cancels the in-flight Goja/HTTP/
  command execution locally and across replicas through the heartbeat).
- Node debug: latest/exact loop attempts, running snapshots, and
  `not_started` views per occurrence.
- Status notifications: per-definition `status_update` webhooks delivered
  from a PostgreSQL transactional outbox, strictly ordered per workflow
  instance and per transport (see "Status notifications").

## Quickstart

### Local (Go 1.26+)

```bash
# PostgreSQL (dev container, matches config.yaml)
docker run -d --name simpwf-pg -p 9921:5432 \
  -e POSTGRES_USER=gorm -e POSTGRES_PASSWORD=gorm -e POSTGRES_DB=gorm \
  postgres:16-alpine

# Migrate (Atlas owns the schema; the app never migrates)
atlas migrate apply --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"

# Run the API + dispatcher
go run ./cmd/app -config config.yaml
# -> http://localhost:9999  (health: /health/live, /health/ready)

# Test suite (needs the scratch databases; see Taskfile)
task test
```

### Docker Compose

```bash
docker compose up --build
# db (postgres:16) -> migrate (atlas apply) -> redis/rabbitmq (healthy) -> app
curl http://localhost:8080/health/ready
# Swagger UI: http://localhost:8080/swagger/index.html
# RabbitMQ management UI: http://localhost:15672 (simpwf/simpwf)
```

The stack runs PostgreSQL on `9921`, Redis on `6379`, RabbitMQ on `5672`
(management UI on `15672`), and the app on `8080` with all broker transports
enabled. To run broker-free, remove the `SIMPWF_INFRA_REDIS_DSN` and
`SIMPWF_INFRA_RABBITMQ_DSN` environment variables from the `app` service.
Note the compose `app` service sets `SIMPWF_AUTH_ENABLED=true` (token
`wadidaw`), so `/v1` calls against the compose stack need the
`X-Api-Token: wadidaw` header; `/health/*` stays public.

Swaggo documentation is committed under `docs/`. Regenerate it after API
annotation changes with `task swagger`. Set `infra.http.swagger_enabled` to
`false` to disable the UI and JSON endpoint. When `auth.enabled` is true,
every `/v1` endpoint requires the `X-Api-Token` header; `/health/*` stays
public and Swagger UI sends the header via the Authorize button.

### End-to-end checks

```bash
# with the app running (pass an explicit workflow JSON; workflow.yaml is the
# annotated sample definition):
bash scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]
```

## Configuration

`config.yaml` holds infra, worker pool, engine limits, auth, and the system audit
user. Without a config file the same keys are read from `SIMPWF_*`
environment variables (see `.env.example`). Highlights:

- `engine.default_node_timeout` / `max_node_timeout` — script,
  `external_call`, and `output` node timeouts (default 30s, cap 5m).
- `engine.condition_timeout` — fixed budget for conditions, input
  validation, and poller `until` predicates.
- `engine.http_allowlist` — allowed http(s) targets for `external_call` and
  HTTP pollers (scheme, host:port, and DNS validated on every request and
  redirect). `"*"` allows any target for development and logs a warning.
- `engine.exec_allowlist` — executables allowed for command nodes (direct
  argv, never a shell, process-group kill on timeout/cancel).
- List-valued keys (`engine.http_allowlist`, `engine.exec_allowlist`) accept
  comma-separated values from environment variables, e.g.
  `SIMPWF_ENGINE_HTTP_ALLOWLIST="api.example.com,jsonplaceholder.typicode.com"`.
- `infra.redis.dsn` — optional Redis DSN (e.g. `redis://localhost:6379/0`);
  empty disables the Redis input/output/status transports. A configured but
  unreachable broker fails startup.
- `infra.rabbitmq.dsn` / `input_queue` / `output_queue` / `status_queue` —
  optional RabbitMQ DSN and the three durable queues (defaults
  `simpwf.input`, `simpwf.output`, `simpwf.status`). Empty DSN disables the
  RabbitMQ transports.
- `auth.enabled` / `api_token` — when `enabled` is true, every `/v1` endpoint
  requires the `X-Api-Token` header (value `api_token`); `/health/*` stays
  public. Without a config file, read from `SIMPWF_AUTH_ENABLED` and
  `SIMPWF_API_TOKEN`. Swagger UI exposes the scheme as `ApiKeyAuth`.
- `system.*` — the configured audit actor; used for `created_by`/`updated_by`
  on definitions and workflow instances and for audit events.

## Pollers

A `poller` node actively repeats a request or waits on a broker until its
`until` predicate returns `true`. Unlike `input` nodes, a poller occupies a
dispatcher worker slot for its whole wait and needs no external delivery
through the input API. Exactly one transport block (`http`, `redis`, or
`rabbitmq`) is required, and `until` (inside that block) must return an
actual boolean — truthy/falsy non-booleans, script errors, and timeouts fail
the node immediately and are not transport retries. Predicates run in the
frozen sandbox with the normalized `response` variable and the frozen
workflow `context`.

| Transport | Response shape                                    | Defaults                                            |
| --------- | ------------------------------------------------- | --------------------------------------------------- |
| `http`      | `{ body, headers, status }` (`map<string, string[]>`) | `GET`, `delay 5s`, `request_timeout 30s`, `max_attempts 10` |
| `redis` GET | `{ body }`                                          | `delay 5s`, `request_timeout 30s`, `max_attempts 10`      |
| `redis` SUB | `{ body }`                                          | `max_wait_time 5m`                                    |
| `rabbitmq`  | `{ body, headers }` (`map<string, string>`)           | `max_wait_time 5m`                                    |

Body normalization is consistent: valid JSON becomes its parsed value,
invalid JSON stays a string. A missing Redis key is a normal response with
`response.body === null`. The full response object is the node output under
`output_property` (default: the node attempt id).

- **HTTP**: the first request starts immediately; `delay` applies only
  between attempts; every request counts toward `max_attempts`, including
  transport errors; HTTP status codes are responses evaluated by `until`,
  not errors. The `Idempotency-Key` header is stable across attempts.
  `request_timeout` covers each call and is **not capped** by
  `engine.max_node_timeout`; poller `delay`/`max_wait_time` are likewise
  uncapped.
- **Redis GET**: repeats `GET` on the rendered key under the same attempt
  semantics as HTTP.
- **Redis SUB**: subscribes to the rendered channel and waits up to
  `max_wait_time` for a matching message; nonmatching messages are
  discarded. Pub/sub is broadcast: multiple active pollers may accept the
  same message, and messages published while nobody is subscribed are lost.
- **RabbitMQ**: consumes the rendered queue and waits up to `max_wait_time`
  for a matching message. The queue must already exist (the engine never
  declares it) and must be exclusive to one active poller execution. Every
  consumed message is acknowledged: false messages are discarded, the first
  match completes the node, and predicate/normalization errors acknowledge
  the message before failing the node.

Poller configuration templates may also use the reserved automatic roots
`workflow_instance_id` and `node_instance_id` (read-only; they always win
over same-named user context values and are never persisted into the
workflow context). Interrupted pollers default to `retry_on_recovery: true`,
so a worker recovery restarts the attempt with a fresh internal budget.
Redis and RabbitMQ pollers require the matching broker DSN; without it those
nodes fail with a missing-transport error and the app stays HTTP-only.

## Lifecycle hooks

Every node type (`script`, `conditions`, `input`, `group`, `external_call`,
`output`, `poller`) accepts optional `pre_script` and `post_script`
context-transform hooks. They run in the same Goja sandbox as node scripts
(no `eval`, registered `go` functions available, hard timeout) and replace
adjacent script nodes that only prepare context before a node or tidy it up
afterwards:

```yaml
- type: external_call
  name: "Notify"
  pre_script:
    script: |
      context.request_id = context.request_id || 'new-request';
    timeout: 5s
  post_script:
    script: |
      context.notified_at = new Date().toISOString();
  http_config:
    url: "https://api.example.com/webhook/{{ request_id }}"
```

- `script` is required when the hook object is present; `timeout` is optional
  and defaults to `engine.default_node_timeout`, capped by
  `engine.max_node_timeout` like normal node scripts.
- Hook return values are always ignored. Hooks produce no node output; only
  exported context mutations persist. A `post_script` additionally receives
  the native node output as a frozen `output` global for convenient reads
  (for `input` it is the accepted parsed payload, for `conditions` it is
  `{ matched, index, key }`; groups receive no output).
- `pre_script` runs before the node's own behavior, including `input_data`
  resolution and template rendering. `post_script` runs after the native
  output was merged into context and before the cursor advances.
- `group`: the pre hook runs before the first child; the post hook runs after
  the last child finished, innermost group first. A failing group hook fails
  the workflow (groups have no node attempt) and preserves the latest
  completed context.
- `input`: the pre hook runs once before parking and its transformed context
  is checkpointed; the post hook runs once per accepted delivery (rejected
  deliveries run neither hook again). An accepted delivery whose `post_script`
  fails is still accepted (the API returns 202) but the workflow fails with
  the payload preserved.
- A hook error or timeout fails the node and the workflow, with
  `pre-script` / `post-script` in the error message.
- Handled executor failures on nodes configured with `on_failure` skip `post_script`.
- Reusable node definitions supply hook defaults; each workflow occurrence
  may override either hook with an object, disable one with an explicit
  `null`, or inherit it by omitting the key.

## Failure routing

`external_call` and `poller` nodes can route execution failures to a configured
fallback node (e.g. an `input` node for manual intervention) without failing
the workflow instance:

```json
"on_failure": {
  "next_node": "backup-node-uuid",
  "output_property": "external_error"
}
```

- Allowed only on `external_call` and `poller` nodes. Both `next_node` (a valid
  UUID resolving in the same workflow/group scope) and `output_property` are required.
- The structured failure payload is saved at `output_property` in the workflow context:
  - `message`: human-readable error description.
  - `reason`: category (e.g. `http`, `http-status`, `command`, `poller`, `poller-until`, `recovery`).
  - `result`: native normalized result when available (e.g. `{ Status, Headers, Body }`), otherwise `null`.
- **External HTTP**: when `on_failure` is configured, HTTP status `>= 300` is treated as a handled failure and routes to `next_node` with reason `http-status` (the partial result is also stored in the `result` field). Without `on_failure`, status `>= 300` remains a normal successful output and runs `post_script`.
- **Pollers**: repeated attempts and until predicate evaluations proceed normally; failure routing only occurs after attempts are exhausted or upon unrecoverable error.
- **Recovery**: interrupted nodes with `retry_on_recovery: false` route to `on_failure` with reason `recovery` and null result.
- **Lifecycle & Events**: handled failures mark the node attempt `failed`, skip the node's `post_script`, emit `node_failed` and `node_failure_routed` events, keep the workflow error empty, and advance the workflow cursor to the fallback node without emitting `workflow_failed`. Pre-hook, `input_data`, post-hook, graph, persistence, and internal engine failures do not route via `on_failure`.

## Status notifications

A workflow definition can publish notifications for externally meaningful
instance state changes by adding a top-level `status_update` block to its
content. Any combination of transports may be configured:

```yaml
status_update:
  http:
    url: "https://example.com/workflow-status"
    method: "POST"          # optional, default POST
    headers:
      Authorization: "Bearer token"
    max_retry: 3            # optional, retries after the initial attempt
    retry_delay: "5s"       # optional, fixed delay between attempts
  redis:
    max_retry: 2            # published to workflow:status:<instance_id>
    retry_delay: "2s"
  rabbitmq:
    max_retry: 1            # published to the configured status_queue
    retry_delay: "10s"
```

Every relevant change — `waiting_for_input`, `input_received`, `paused`,
`resumed`, `finished`, `failed`, `stopped` — is enqueued in a PostgreSQL
outbox in the same transaction as the state transition, with **one outbox
row per configured transport** and **one shared logical event id** across
all transports of the event. Delivery is strictly in order per workflow
instance **and per transport**: a transport's later events never overtake
its earlier ones, and each transport retries and dead-letters
independently. Internal scheduler churn (`waiting <-> running`) and
pending-pause flag changes emit nothing.

Delivery is at-least-once. Each payload carries a stable `id` (the logical
event id) echoed as `Idempotency-Key` / `X-SimpWF-Event-ID` on HTTP, as the
AMQP `message_id` and `IdempotencyKey` header on RabbitMQ, and embedded in
the Redis payload (`id`), so receivers can dedupe across transports. The payload
also carries `type` (`workflow.status_changed`), `workflow_instance_id`, `workflow_definition_id`, `event`,
`from_status`, `to_status`, `from_waiting_reason`, `to_waiting_reason`,
`revision`, `occurred_at`, and `error` (no workflow context).

- **HTTP**: a non-2xx response or request error is retried up to
  `http.max_retry` times with `http.retry_delay` between attempts, then
  dead-lettered. Targets obey `engine.http_allowlist` (scheme, host:port,
  DNS, and redirect revalidation).
- **Redis**: the event JSON is published to `workflow:status:<instance_id>`.
  A successful publish counts as delivered even with zero subscribers
  (best-effort).
- **RabbitMQ**: the event JSON is published as a persistent,
  publisher-confirmed message to the configured `status_queue`, with
  `NodeInstanceId` (the workflow instance id) and `IdempotencyKey` headers
  and the logical id as the AMQP `message_id`.

Transports are enabled only when the matching broker DSN is configured; a
`status_update` block for a disabled broker is delivered to no one and
dead-letters (the definition configures a transport the deployment lacks).

## API entry points

| Method     | Path                                             | Purpose                                                         |
| ---------- | ------------------------------------------------ | --------------------------------------------------------------- |
| GET        | `/health/live`, `/health/ready`                      | liveness / readiness                                            |
| POST       | `/v1/node/definition`                              | create node definition (immutable)                              |
| GET        | `/v1/node/definition`                              | list (paged, `latest_only`, `type`, ...)                            |
| GET/DELETE | `/v1/node/definition/{id}`                         | get / delete                                                    |
| POST       | `/v1/workflow/definition`                          | create workflow definition (immutable)                          |
| GET        | `/v1/workflow/definition`                          | list (paged, `latest_only`, ...)                                  |
| GET/DELETE | `/v1/workflow/definition/{id}`                     | get / delete                                                    |
| POST       | `/v1/workflow/instance`                            | create instance (202)                                           |
| GET        | `/v1/workflow/instance`                            | list (paged, `id`/`workflow_definition_id`/`status` filters)          |
| GET        | `/v1/workflow/instance/{id}/status`                | status + counters + cursor + audit actors                       |
| GET        | `/v1/workflow/instance/{id}/context`               | full context JSON                                               |
| PUT        | `/v1/workflow/instance/{id}/context`               | replace context (paused only, optional `X-Context-Update-Reason`) |
| GET        | `/v1/workflow/instance/{id}/status/node/{node_id}` | node debug (`?attempt=N`)                                         |
| PUT        | `/v1/workflow/instance/{id}/input`                 | deliver webhook input (`Idempotency-Key`)                         |
| POST       | `/v1/workflow/instance/{id}/pause`                 | pause (200 immediate / 202 deferred)                            |
| POST       | `/v1/workflow/instance/{id}/resume`                | resume                                                          |
| POST       | `/v1/workflow/instance/{id}/stop`                  | force stop                                                      |

See `api/openapi.yaml` for the authoritative contract, `workflow.yaml` for an
annotated sample definition, and `scripts/e2e.sh` for black-box API checks
(`bash scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]`).

## Validation

```bash
gofmt -l .                     # no output
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
golangci-lint run ./...
atlas migrate validate --config file://migrations/atlas.hcl --env gorm \
  --var dev_url="postgres://gorm:gorm@localhost:9921/gorm?sslmode=disable"
```

## Layout

```
cmd/app            composition root (config, repos, services, engine, dispatcher, broker clients, consumers, router)
cmd/atlas-loader   Atlas Go Program Mode schema loader (never AutoMigrate in prod)
internal/workflow/model       domain entities, statuses, state machine invariants
internal/workflow/repository  GORM models + fenced claim/checkpoint queries
internal/workflow/executor    Goja sandbox + script/conditions/input/output/HTTP/command/poller executors
internal/workflow/engine      cursor machine, cancellation registry, dispatcher
internal/workflow/transport   optional Redis/RabbitMQ adapters + narrow publisher/poller interfaces
internal/workflow/inputtransport broker input consumers (redis envelope, rabbit queue)
internal/workflow/statusupdate outbox dispatcher + http/redis/rabbitmq status-notification publishers
internal/workflow/service     use-case orchestration (instances, controls, debug)
internal/workflow/handler     Gin routes, DTOs, problem+json
pkg/*              configuration, database, ids, contextpath, jsfunc (no internal imports)
api/openapi.yaml   authoritative OpenAPI 3.1 contract
docs/              generated Swaggo documentation
migrations/        Atlas config + versioned SQL (migrations/versions)
workflow.yaml      annotated sample workflow definition
scripts/           seed.sh (sample seed) + e2e.sh (black-box API checks)
```
