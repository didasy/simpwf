# s3fetch — example custom node

Bundled worked example: pipes a URL into S3-compatible storage (`pipe`)
or presigns an existing key (`presign`). Real deployments add own leaf
packages under `pkg/customnode/<name>`; copy this package as template.

## Local RustFS

`docker-compose.yml` runs RustFS (`rustfs/rustfs`, plain HTTP) with dev
credentials `rustfsadmin`/`rustfsadmin`: S3 API on `localhost:9000`,
console on `localhost:9001`. The `app` service sets
`SIMPWF_S3_ENDPOINT=rustfs:9000` plus the same credentials, so instance
contexts carry them under `{{ env.SIMPWF_S3_* }}`. Local runs use
`"use_ssl": false` (RustFS is plain HTTP here); buckets must exist before
`pipe` (create once in console or via `mc mb`).

## Workflow JSON

```json
{
  "id": "<uuid>",
  "type": "s3fetch",
  "name": "fetch-report",
  "config": {
    "operation": "pipe",
    "endpoint": "{{ env.SIMPWF_S3_ENDPOINT }}",
    "region": "us-east-1",
    "bucket": "reports",
    "key": "daily/{{ run_id }}.pdf",
    "source_url": "https://example.com/report.pdf",
    "expiry_seconds": 3600,
    "use_ssl": true,
    "access_key": "{{ env.SIMPWF_S3_ACCESS_KEY }}",
    "secret_key": "{{ env.SIMPWF_S3_SECRET_KEY }}"
  },
  "timeout": "120s",
  "output_property": "report",
  "next_node": "<uuid>"
}
```

Presign (no download, key must exist):

```json
{
  "id": "<uuid>",
  "type": "s3fetch",
  "name": "share-report",
  "config": {
    "operation": "presign",
    "endpoint": "{{ env.SIMPWF_S3_ENDPOINT }}",
    "bucket": "reports",
    "key": "daily/abc.pdf",
    "expiry_seconds": 3600,
    "access_key": "{{ env.SIMPWF_S3_ACCESS_KEY }}",
    "secret_key": "{{ env.SIMPWF_S3_SECRET_KEY }}"
  },
  "timeout": "30s",
  "output_property": "report"
}
```

Only `type` + `config` custom. All other keys standard common fields,
parsed/validated by core (timeout cap, hooks, `on_failure`,
`retry_on_recovery`, `output_property`, `next_node`).

## Config schema

- `operation` `pipe`|`presign`, required.
- `endpoint` bare `host:port`, required, templated. `http(s)://` prefix
  tolerated (stripped at validate).
- `region` string, defaults `us-east-1`.
- `bucket`, `key` strings, required, templated.
- `source_url` string, templated. Required for `pipe`, forbidden for
  `presign`.
- `expiry_seconds` int, defaults `3600`. Range `1..604800` (S3 presign
  ceiling is 7 days). `0`/omitted = default.
- `use_ssl` bool, defaults `true`.
- `access_key`, `secret_key` strings, required, templated — e.g.
  `{{ env.SIMPWF_S3_ACCESS_KEY }}`.

Bad config fails at parse (definition + workflow create time, never
runtime): `node type "s3fetch": invalid config: <reason>`. Presence
checks only — template shapes like `{{ env.* }}` always pass validate
and resolve at runtime. Missing `env` key at runtime = render error.

## Executor

`Execute` type-asserts validated form (`req.Node.Custom.(Config)`),
renders each field via `contextpath.RenderTemplate` against
`req.Context`, then:

- `pipe`: downloads `source_url` with injected shared `Deps.HTTP`
  (allowlist applies to `source_url`; output cap applies; oversize
  download fails), `PutObject`s bytes to `bucket/key`,
  `PresignedGetObject`s the result. Output
  `{"bucket","key","url","expires_at" (UTC RFC3339),"size"}`.
- `presign`: `StatObject`s first (fail fast on missing key), then
  presigns. Output `{"bucket","key","url","expires_at"}` (no `size`).

Never returns file bytes — only the small object. S3 errors map to
`NodeError`; with `on_failure` set, partial `{"bucket","key"}` output
returned alongside error so `routeFailure` stores it (same contract as
builtin http).

## Timeouts and size

Uploads often exceed the 30s default: set `timeout: "120s"` (or higher)
on `pipe` nodes. Downloads count against `MaxOutputBytes` — small files
only (reports, thumbnails, CSVs), not multi-GB objects.

## Trusted-author env note

Only `SIMPWF_*` process env is visible to workflows, snapshotted at
instance creation under the reserved `env` root (`{{ env.SIMPWF_X }}`).
Workflow authors who can set config templates can exfiltrate those
values via outputs — treat workflow authoring as trusted, same as
script nodes that can already read full context.

## Registration

`init()` calls `customnode.MustRegister`. App pulls it via blank import
in `pkg/customnode/all/all.go`, itself blank-imported once in
`cmd/app/main.go`. New node = new subpackage + one line in `all/all.go`.
