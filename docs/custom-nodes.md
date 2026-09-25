# Custom Nodes — authoring guide

One-time engine change; new node = new subpackage under
`pkg/customnode/<name>` + one blank-import line. Zero core edits.

## Contract

Workflow JSON: `{"type":"<name>","config":{...}}` plus standard common
fields (`id`, `name`, `timeout`, `input_data`, `output_property`,
`next_node`, `on_failure`, `retry_on_recovery`, `pre_script`,
`post_script`, `metadata`). Only `type`+`config` custom.

Leaf package (`pkg/customnode/<name>/<name>.go`):

```go
package <name>

func init() {
    customnode.MustRegister(customnode.Definition{
        Type:     "<name>",
        Validate: ValidateConfig, // func(json.RawMessage) (any, error)
        New: func(d customnode.Deps) (executor.Executor, error) {
            return &Executor{...}, nil
        },
    })
}

func (e *Executor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
    cfg := req.Node.Custom.(Config) // validated form
    return &executor.Result{Output: ...}, nil
}
```

`customnode.Deps = { Limits executor.Limits; Logger *logrus.Logger;
HTTP *executor.HTTPExecutor; Funcs *jsfunc.Registry }` — same power as
builtin executors.

Bundle step: append `_ "…/pkg/customnode/<name>"` to
`pkg/customnode/all/all.go` (already blank-imported once in
`cmd/app/main.go`).

## Naming

`^[a-z][a-z0-9_]*$`, max 64 chars, no builtin collision (`script`,
`conditions`, `input`, `group`, `external_call`, `output`, `poller`).
Violations fail at registration (startup panic via `MustRegister`).

## Inheritance (no extra code)

Custom types ride generic `engine.executeStep` path: pre hook →
`executors[nc.Type].Execute` → `output_property` merge → post hook →
cursor advance → `on_failure→routeFailure` on error →
`recover→RetryOnRecovery`. Counters, recovery, lean-context diff, debug
step-through all apply. Group/input special-casing untouched.

`on_failure` allowed for `external_call`, `poller`, and any registered
custom type. Other builtins reject it at parse.

## Validation errors

- Unknown/unimported type → `node type %q is not supported`. Unimported
  custom = same error; fix = blank import.
- Missing `config` → `node type %q: config is required`.
- Bad `config` → `node type %q: invalid config: <node error>`, at
  definition/workflow parse time, never runtime.
- Builtin executable field on custom node (`script`, `http_config`,
  `http`, `nodes`, …) → `does not support that field; custom nodes
  carry only type and config`.
- Duplicate registration / builtin collision → startup panic naming
  type. Facade rolls back model entry on executor conflict so no
  half-registered type survives.
- Factory error at `NewExecutors` → startup panic naming type (fail
  fast, never per-request).

## Timeouts

`timeout` parsed with standard default + cap (`nodeTimeout`). Omitted =
`DefaultTimeout`.

## `{{ env.* }}` — allowlisted env in templates

At instance creation, `Create` snapshots process env vars starting with
`SIMPWF_` into the instance context under the reserved `env` root, so
configs address them as `{{ env.SIMPWF_X }}`:

- Only `SIMPWF_*` is visible. Anything else (`NOT_MINE`, `PATH`, …) never
  enters the context.
- The snapshot is per-instance, taken once at creation. Later process-env
  changes do not affect running instances.
- Missing key = render error (`contextpath: path not found`), failing the
  node like any other unresolvable template.
- `env` is reserved: a create body cannot shadow snapshot values
  (snapshot wins per key), and full-replacement `UpdateContext` carries
  the stored snapshot forward (stored wins per key). Old instances created
  before this feature have an empty `env` map; re-create to get values.
- Trusted-author boundary: workflow authors who can write config
  templates can exfiltrate `env` values via node outputs — same trust
  level as script nodes, which already read full context.

Worked examples: [`pkg/customnode/s3fetch/README.md`](../pkg/customnode/s3fetch/README.md)
(the `s3fetch` leaf — `pipe` a URL into S3, or `presign` an existing key);
[`pkg/customnode/jev/README.md`](../pkg/customnode/jev/README.md)
(the `jev` leaf — Jev 1.13 Decisions API, raw probs, downstream gating).

## Local test loop

1. Add leaf package + `all.go` line.
2. `rtk go test ./pkg/customnode/... ./internal/workflow/model/ ./internal/workflow/executor/`.
3. Parse-level: `model.ParseNodeContent` with registered type; check
   `Custom` value, `CustomConfig` bytes, common fields.
4. Engine-level: register stub factory + validator in test (see
   `internal/workflow/engine/engine_custom_test.go`), drive workflow,
   assert cursor/output/hooks/`on_failure`/`retry_on_recovery`.
5. `rtk go vet`, `rtk golangci-lint` if configured.
