# jev — Decisions API custom node

Calls the Jev 1.13 Decisions API on OpenRouter (`POST
https://openrouter.ai/api/alpha/decisions`) with application state plus
typed questions (`noul`, `choice`, `score`). Returns raw probabilities
verbatim — no thresholds live here; downstream `conditions`/`script`
nodes gate.

## Workflow JSON

```json
{
  "id": "<uuid>",
  "type": "jev",
  "name": "jev-triage",
  "config": {
    "model": "typesafe/jev-1.13",
    "endpoint": "https://openrouter.ai/api/alpha/decisions",
    "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
    "state": "{{ ticket }}",
    "questions": {
      "is_urgent": {
        "type": "noul",
        "instructions": "Does this message convey urgency?",
        "criteria": {"true": "Explicitly time-sensitive", "false": "No urgency expressed"}
      },
      "department": {
        "type": "choice",
        "instructions": "Which team should own this ticket?",
        "criteria": {"billing": "Payments, refunds", "technical": "Bugs, outages"}
      },
      "frustration": {
        "type": "score",
        "instructions": "How frustrated is the customer?",
        "criteria": ["Calm", "Frustrated", "Very angry"]
      }
    }
  },
  "timeout": "60s",
  "output_property": "jev"
}
```

Only `type` + `config` custom. All other keys standard common fields,
parsed/validated by core (timeout cap, hooks, `on_failure`,
`retry_on_recovery`, `output_property`, `next_node`).

## Config reference

- `model` string, defaults `typesafe/jev-1.13`. Non-blank when set.
- `endpoint` string, defaults `https://openrouter.ai/api/alpha/decisions`.
  Must be an absolute `http(s)` URL when set.
- `api_key` string, required, templated — e.g.
  `{{ env.SIMPWF_OPENROUTER_KEY }}`. Verified non-blank after render;
  never logged.
- `state` required, any JSON (`string`|`object`|`array`). Strings render
  with `RenderTemplate`, objects/arrays with `RenderJSON`, so
  `"{{ ticket }}"` resolves typed while `{"ticket": "{{ ticket }}"}` keeps
  structure.
- `questions` required, ≥1 entry; qid non-empty. Each entry:
  - `type` `noul`|`choice`|`score`, required.
  - `instructions` required non-null (`string`|`object`|`array`; strings
    must be non-blank).
  - `noul` `criteria` optional; when present an object whose `true`/`false`
    values, if present, must be non-empty strings.
  - `choice` `criteria` required object, 2..255 entries; keys non-empty;
    values `string` (non-empty) | `null` | `object`.
  - `score` `criteria` required array, 2..10 entries; each `string`
    (non-empty) | `object`.
- Unknown fields ignored (matches `s3fetch`: strict on known, ignore rest).

Bad config fails at parse (definition + workflow create time, never
runtime): `node type "jev": invalid config: <reason>`. Template shapes
like `{{ env.* }}` always pass validate and resolve at runtime. Missing
`env` key at runtime = render error.

## Ops: env + allowlist

- Server env `SIMPWF_OPENROUTER_KEY` must be set. At instance creation,
  `Create` snapshots `SIMPWF_*` process env under the reserved `env` root;
  the snapshot is per-instance, taken once.
- `openrouter.ai` must be in the engine HTTP allowlist — the node POSTs
  through the shared `HTTPExecutor.Do`, so allowlist/DNS policy applies
  unchanged.
- Workflow authors who can set config templates can exfiltrate `env`
  values via outputs — treat workflow authoring as trusted, same as
  script nodes.

## Raw output shape

```json
{
  "model": "typesafe/jev-1.13-20260917",
  "answers": {
    "is_urgent": {"type": "noul", "noul": 0.96},
    "department": {"type": "choice", "choice": "billing", "confidence": 0.67,
      "probabilities": {"billing": 0.78, "technical": 0.22}},
    "frustration": {"type": "score", "score": 1.99, "confidence": 0.99,
      "probabilities": {"0": 0, "1": 0, "2": 1}}
  },
  "usage": {"input_tokens": 476, "output_tokens": 70, "cost": 0.000019992}
}
```

`model` names the dated snapshot that served the request. Answers pass
through verbatim (numbers normalized); `usage` passes through when present.

## Downstream gating example

Thresholds live outside this node, e.g. a `conditions` node after it:

```js
// urgent billing tickets → "hot", everything else → "queue"
return context.jev.answers.is_urgent.noul > 0.9
  && context.jev.answers.department.choice === "billing";
```

## Error semantics

Fail fast, no partial output, never partial approve:

- Transport error / non-2xx (`jev-status` for HTTP ≥300) / oversize body /
  undecodable envelope → `NodeError`, nil result.
- Response missing a requested qid, `type` mismatch, or range violation
  (`noul` outside 0..1, `choice` naming an unrequested option or missing
  `probabilities`, `score` non-numeric or missing `probabilities`) →
  `NodeError`, never a partial approve.
- With `on_failure` set, an empty-map output (`{}`) accompanies the error
  so `routeFailure` has output to store (same contract shape as builtin
  http, which returns its result alongside `http-status`).

## Seed script

`scripts/seed_jev.sh [BASE_URL]` POSTs the `jev-triage` definition plus a
single-node `jev-demo` workflow (mirrors `scripts/seed.sh`: stdout JSON
`{node_definition_ids, workflow_definition_id}`, `seed-jev:` lines to
stderr). Needs a live server plus the ops prereqs above; thresholds stay
in workflow code, not in the seed.

## Registration

`init()` calls `customnode.MustRegister`. App pulls it via blank import
in `pkg/customnode/all/all.go`, itself blank-imported once in
`cmd/app/main.go`. Nil shared HTTP client = startup error.
