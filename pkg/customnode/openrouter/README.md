# openrouter — Chat Completions and Responses API custom node

Calls OpenRouter text models through the shared HTTP executor. It sends a
small rendered request, returns only compact text and usage metadata, and
supports both API shapes:

- `chat`: `POST https://openrouter.ai/api/v1/chat/completions` with
  `{model, messages, ...}`.
- `responses`: `POST https://openrouter.ai/api/v1/responses` with
  `{model, input, ...}`.

## Ops prerequisites

- Set server env `SIMPWF_OPENROUTER_KEY`. Instance creation snapshots
  `SIMPWF_*` values under `env`; reference it as
  `{{ env.SIMPWF_OPENROUTER_KEY }}`. Snapshot is per-instance and taken once.
- Add `openrouter.ai` to `engine.http_allowlist` in `config.yaml`, or to
  comma-separated `SIMPWF_ENGINE_HTTP_ALLOWLIST` when running without a
  config file. Shared HTTP allowlist, DNS, redirect, and output-cap policy
  applies unchanged.
- Workflow authors can read snapshot values through templates. Treat
  workflow authoring as trusted, same as script-node authors.
- Use 60s or longer timeouts for LLM calls. Node-level `timeout` works;
  config `timeout` overrides request timeout when set.

## Config reference

- `api`: `chat` (default) or `responses`.
- `model`: optional, defaults to `openai/gpt-4o-mini`; template allowed.
- `endpoint`: optional absolute `http(s)` URL; template allowed. Defaults
  per API as listed above. Rendered endpoint is revalidated before request.
- `api_key`: required, non-blank raw string, template allowed. Typical
  value: `{{ env.SIMPWF_OPENROUTER_KEY }}`. Never returned or logged.
- Exactly one input form:
  - `prompt` required non-blank string, template allowed. Optional
    `system` non-blank string is prepended as a system message. `system`
    is forbidden with explicit `messages[]`.
  - `messages[]` required non-empty array. Each entry has non-blank `role`
    and `content`. `content` is a templateable string or array of content
    parts. Roles are `system`, `user`, or `assistant`; `developer` is also
    accepted for `responses`. Templated roles are checked after rendering.
- Optional passthrough fields, each template allowed:
  - `temperature`: `0..2`.
  - `max_tokens`: positive integer. Sent as `max_tokens` for chat and
    `max_output_tokens` for responses.
  - `top_p`: greater than `0`, at most `1`.
  - `stop`: non-empty string array, at least one non-blank value.
  - `timeout`: positive Go duration. Falls back to node timeout, then 30s.
- Unknown fields are ignored. Literal invalid values fail parse; template
  values are checked after rendering. Template failures fail execution.

## Example workflow YAML

Standalone example: [`docs/openrouter-workflow.yaml`](../../docs/openrouter-workflow.yaml).

```yaml
start_node_id: "019fea42-0001-7000-8000-000000000001"
nodes:
  - type: openrouter
    id: "019fea42-0001-7000-8000-000000000001"
    name: "chat-answer"
    config:
      api: chat
      model: openai/gpt-4o-mini
      api_key: "{{ env.SIMPWF_OPENROUTER_KEY }}"
      system: "Answer briefly and accurately."
      prompt: "{{ question }}"
      temperature: 0.2
      max_tokens: 256
      top_p: 0.9
    timeout: 60s
    output_property: chat_answer
    next_node: "019fea42-0001-7000-8000-000000000002"

  - type: openrouter
    id: "019fea42-0001-7000-8000-000000000002"
    name: "responses-answer"
    config:
      api: responses
      model: openai/gpt-4o-mini
      api_key: "{{ env.SIMPWF_OPENROUTER_KEY }}"
      messages:
        - role: developer
          content: "Use the chat answer and produce a concise follow-up."
        - role: user
          content: "Question: {{ question }}\nPrior answer: {{ chat_answer.text }}"
    timeout: 60s
    output_property: responses_answer
```

## Output

```json
{
  "model": "openai/gpt-4o-mini",
  "api": "chat",
  "text": "Model answer",
  "usage": {
    "prompt_tokens": 7,
    "completion_tokens": 2,
    "total_tokens": 9
  },
  "finish_reason": "stop"
}
```

Chat text comes from `choices[0].message.content`. Responses text comes
from `output_text`, falling back to nested `output[].content[]` text parts.
Usage numbers normalize to `float64`. Raw provider response is never
returned.

Transport, decode, missing/blank text, and output-cap failures return
`NodeError` reason `openrouter`. HTTP status >= 300 returns reason
`openrouter-status`. With `on_failure`, an empty output object accompanies
the error so normal `routeFailure` output storage works.

## Seed workflow

After the server is running and ops prerequisites are set:

```bash
scripts/seed_openrouter.sh [BASE_URL]
```

Script creates `openrouter-chat` and `openrouter-responses` node
definitions plus a two-node `openrouter-demo` workflow. It prints JSON
containing `node_definition_ids` and `workflow_definition_id` to stdout;
progress goes to stderr. Start instances with context:

```json
{"question": "What is a workflow engine?"}
```

## Registration

`init()` calls `customnode.MustRegister`. App pulls it via blank import in
`pkg/customnode/all/all.go`. Nil shared HTTP client fails startup.
