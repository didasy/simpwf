#!/usr/bin/env bash
# Seed openrouter custom-node definitions plus a chat-to-responses demo.
#
# Prereqs (server side):
#   - SIMPWF_OPENROUTER_KEY set in the server env (snapshotted per
#     instance under {{ env.SIMPWF_OPENROUTER_KEY }} at creation).
#   - openrouter.ai in the engine HTTP allowlist.
#
# Usage: scripts/seed_openrouter.sh [BASE_URL]

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"

fail() {
  echo "seed-openrouter: FAIL: $*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

echo "seed-openrouter: prereqs: SIMPWF_OPENROUTER_KEY in server env, openrouter.ai allowlisted" >&2

node_definitions=$(cat <<'JSON'
{
  "openrouter_chat": {
    "name": "openrouter-chat",
    "type": "openrouter",
    "content": {
      "type": "openrouter",
      "config": {
        "api": "chat",
        "model": "openai/gpt-4o-mini",
        "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
        "system": "Answer briefly and accurately.",
        "prompt": "{{ question }}",
        "temperature": 0.2,
        "max_tokens": 256,
        "top_p": 0.9
      },
      "output_property": "chat_answer",
      "timeout": "60s"
    }
  },
  "openrouter_responses": {
    "name": "openrouter-responses",
    "type": "openrouter",
    "content": {
      "type": "openrouter",
      "config": {
        "api": "responses",
        "model": "openai/gpt-4o-mini",
        "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
        "messages": [
          {
            "role": "developer",
            "content": "Use the prior chat answer and produce a concise follow-up."
          },
          {
            "role": "user",
            "content": "Question: {{ question }}\nPrior answer: {{ chat_answer.text }}"
          }
        ],
        "temperature": 0.3,
        "max_tokens": 256
      },
      "output_property": "responses_answer",
      "timeout": "60s"
    }
  }
}
JSON
)

workflow_template=$(cat <<'JSON'
{
  "content": {
    "start_node_id": "019fea42-0001-7000-8000-000000000001",
    "status_update": {
      "redis": {}
    },
    "keys": {},
    "nodes": [
      {
        "id": "019fea42-0001-7000-8000-000000000001",
        "name": "openrouter-chat",
        "next_node": "019fea42-0001-7000-8000-000000000002"
      },
      {
        "id": "019fea42-0001-7000-8000-000000000002",
        "name": "openrouter-responses"
      }
    ]
  },
  "name": "openrouter-demo"
}
JSON
)

node_definition_ids='{}'

while IFS= read -r key; do
  payload=$(jq -ce --arg key "$key" '.[$key]' <<<"$node_definitions") \
    || fail "read node definition $key"

  response=$(curl -fsS -X POST \
    -H 'Content-Type: application/json' \
    --data-binary "$payload" \
    "$BASE_URL/v1/node/definition") \
    || fail "create node definition $key"

  id=$(jq -er '.id' <<<"$response") \
    || fail "parse node definition id for $key"
  name=$(jq -er '.name' <<<"$payload") \
    || fail "parse node definition name for $key"
  schema_type=$(jq -er '.schema.properties.type.const // empty' <<<"$response") \
    || fail "node definition $key response missing schema"

  node_definition_ids=$(jq -c \
    --arg name "$name" \
    --arg id "$id" \
    '.[$name] = $id' <<<"$node_definition_ids")

  echo "seed-openrouter: created node definition $name ($id, schema: $schema_type)" >&2
done < <(jq -er 'keys[]' <<<"$node_definitions")

workflow_payload=$(jq -ce --argjson ids "$node_definition_ids" '
  .content.nodes |= map(
    .node_definition_id = (
      $ids[.name] // error("no node definition id for workflow node " + .name)
    )
  )
' <<<"$workflow_template") || fail "build workflow definition payload"

workflow_response=$(curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  --data-binary "$workflow_payload" \
  "$BASE_URL/v1/workflow/definition") \
  || fail "create workflow definition"

workflow_definition_id=$(jq -er '.id' <<<"$workflow_response") \
  || fail "parse workflow definition id"
workflow_schemas=$(jq -er '.schemas | keys | sort | join(",")' <<<"$workflow_response") \
  || fail "workflow definition response missing schemas"

echo "seed-openrouter: created workflow definition $workflow_definition_id (schemas: $workflow_schemas)" >&2
jq -n \
  --arg workflow_definition_id "$workflow_definition_id" \
  --argjson node_definition_ids "$node_definition_ids" \
  '{
    node_definition_ids: $node_definition_ids,
    workflow_definition_id: $workflow_definition_id
  }'
