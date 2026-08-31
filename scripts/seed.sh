#!/usr/bin/env bash
# Create sample node definitions, then create the sample workflow with the
# returned node definition IDs.
#
# Usage: scripts/seed.sh [BASE_URL]

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"

fail() {
  echo "seed: FAIL: $*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

node_definitions=$(cat <<'JSON'
{
  "script": {
    "name": "calculate-total",
    "type": "script",
    "content": {
      "type": "script",
      "script": "return input.length;",
      "input_data": "posts.Body",
      "output_property": "total",
      "timeout": "30s"
    }
  },
  "conditions": {
    "name": "route-by-total",
    "type": "conditions",
    "content": {
      "type": "conditions",
      "conditions": [
        {
          "condition": "return context.posts.Body.length >= 100;",
          "key": "many"
        },
        {
          "condition": "return context.posts.Body.length < 100;",
          "key": "less"
        }
      ]
    }
  },
  "input": {
    "name": "new-post",
    "type": "input",
    "content": {
      "type": "input",
      "channel": "http",
      "context_path": "new_post",
      "validation": {
        "script": "return input != null && Object.prototype.toString.call(input) === '[object Object]';"
      }
    }
  },
  "external_call": {
    "name": "get-posts",
    "type": "external_call",
    "content": {
      "type": "external_call",
      "http_config": {
        "url": "https://jsonplaceholder.typicode.com/posts",
        "method": "GET"
      },
      "output_property": "posts",
      "timeout": "30s",
      "retry_on_recovery": true
    }
  }
}
JSON
)

workflow_template=$(cat <<'JSON'
{
  "content": {
    "start_node_id": "019fea41-0001-7000-8000-000000000001",
    "status_update": {
      "redis": {}
    },
    "keys": {
      "many": "019fea41-3005-758d-a647-ad9a0ca8e21a",
      "less": ""
    },
    "nodes": [
      {
        "id": "019fea41-0001-7000-8000-000000000001",
        "name": "get-posts",
        "output_property": "posts",
        "retry_on_recovery": true,
        "next_node": "019fea41-0682-7c77-9eec-ae9037cc0e71"
      },
      {
        "id": "019fea41-0682-7c77-9eec-ae9037cc0e71",
        "name": "route-by-total"
      },
      {
        "id": "019fea41-3005-758d-a647-ad9a0ca8e21a",
        "name": "new-post",
        "next_node": "019fea40-ddb2-7b4b-98fb-1c01a04d33e8"
      },
      {
        "id": "019fea40-ddb2-7b4b-98fb-1c01a04d33e8",
        "name": "calculate-total",
        "output_property": "total"
      }
    ]
  },
  "name": "main"
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

  node_definition_ids=$(jq -c \
    --arg name "$name" \
    --arg id "$id" \
    '.[$name] = $id' <<<"$node_definition_ids")

  echo "seed: created node definition $name ($id)" >&2
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

echo "seed: created workflow definition $workflow_definition_id" >&2
jq -n \
  --arg workflow_definition_id "$workflow_definition_id" \
  --argjson node_definition_ids "$node_definition_ids" \
  '{
    node_definition_ids: $node_definition_ids,
    workflow_definition_id: $workflow_definition_id
  }'
