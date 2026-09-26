#!/usr/bin/env bash
# Seed the jev custom-node definition plus a demo single-node workflow.
#
# Background: the jev type registers in-process at startup
# (pkg/customnode/jev init via pkg/customnode/all blank import).
# GET /v1/node/definition lists only rows in the node_definitions table,
# so fresh stacks return empty until definitions are POSTed. This script
# POSTs a ready-to-use jev triage definition plus a demo workflow wiring
# that node, mirroring scripts/seed.sh.
#
# Prereqs (server side):
#   - SIMPWF_OPENROUTER_KEY set in the server env (snapshotted per
#     instance under {{ env.SIMPWF_OPENROUTER_KEY }} at creation).
#   - openrouter.ai in the engine HTTP allowlist.
#
# Usage: scripts/seed_jev.sh [BASE_URL]

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"

fail() {
  echo "seed-jev: FAIL: $*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

echo "seed-jev: prereqs: SIMPWF_OPENROUTER_KEY in server env, openrouter.ai allowlisted" >&2

node_definitions=$(cat <<'JSON'
{
  "jev_triage": {
    "name": "jev-triage",
    "type": "jev",
    "content": {
      "type": "jev",
      "config": {
        "api_key": "{{ env.SIMPWF_OPENROUTER_KEY }}",
        "state": "{{ ticket }}",
        "questions": {
          "is_urgent": {
            "type": "noul",
            "instructions": "Does this message convey urgency?",
            "criteria": {
              "true": "Explicitly time-sensitive",
              "false": "No urgency expressed"
            }
          },
          "department": {
            "type": "choice",
            "instructions": "Which team should own this ticket?",
            "criteria": {
              "billing": "Payments, refunds, checkout issues",
              "technical": "Bugs, outages, rendering issues"
            }
          },
          "frustration": {
            "type": "score",
            "instructions": "How frustrated is the customer?",
            "criteria": ["Calm", "Frustrated", "Very angry"]
          }
        }
      },
      "output_property": "jev",
      "timeout": "60s"
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
    "keys": {},
    "nodes": [
      {
        "id": "019fea41-0001-7000-8000-000000000001",
        "name": "jev-triage"
      }
    ]
  },
  "name": "jev-demo"
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

  echo "seed-jev: created node definition $name ($id, schema: $schema_type)" >&2
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

echo "seed-jev: created workflow definition $workflow_definition_id (schemas: $workflow_schemas)" >&2
jq -n \
  --arg workflow_definition_id "$workflow_definition_id" \
  --argjson node_definition_ids "$node_definition_ids" \
  '{
    node_definition_ids: $node_definition_ids,
    workflow_definition_id: $workflow_definition_id
  }'
