#!/usr/bin/env bash
# Seed a parallel fan-out/join demo: node definitions plus a workflow wiring
# them, mirroring scripts/seed_jev.sh.
#
# Background: parallel_start/parallel_end are structural engine nodes, but
# their definitions still live in the node_definitions table, so a fresh
# stack needs them POSTed before a workflow can reference them. The fanout
# definition carries the branch map (branch name -> graph node id) and the
# join id; the join definition carries the combining_script that merges the
# frozen branch contexts back into the parent.
#
# The demo forks two script branches over the input number `n`, joins them
# with a combining script, and reports the sum. It is self-contained: no
# external HTTP calls, so it runs offline.
#
# Usage: scripts/seed_parallel.sh [BASE_URL]
#
# Start an instance with:
#   curl -s -X POST http://localhost:8080/v1/workflow/instance \
#     -H 'Content-Type: application/json' \
#     -d '{"workflow_definition_id":"<id>","context":{"n":21}}'
# Once the join completes, context.total is {"sum":105}.

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"

fail() {
  echo "seed-parallel: FAIL: $*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

node_definitions=$(cat <<'JSON'
{
  "parallel_fanout": {
    "name": "parallel-fanout",
    "type": "parallel_start",
    "content": {
      "type": "parallel_start",
      "branches": {
        "left": "019fea43-0001-7000-8000-000000000002",
        "right": "019fea43-0001-7000-8000-000000000003"
      },
      "parallel_end_node_id": "019fea43-0001-7000-8000-000000000004"
    }
  },
  "double_n": {
    "name": "double-n",
    "type": "script",
    "content": {
      "type": "script",
      "script": "return { doubled: input * 2 };",
      "input_data": "n",
      "output_property": "left",
      "timeout": "30s"
    }
  },
  "triple_n": {
    "name": "triple-n",
    "type": "script",
    "content": {
      "type": "script",
      "script": "return { tripled: input * 3 };",
      "input_data": "n",
      "output_property": "right",
      "timeout": "30s"
    }
  },
  "parallel_join": {
    "name": "parallel-join",
    "type": "parallel_end",
    "content": {
      "type": "parallel_end",
      "combining_script": "context.sum = branch[\"left\"].context.left.doubled + branch[\"right\"].context.right.tripled;"
    }
  },
  "report_sum": {
    "name": "report-sum",
    "type": "script",
    "content": {
      "type": "script",
      "script": "return { sum: context.sum };",
      "output_property": "total",
      "timeout": "30s"
    }
  }
}
JSON
)

workflow_template=$(cat <<'JSON'
{
  "content": {
    "start_node_id": "019fea43-0001-7000-8000-000000000001",
    "status_update": {
      "redis": {}
    },
    "keys": {},
    "nodes": [
      {
        "id": "019fea43-0001-7000-8000-000000000001",
        "name": "parallel-fanout"
      },
      {
        "id": "019fea43-0001-7000-8000-000000000002",
        "name": "double-n",
        "next_node": "019fea43-0001-7000-8000-000000000004"
      },
      {
        "id": "019fea43-0001-7000-8000-000000000003",
        "name": "triple-n",
        "next_node": "019fea43-0001-7000-8000-000000000004"
      },
      {
        "id": "019fea43-0001-7000-8000-000000000004",
        "name": "parallel-join",
        "next_node": "019fea43-0001-7000-8000-000000000005"
      },
      {
        "id": "019fea43-0001-7000-8000-000000000005",
        "name": "report-sum"
      }
    ]
  },
  "name": "parallel-demo"
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

  echo "seed-parallel: created node definition $name ($id, schema: $schema_type)" >&2
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

echo "seed-parallel: created workflow definition $workflow_definition_id (schemas: $workflow_schemas)" >&2
jq -n \
  --arg workflow_definition_id "$workflow_definition_id" \
  --argjson node_definition_ids "$node_definition_ids" \
  '{
    node_definition_ids: $node_definition_ids,
    workflow_definition_id: $workflow_definition_id
  }'
