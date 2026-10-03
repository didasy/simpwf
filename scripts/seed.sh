#!/usr/bin/env bash
# Create sample node definitions, then create the sample workflows with the
# returned node definition IDs: "main" (private input node) and
# "main-public" (same graph, public input node).
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
  "script_no_output": {
    "name": "add-lucky-number",
    "type": "script",
    "content": {
      "type": "script",
      "script": "return Math.floor(Math.random() * 1000);",
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
      "output_property": "new_post",
      "form": {
        "schema": {
          "type": "object",
          "required": ["title", "body"],
          "properties": {
            "title": {"type": "string", "minLength": 1},
            "body": {"type": "string", "minLength": 1}
          }
        },
        "ui": {
          "order": ["title", "body"],
          "fields": {
            "title": {"label": "Title", "placeholder": "Post title"},
            "body": {"label": "Body", "widget": "textarea", "placeholder": "Write something..."}
          }
        }
      },
      "validation": {
        "script": "return input != null && Object.prototype.toString.call(input) === '[object Object]';"
      }
    }
  },
  "input_public": {
    "name": "public-post",
    "type": "input",
    "content": {
      "type": "input",
      "channel": "http",
      "output_property": "public_post",
      "form": {
        "schema": {
          "type": "object",
          "required": ["title", "body"],
          "properties": {
            "title": {"type": "string", "minLength": 1},
            "body": {"type": "string", "minLength": 1}
          }
        },
        "ui": {
          "order": ["title", "body"],
          "fields": {
            "title": {"label": "Title", "placeholder": "Post title"},
            "body": {"label": "Body", "widget": "textarea", "placeholder": "Write something..."}
          }
        }
      },
      "validation": {
        "script": "return input != null && Object.prototype.toString.call(input) === '[object Object]';"
      },
      "public": true
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

# The "less" key has no target, so that branch ends the run: main parks on
# the private new-post input only when there are many posts.
workflow_template=$(cat <<'JSON'
{
  "content": {
    "start_node_id": "019fea41-0001-7000-8000-000000000001",
    "status_update": {
      "redis": {}
    },
    "keys": {
      "many": "019fea41-3005-758d-a647-ad9a0ca8e21a",
      "less": null
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
        "output_property": "total",
        "next_node": "019fea42-0000-7016-8814-16cbe5663eff"
      },
      {
        "id": "019fea42-0000-7016-8814-16cbe5663eff",
        "name": "add-lucky-number"
      }
    ]
  },
  "name": "main"
}
JSON
)

# The same graph as main, except the "many" branch parks on a public input
# node instead of the private one, so it accepts an anonymous delivery with no
# credential at all. The "less" branch ends the run, as in main. Node ids are
# distinct from main so both definitions coexist.
workflow_public_template=$(cat <<'JSON'
{
  "content": {
    "start_node_id": "01a0fcdb-cbfd-7f95-b6a5-e836c523cd58",
    "status_update": {
      "redis": {}
    },
    "keys": {
      "many": "01a0fcdb-cbfd-756b-820b-b0b6e5d6c853",
      "less": null
    },
    "nodes": [
      {
        "id": "01a0fcdb-cbfd-7f95-b6a5-e836c523cd58",
        "name": "get-posts",
        "output_property": "posts",
        "retry_on_recovery": true,
        "next_node": "01a0fcdb-cbfd-79b1-ad03-108fc6663e1d"
      },
      {
        "id": "01a0fcdb-cbfd-79b1-ad03-108fc6663e1d",
        "name": "route-by-total"
      },
      {
        "id": "01a0fcdb-cbfd-756b-820b-b0b6e5d6c853",
        "name": "public-post",
        "next_node": "01a0fcdb-cbfd-7060-9acc-b5617f757925"
      },
      {
        "id": "01a0fcdb-cbfd-7060-9acc-b5617f757925",
        "name": "calculate-total",
        "output_property": "total",
        "next_node": "01a0fcdb-cbfd-7d5d-a8f1-c32d66004f8f"
      },
      {
        "id": "01a0fcdb-cbfd-7d5d-a8f1-c32d66004f8f",
        "name": "add-lucky-number"
      }
    ]
  },
  "name": "main-public"
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

  echo "seed: created node definition $name ($id, schema: $schema_type)" >&2
done < <(jq -er 'keys[]' <<<"$node_definitions")

# create_workflow resolves node definitions, posts the definition, and prints
# the created id on stdout. Every node definition is created above, so the
# same id map serves both workflow templates.
create_workflow() {
  local template=$1
  local label=$2
  local payload response id schemas

  payload=$(jq -ce --argjson ids "$node_definition_ids" '
    .content.nodes |= map(
      .node_definition_id = (
        $ids[.name] // error("no node definition id for workflow node " + .name)
      )
    )
  ' <<<"$template") || fail "build $label workflow definition payload"

  response=$(curl -fsS -X POST \
    -H 'Content-Type: application/json' \
    --data-binary "$payload" \
    "$BASE_URL/v1/workflow/definition") \
    || fail "create $label workflow definition"

  id=$(jq -er '.id' <<<"$response") \
    || fail "parse $label workflow definition id"
  schemas=$(jq -er '.schemas | keys | sort | join(",")' <<<"$response") \
    || fail "$label workflow definition response missing schemas"

  echo "seed: created $label workflow definition $id (schemas: $schemas)" >&2
  printf '%s\n' "$id"
}

main_workflow_definition_id=$(create_workflow "$workflow_template" main) \
  || fail "create main workflow definition"
public_workflow_definition_id=$(create_workflow "$workflow_public_template" main-public) \
  || fail "create public-input workflow definition"

jq -n \
  --arg workflow_definition_id "$main_workflow_definition_id" \
  --arg public_workflow_definition_id "$public_workflow_definition_id" \
  --argjson node_definition_ids "$node_definition_ids" \
  '{
    node_definition_ids: $node_definition_ids,
    workflow_definition_id: $workflow_definition_id,
    public_workflow_definition_id: $public_workflow_definition_id
  }'
