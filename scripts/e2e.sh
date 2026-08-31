#!/usr/bin/env bash
# SimpWF black-box end-to-end checks against a running app.
#
#   bash scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]
#
# Prerequisites: the app must be running (task run or docker compose up),
# curl and jq must be installed. The workflow under test uses only script,
# conditions, and input nodes, so no HTTP/exec allowlist entries are needed.

set -euo pipefail

BASE_URL="${1:-http://localhost:9999}"
WORKFLOW_JSON="${2:-$(dirname "$0")/../docs/examples/workflow.json}"

fail() { echo "e2e: FAIL: $*" >&2; exit 1; }
pass() { echo "e2e: ok: $*"; }

# --- wait for readiness -------------------------------------------------------
for i in $(seq 1 60); do
  if curl -fsS "$BASE_URL/health/ready" >/dev/null 2>&1; then
    pass "app is ready"
    break
  fi
  sleep 1
  if [[ "$i" == "60" ]]; then fail "app did not become ready"; fi
done

# --- create workflow definition ------------------------------------------------
wf_resp=$(curl -fsS -X POST "$BASE_URL/v1/workflow/definition" \
  -H 'Content-Type: application/json' \
  --data-binary "@$WORKFLOW_JSON") || fail "create workflow definition"
WF_ID=$(printf '%s' "$wf_resp" | jq -er '.id') || fail "parse workflow definition id"
pass "created workflow definition $WF_ID"

# --- create an instance (Manager path -> input node) ---------------------------
inst_resp=$(curl -fsS -X POST "$BASE_URL/v1/workflow/instance" \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"context\":{\"user\":{\"name\":\"Jono\",\"title\":\"Manager\"}}}") \
  || fail "create instance"
INST_ID=$(printf '%s' "$inst_resp" | jq -er '.id') || fail "parse instance id"
pass "created instance $INST_ID"

# --- wait for the instance to park on the input node ----------------------------
for _ in $(seq 1 60); do
  status=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST_ID/status") || fail "get status"
  s=$(printf '%s' "$status" | jq -r '.status')
  reason=$(printf '%s' "$status" | jq -r '.waiting_reason // "none"')
  [[ "$s" == "waiting" && "$reason" == "input" ]] && break
  sleep 1
done
[[ "$s" == "waiting" && "$reason" == "input" ]] || fail "instance did not park on input (status=$s reason=$reason)"
pass "instance waiting on input"

NODE_IDS=$(printf '%s' "$wf_resp" | jq -r '.content.nodes[].id')
INPUT_NODE_ID=$(printf '%s' "$wf_resp" | jq -er '.content.nodes[] | select(.type == "input") | .id')
STAFF_NODE_ID=$(printf '%s' "$wf_resp" | jq -er '.content.nodes[] | select(.name == "Staff Path") | .id')

# --- node debug: input node not_started until delivery? --------------------------
# The input node has an occurrence once the instance parks; verify a node that
# never ran (Staff Path) reports not_started.
debug=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST_ID/status/node/$STAFF_NODE_ID") || fail "node debug (unstarted)"
[[ "$(printf '%s' "$debug" | jq -r '.status')" == "not_started" ]] || fail "staff node not not_started: $(printf '%s' "$debug" | jq -r '.status')"
pass "unstarted node reports not_started"

# --- deliver input ---------------------------------------------------------------
delivery=$(curl -fsS -X PUT "$BASE_URL/v1/workflow/instance/$INST_ID/input" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: e2e-001' \
  -d '{"approved":true}') || fail "deliver input"
[[ "$(printf '%s' "$delivery" | jq -r '.accepted')" == "true" ]] || fail "input not accepted: $delivery"
pass "input accepted"

# Replay with the same key must be idempotent (still accepted).
replay=$(curl -fsS -X PUT "$BASE_URL/v1/workflow/instance/$INST_ID/input" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: e2e-001' \
  -d '{"approved":true}') || fail "replay input"
[[ "$(printf '%s' "$replay" | jq -r '.accepted')" == "true" ]] || fail "input replay not idempotent: $replay"
pass "input replay idempotent"

# --- wait for finish ---------------------------------------------------------------
for _ in $(seq 1 60); do
  status=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST_ID/status") || fail "get status"
  s=$(printf '%s' "$status" | jq -r '.status')
  [[ "$s" == "finished" ]] && break
  sleep 1
done
[[ "$s" == "finished" ]] || fail "instance did not finish (status=$s)"
pass "instance finished"

ctx=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST_ID/context") || fail "get context"
[[ "$(printf '%s' "$ctx" | jq -r '.context.approval.approved')" == "true" ]] || fail "approval not written to context"
[[ "$(printf '%s' "$ctx" | jq -r '.context.final.done')" == "true" ]] || fail "final node output missing"
pass "context contains input and script outputs"

# --- node debug on a finished node ---------------------------------------------------
debug=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST_ID/status/node/$INPUT_NODE_ID?attempt=1") || fail "node debug (finished)"
[[ "$(printf '%s' "$debug" | jq -r '.status')" == "finished" ]] || fail "input node not finished: $(printf '%s' "$debug" | jq -r '.status')"
[[ "$(printf '%s' "$debug" | jq -r '.attempt_count')" == "1" ]] || fail "attempt_count != 1"
pass "node debug reports finished attempt"

# --- controls ------------------------------------------------------------------------
# A finished instance rejects pause/resume/stop with 409.
for verb in pause resume stop; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/instance/$INST_ID/$verb")
  [[ "$code" == "409" ]] || fail "$verb on finished instance -> $code, want 409"
done
pass "terminal instance rejects controls (409)"

# Fresh instance: pause (immediate on waiting), resume, stop.
inst2=$(curl -fsS -X POST "$BASE_URL/v1/workflow/instance" \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"context\":{\"user\":{\"name\":\"Jono\",\"title\":\"Manager\"}}}") \
  || fail "create instance 2"
INST2=$(printf '%s' "$inst2" | jq -er '.id')

for _ in $(seq 1 60); do
  status=$(curl -fsS "$BASE_URL/v1/workflow/instance/$INST2/status") || fail "get status 2"
  s=$(printf '%s' "$status" | jq -r '.status')
  reason=$(printf '%s' "$status" | jq -r '.waiting_reason // "none"')
  [[ "$s" == "waiting" && "$reason" == "input" ]] && break
  sleep 1
done
[[ "$s" == "waiting" && "$reason" == "input" ]] || fail "instance 2 did not park on input"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/instance/$INST2/pause")
[[ "$code" == "200" ]] || fail "pause -> $code, want 200"
pass "pause immediate (200)"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/instance/$INST2/resume")
[[ "$code" == "200" ]] || fail "resume -> $code, want 200"
pass "resume (200)"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/instance/$INST2/stop")
[[ "$code" == "200" ]] || fail "stop -> $code, want 200"
stop_resp=$(curl -fsS -X POST "$BASE_URL/v1/workflow/instance/$INST2/stop")
[[ "$(printf '%s' "$stop_resp" | jq -r '.status')" == "stopped" ]] || fail "stop response: $stop_resp"
pass "stop (200, idempotent)"

echo "e2e: ALL CHECKS PASSED"
