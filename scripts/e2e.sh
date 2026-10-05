#!/usr/bin/env bash
# SimpWF black-box end-to-end checks against a running app.
#
#   bash scripts/e2e.sh [BASE_URL] [WORKFLOW_JSON]
#
# Prerequisites: the app must be running (task run or docker compose up),
# curl and jq must be installed. The workflow under test uses only script,
# conditions, and input nodes, so no HTTP/exec allowlist entries are needed.
#
# The schedule section covers create/tick/pause/resume/delete over HTTP.
# Restart durability (a surviving schedule resumes firing after an app
# restart) needs control of the app lifecycle, so it stays a manual check:
# create a schedule with "@every 5s", note the instance total, restart the
# app, and confirm the total grows within ~30s.

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
printf '%s' "$wf_resp" | jq -er '.schemas | select(. != null and length > 0)' >/dev/null || fail "workflow definition missing schemas"
pass "created workflow definition $WF_ID (schemas attached)"

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

# --- cron schedules ------------------------------------------------------------
sched_resp=$(curl -fsS -X POST "$BASE_URL/v1/workflow/schedules" \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"crontab\":\"@every 5s\",\"context\":{\"scheduled\":true}}") \
  || fail "create schedule"
SCHED_ID=$(printf '%s' "$sched_resp" | jq -er '.id') || fail "parse schedule id"
[[ "$(printf '%s' "$sched_resp" | jq -r '.enabled')" == "true" ]] || fail "schedule not enabled: $sched_resp"
pass "created schedule $SCHED_ID"

list=$(curl -fsS "$BASE_URL/v1/workflow/schedules") || fail "list schedules"
[[ "$(printf '%s' "$list" | jq -r '.total')" -ge 1 ]] || fail "schedule list total: $list"
one=$(curl -fsS "$BASE_URL/v1/workflow/schedules/$SCHED_ID") || fail "get schedule"
[[ "$(printf '%s' "$one" | jq -r '.id')" == "$SCHED_ID" ]] || fail "get schedule: $one"
pass "list/get schedule"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/schedules" \
  -H 'Content-Type: application/json' \
  -d '{"workflow_definition_id":"bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb","crontab":"* * * * *"}')
[[ "$code" == "404" ]] || fail "unknown definition -> $code, want 404"
pass "unknown definition rejected (404)"

# A tick must create an instance of the definition. Counts are relative so a
# shared database cannot flake the check.
sched_before=$(curl -fsS "$BASE_URL/v1/workflow/instance?workflow_definition_id=$WF_ID&per_page=100" | jq -r '.total')
fired=""
for _ in $(seq 1 40); do
  sched_now=$(curl -fsS "$BASE_URL/v1/workflow/instance?workflow_definition_id=$WF_ID&per_page=100" | jq -r '.total')
  [[ "$sched_now" -gt "$sched_before" ]] && { fired=1; break; }
  sleep 1
done
[[ -n "$fired" ]] || fail "no scheduled instance within 40s"
pass "schedule tick created an instance"

paused=$(curl -fsS -X POST "$BASE_URL/v1/workflow/schedules/$SCHED_ID/pause") || fail "pause schedule"
[[ "$(printf '%s' "$paused" | jq -r '.enabled')" == "false" ]] || fail "pause response: $paused"
sleep 2
paused_total=$(curl -fsS "$BASE_URL/v1/workflow/instance?workflow_definition_id=$WF_ID&per_page=100" | jq -r '.total')
sleep 12
still=$(curl -fsS "$BASE_URL/v1/workflow/instance?workflow_definition_id=$WF_ID&per_page=100" | jq -r '.total')
[[ "$still" == "$paused_total" ]] || fail "instances grew while paused ($paused_total -> $still)"
pass "paused schedule is silent"

curl -fsS -X POST "$BASE_URL/v1/workflow/schedules/$SCHED_ID/resume" >/dev/null || fail "resume schedule"
resumed=""
for _ in $(seq 1 30); do
  sched_now=$(curl -fsS "$BASE_URL/v1/workflow/instance?workflow_definition_id=$WF_ID&per_page=100" | jq -r '.total')
  [[ "$sched_now" -gt "$paused_total" ]] && { resumed=1; break; }
  sleep 1
done
[[ -n "$resumed" ]] || fail "no tick after resume within 30s"
pass "resumed schedule fired again"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$BASE_URL/v1/workflow/schedules/$SCHED_ID")
[[ "$code" == "204" ]] || fail "delete schedule -> $code, want 204"
code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/v1/workflow/schedules/$SCHED_ID")
[[ "$code" == "404" ]] || fail "get after delete -> $code, want 404"
pass "deleted schedule"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/workflow/schedules" \
  -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"crontab\":\"nope\"}")
[[ "$code" == "422" ]] || fail "invalid crontab -> $code, want 422"
pass "invalid crontab rejected (422)"

echo "e2e: ALL CHECKS PASSED"
