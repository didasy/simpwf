#!/usr/bin/env bash
# SimpWF end-to-end check of the OIDC + RBAC + input-gate + attribution flow,
# against a running app wired to a throwaway mock identity provider.
#
#   bash scripts/e2e-oidc.sh [APP_BASE_URL] [OIDC_ISSUER_URL]
#
# The script stands up a mock OIDC provider (discovery + JWKS), mints three
# JWTs signed by its key, and drives the app the way a frontend would:
#
#   * a login contract is readable without any credential;
#   * a token with a role that holds input:deliver but is absent from the
#     parked node's allowed_roles is refused 403 (the second gate);
#   * the same caller on a node that lists the role is accepted, and the
#     context records who delivered it (the attribution envelope);
#   * an API token bypasses both gates and is recorded as the system user.
#
# The app must be started with auth.oidc.enabled, the issuer below, and a role
# catalog whose "finance" role grants input:deliver (see config.yaml).
# SIMPWF_API_TOKEN must name the same credential the app was started with; the
# task target exports it once for both.

set -euo pipefail

APP_BASE_URL="${1:-http://localhost:9999}"
OIDC_ISSUER_URL="${2:-http://127.0.0.1:9099}"
MOCK_PID=""
WORK_DIR=""

fail() { echo "e2e-oidc: FAIL: $*" >&2; exit 1; }
pass() { echo "e2e-oidc: ok: $*"; }

# The bypass and attribution assertions are half of what this script checks,
# so the token is mandatory rather than optional: a run without it would
# report success while skipping the behavior it exists to prove.
[[ -n "${SIMPWF_API_TOKEN:-}" ]] \
  || fail "SIMPWF_API_TOKEN must be set: the API-token bypass and attribution checks are mandatory"

cleanup() {
  if [[ -n "$MOCK_PID" ]]; then
    kill "$MOCK_PID" 2>/dev/null || true
    wait "$MOCK_PID" 2>/dev/null || true
  fi
  # The workflow definition is written to a scratch directory, so a failed
  # run would otherwise leave it behind for every later run to trip over.
  if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

# --- mock OIDC provider ------------------------------------------------------------
# A real signing key and a real JWKS: the app performs discovery and verifies
# the signature exactly as it would against Zitadel or Keycloak. If a provider
# is already listening on the issuer port (an app started beforehand needs one
# at boot), reuse it instead of binding a second time.
if curl -fsS "$OIDC_ISSUER_URL/.well-known/openid-configuration" >/dev/null 2>&1; then
  pass "reusing the mock OIDC provider already listening on $OIDC_ISSUER_URL"
else
  python3 "$(dirname "$0")/mock_oidc_provider.py" "$OIDC_ISSUER_URL" &
  MOCK_PID=$!
  for _ in $(seq 1 50); do
    curl -fsS "$OIDC_ISSUER_URL/.well-known/openid-configuration" >/dev/null 2>&1 && break
    sleep 0.2
  done
  curl -fsS "$OIDC_ISSUER_URL/.well-known/openid-configuration" >/dev/null || fail "mock OIDC provider did not start"
  pass "mock OIDC provider serving discovery and JWKS"
fi

finance_token() { curl -fsS "$OIDC_ISSUER_URL/token/$1"; }

# --- the login contract is public ----------------------------------------------------
cfg=$(curl -fsS "$APP_BASE_URL/v1/auth/config") || fail "read auth config without a credential"
[[ "$(printf '%s' "$cfg" | jq -r '.enabled')" == "true" ]] || fail "auth config does not advertise OIDC: $cfg"
[[ "$(printf '%s' "$cfg" | jq -r '.issuer')" == "$OIDC_ISSUER_URL" ]] || fail "auth config issuer mismatch: $cfg"
[[ "$(printf '%s' "$cfg" | jq -r '.roles | index("finance") != null')" == "true" ]] || fail "auth config does not list the finance role: $cfg"
[[ "$(printf '%s' "$cfg" | jq -r '.authorization_url')" == "$OIDC_ISSUER_URL/authorize" ]] || fail "auth config authorization_url: $cfg"
[[ "$(printf '%s' "$cfg" | jq -r '.token_url')" == "$OIDC_ISSUER_URL/token" ]] || fail "auth config token_url: $cfg"
pass "GET /v1/auth/config is public and carries the login contract"

# A missing credential is 401, never an open door.
code=$(curl -sS -o /dev/null -w '%{http_code}' "$APP_BASE_URL/v1/auth/me")
[[ "$code" == "401" ]] || fail "GET /v1/auth/me without a credential -> $code, want 401"
pass "no credential is 401"

# --- /v1/auth/me resolves the token's identity ---------------------------------------
me=$(curl -fsS "$APP_BASE_URL/v1/auth/me" -H "Authorization: Bearer $(finance_token finance)") || fail "auth me"
[[ "$(printf '%s' "$me" | jq -r '.roles[0]')" == "finance" ]] || fail "auth me roles: $me"
[[ "$(printf '%s' "$me" | jq -r '.service')" == "false" ]] || fail "auth me reports the caller as a service principal: $me"
[[ "$(printf '%s' "$me" | jq -r '.permissions | index("input:deliver") != null')" == "true" ]] \
  || fail "finance does not hold input:deliver: $me"
USER_ID=$(printf '%s' "$me" | jq -er '.id')
pass "bearer token resolves to a user holding input:deliver ($USER_ID)"

# The same human resolves to the same user row on a second request.
me2=$(curl -fsS "$APP_BASE_URL/v1/auth/me" -H "Authorization: Bearer $(finance_token finance)") || fail "auth me again"
[[ "$(printf '%s' "$me2" | jq -er '.id')" == "$USER_ID" ]] || fail "identity is not stable across requests"
pass "identity is stable across requests"

# --- the role catalog is readable by a role that grants roles:read -------------------
# finance is not granted roles:read in config.yaml, so it must be refused.
code=$(curl -sS -o /dev/null -w '%{http_code}' "$APP_BASE_URL/v1/roles" -H "Authorization: Bearer $(finance_token finance)")
[[ "$code" == "403" ]] || fail "GET /v1/roles as finance -> $code, want 403 (no roles:read)"
pass "role catalog refuses a role without roles:read"

# --- build a workflow whose input node is closed to finance -------------------------
# The example node lists manager, so finance passes the endpoint gate and is
# then refused by the node gate: the case the second gate exists for.
WORK_DIR=$(mktemp -d)
cat > "$WORK_DIR/workflow.json" <<'JSON'
{
  "name": "e2e-oidc",
  "content": {
    "start_node_id": "11111111-1111-7111-8111-111111111101",
    "nodes": [
      {
        "type": "input",
        "id": "11111111-1111-7111-8111-111111111101",
        "name": "Approval",
        "channel": "http",
        "output_property": "approval",
        "allowed_roles": ["manager"],
        "record_actor": true
      }
    ]
  }
}
JSON

# The definition itself needs definitions:write, which the catalog grants to
# manager but not to finance. Creating it as manager also proves the write
# gate is role-based rather than token-based.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$APP_BASE_URL/v1/workflow/definition" \
  -H "Authorization: Bearer $(finance_token finance)" -H 'Content-Type: application/json' \
  --data-binary "@$WORK_DIR/workflow.json")
[[ "$code" == "403" ]] || fail "create definition as finance -> $code, want 403 (no definitions:write)"
pass "definition write refuses a role without definitions:write"

wf=$(curl -fsS -X POST "$APP_BASE_URL/v1/workflow/definition" \
  -H "Authorization: Bearer $(finance_token manager)" \
  -H 'Content-Type: application/json' \
  --data-binary "@$WORK_DIR/workflow.json") || fail "create workflow definition as manager"
WF_ID=$(printf '%s' "$wf" | jq -er '.id')
pass "created workflow definition $WF_ID"

# --- the second gate: finance holds input:deliver but is not listed on the node ------
inst=$(curl -fsS -X POST "$APP_BASE_URL/v1/workflow/instance" \
  -H "Authorization: Bearer $(finance_token finance)" -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"context\":{}}") || fail "create instance as finance"
INST_ID=$(printf '%s' "$inst" | jq -er '.id')

for _ in $(seq 1 60); do
  status=$(curl -fsS "$APP_BASE_URL/v1/workflow/instance/$INST_ID/status" -H "Authorization: Bearer $(finance_token finance)")
  s=$(printf '%s' "$status" | jq -r '.status')
  reason=$(printf '%s' "$status" | jq -r '.waiting_reason // "none"')
  [[ "$s" == "waiting" && "$reason" == "input" ]] && break
  sleep 1
done
[[ "$s" == "waiting" && "$reason" == "input" ]] || fail "instance did not park on input (status=$s reason=$reason)"
pass "instance waiting on input"

# pending_input tells the frontend about both new fields before it tries.
[[ "$(printf '%s' "$status" | jq -r '.pending_input.allowed_roles[0]')" == "manager" ]] \
  || fail "pending_input does not report allowed_roles: $status"
[[ "$(printf '%s' "$status" | jq -r '.pending_input.record_actor')" == "true" ]] \
  || fail "pending_input does not report record_actor: $status"
pass "pending_input reports allowed_roles and record_actor"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$APP_BASE_URL/v1/workflow/instance/$INST_ID/input" \
  -H "Authorization: Bearer $(finance_token finance)" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: e2e-oidc-1' -d '{"approved":true}')
[[ "$code" == "403" ]] || fail "finance delivery to a manager-only node -> $code, want 403"
pass "second gate refuses finance on a node listing only manager"

# A caller holding no role fails the endpoint gate instead.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$APP_BASE_URL/v1/workflow/instance/$INST_ID/input" \
  -H "Authorization: Bearer $(finance_token noroles)" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: e2e-oidc-2' -d '{"approved":true}')
[[ "$code" == "403" ]] || fail "role-less delivery -> $code, want 403"
pass "first gate refuses a caller with no roles"

# --- the bypass: a valid API token skips both gates -----------------------------------
# The instance is still parked: a refused delivery leaves it waiting.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$APP_BASE_URL/v1/workflow/instance/$INST_ID/input" \
  -H "X-Api-Token: $SIMPWF_API_TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: e2e-oidc-token' -d '{"approved":true,"by":"token"}')
[[ "$code" == "202" ]] || fail "API token delivery -> $code, want 202"
pass "API token bypasses both gates"

ctx=$(curl -fsS "$APP_BASE_URL/v1/workflow/instance/$INST_ID/context" -H "X-Api-Token: $SIMPWF_API_TOKEN")
SYS_ID=$(printf '%s' "$ctx" | jq -er '.context.approval.user_id')
[[ -n "$SYS_ID" ]] || fail "bypassed delivery wrote no user_id: $ctx"
[[ "$(printf '%s' "$ctx" | jq -r '.context.approval.input_data.by')" == "token" ]] \
  || fail "envelope did not carry the raw payload: $ctx"
pass "bypassed delivery is attributed to the system user ($SYS_ID)"

# A fresh instance for the permitted-bearer path.
inst2=$(curl -fsS -X POST "$APP_BASE_URL/v1/workflow/instance" \
  -H "Authorization: Bearer $(finance_token manager)" -H 'Content-Type: application/json' \
  -d "{\"workflow_definition_id\":\"$WF_ID\",\"context\":{}}") || fail "create instance 2"
INST2=$(printf '%s' "$inst2" | jq -er '.id')
for _ in $(seq 1 60); do
  status2=$(curl -fsS "$APP_BASE_URL/v1/workflow/instance/$INST2/status" -H "Authorization: Bearer $(finance_token manager)")
  s2=$(printf '%s' "$status2" | jq -r '.status')
  reason2=$(printf '%s' "$status2" | jq -r '.waiting_reason // "none"')
  [[ "$s2" == "waiting" && "$reason2" == "input" ]] && break
  sleep 1
done
[[ "$s2" == "waiting" && "$reason2" == "input" ]] || fail "instance 2 did not park on input"

delivery=$(curl -fsS -X PUT "$APP_BASE_URL/v1/workflow/instance/$INST2/input" \
  -H "Authorization: Bearer $(finance_token manager)" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: e2e-oidc-3' -d '{"approved":true}') || fail "manager delivery"
[[ "$(printf '%s' "$delivery" | jq -r '.accepted')" == "true" ]] || fail "manager delivery not accepted: $delivery"

ctx2=$(curl -fsS "$APP_BASE_URL/v1/workflow/instance/$INST2/context" -H "Authorization: Bearer $(finance_token manager)")
# The manager's own user id, resolved from the token on this very request.
MGR_ID=$(curl -fsS "$APP_BASE_URL/v1/auth/me" -H "Authorization: Bearer $(finance_token manager)" | jq -er '.id')
[[ "$(printf '%s' "$ctx2" | jq -r '.context.approval.user_id')" == "$MGR_ID" ]] \
  || fail "envelope user_id is not the deliverer: $ctx2"
[[ "$(printf '%s' "$ctx2" | jq -r '.context.approval.input_data.approved')" == "true" ]] \
  || fail "envelope input_data is not the raw payload: $ctx2"
pass "accepted delivery records the deliverer and the raw payload"

echo "e2e-oidc: ALL CHECKS PASSED"
