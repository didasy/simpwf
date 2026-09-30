# Zitadel OIDC for SimpWF (API-only, machine user)

SimpWF is OIDC resource server only. No callback, no session, no client secret on SimpWF side. Frontend or script runs code+PKCE (human) or client credentials (machine), calls API with `Authorization: Bearer <jwt>`. SimpWF validates signature, `iss`, `aud`, `exp` via discovery + JWKS.

This guide covers machine-user path for API testing. Human browser login uses same issuer/client config; token carries same `roles` claim.

## 1. Zitadel setup

Project `SimpWF`:

1. Roles → New `admin`. Spelling must match SimpWF catalog keys. Unknown roles deny by default.
2. Users → New Service Account (e.g. `simpwf_admin`) → Actions → Generate Client Secret. Save numeric ClientID + secret. One-time view. Rotate if leaked.
3. Same user → Token Type = **JWT**. Default opaque token fails SimpWF signature check.
4. Project → Authorizations → authorize service account → role `admin`. Same org as project.
5. Project → Settings → check Assert Roles on Authentication (labeled Return User roles during authentication in some versions).

No API application needed for this path. Service account mints token directly. API app ID only matters as audience alternative.

## 2. Scopes (exact names)

Reserved scope names are exact. Singular `urn:zitadel:iam:org:project:roles` is not valid scope; Zitadel silently ignores unknown scopes and token comes back bare.

Mint with:

```
scope=openid profile email urn:zitadel:iam:org:project:id:<PROJECTID>:aud urn:zitadel:iam:org:projects:roles
```

- `...:aud` puts project ID in `aud`.
- `...projects:roles` (plural) emits per-project claim `urn:zitadel:iam:org:project:<PROJECTID>:roles`.

```bash
curl -s -X POST http://zitadel.local:28080/oauth/v2/token \
  -u "CLIENT_ID:SECRET" \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode "scope=openid profile email urn:zitadel:iam:org:project:id:<PROJECTID>:aud urn:zitadel:iam:org:projects:roles" \
| tee token.json
jq -r .access_token token.json | cut -d. -f2 | base64 -d 2>/dev/null | jq .
```

Good payload:

```json
{
  "iss": "http://zitadel.local:28080",
  "aud": ["<PROJECTID>"],
  "client_id": "simpwf_admin",
  "urn:zitadel:iam:org:project:<PROJECTID>:roles": {
    "admin": { "<ORGID>": "<domain>" }
  }
}
```

Bare token (no `urn:zitadel:*`) = missing grant, unchecked assert flag, or wrong scope name. Check in that order.

## 3. Flat `roles` claim via Action

SimpWF `roleClaim` parser reads array, single string, or space-delimited string. Zitadel native claim is nested object → SimpWF returns nil → 403. Add Complement Token Action emitting flat `roles`.

Action rules (Zitadel goja engine, ES5 only):

- Action Name must equal function name. Mismatch → `server_error`, `"function not found"` in logs.
- No `?.`, no arrow functions. Use `var` + `function()`.

```js
function flatRoles(ctx, api) {
  var grants = [];
  try {
    grants = ctx.v1.user.grants.grants || [];
  } catch (e) {
    return;
  }
  var roles = [];
  grants.forEach(function(g) {
    (g.roles || []).forEach(function(r) {
      if (roles.indexOf(r) === -1) roles.push(r);
    });
  });
  if (roles.length) api.v1.claims.setClaim('roles', roles);
}
```

Flows → Complement Token → attach to **both** triggers: Pre Access Token Creation, Pre Userinfo Creation. Save → Deploy (required after every edit). Re-mint. Expect both claims:

```json
{
  "urn:zitadel:iam:org:project:<PROJECTID>:roles": { "admin": {...} },
  "roles": ["admin"]
}
```

`roles` is what SimpWF reads. URN is proof grant works.

`server_error` + `function not found` right after adding Action = name mismatch or ES6 syntax. Fix name, redeploy.

## 4. Shared domain (host + container must agree)

SimpWF verifies `iss` strict via `oidc.NewProvider` + `NewVerifier`. Issuer string must equal token `iss` and be fetchable from inside app container. `localhost` inside container = itself, so discovery fails at boot.

Use shared name both sides resolve. `host.docker.internal` conflicts with Docker Desktop's own hosts entry on Windows; `zitadel.local` avoids fight.

Hosts entries:

```
# Windows C:\Windows\System32\drivers\etc\hosts (Notepad as Admin)
127.0.0.1 zitadel.local
# WSL /etc/hosts
127.0.0.1 zitadel.local
```

`zitadel.docker-compose.yml`:

```yaml
ZITADEL_EXTERNALDOMAIN: zitadel.local
ZITADEL_DEFAULTINSTANCE_FEATURES_LOGINV2_BASEURI: http://zitadel.local:28080/ui/v2/login/
ZITADEL_OIDC_DEFAULTLOGINURLV2: http://zitadel.local:28080/ui/v2/login/login?authRequest=
ZITADEL_OIDC_DEFAULTLOGOUTURLV2: http://zitadel.local:28080/ui/v2/login/logout?post_logout_redirect=
# zitadel-login service:
CUSTOM_REQUEST_HEADERS: Host:zitadel.local:28080,X-Forwarded-Proto:http
```

Domain pins at instance creation. Changing env on existing instance gives `Instance not found`. Fresh re-init required:

```bash
docker compose -f zitadel.docker-compose.yml down -v
docker compose -f zitadel.docker-compose.yml up -d
```

Then recreate role, service account, grant, flag, Action. Console: `http://zitadel.local:28080/ui/console`. Stale login cookies after re-init cause `Auth Request does not exist`; clear site cookies or use fresh tab.

Verify before SimpWF:

```bash
curl -s http://zitadel.local:28080/.well-known/openid-configuration | jq -r .issuer
# want exactly future SIMPWF_AUTH_OIDC_ISSUER
```

Chrome notes: unknown names need `http://` scheme prefix. Secure DNS (DoH) bypasses hosts file; toggle off for test. `chrome://net-internals/#dns` clear cache. Corporate proxy needs bypass entry.

## 5. SimpWF compose

`docker-compose.yml` `app.environment` already parameterized via `${VAR:-default}`. Env export needs zero edits; hardcode works too.

```bash
export SIMPWF_AUTH_OIDC_ENABLED=true
export SIMPWF_AUTH_OIDC_ISSUER="http://zitadel.local:28080"
export SIMPWF_AUTH_OIDC_CLIENT_ID="<numeric-machine-user-client-ID>"
export SIMPWF_AUTH_OIDC_ROLES_CLAIM="roles"
export SIMPWF_AUTH_OIDC_USERNAME_CLAIM="client_id"
docker compose up --build
```

Notes:

- `CLIENT_ID` = numeric service-account client ID, not display name.
- `AUDIENCE` empty → falls back to client ID. Zitadel `aud` includes it.
- `ROLES_CLAIM: roles` = custom Action claim, not Zitadel native URN.
- `USERNAME_CLAIM: client_id` recommended for machine tokens: they carry no `name`/`preferred_username`/`email`, so default chain falls back to numeric `sub`. `client_id` renders readable name in `/v1/auth/me` and audit trail.
- `AUTH_ENABLED: false` = pure OIDC test. Set `true` (+ non-empty token) for `X-Api-Token` bypass alongside.
- Default `SIMPWF_AUTH_ROLE_PERMISSIONS` already grants `admin` full catalog.

Container must resolve issuer. Add to `app` service, sibling of `ports`/`depends_on` (not under `environment`):

```yaml
extra_hosts:
  - "zitadel.local:host-gateway"
```

Missing this = boot fails `oidc discovery`. Issuer string must match token `iss` byte-for-byte, no trailing slash.

## 6. Verify

```bash
curl http://localhost:8080/v1/auth/config
curl http://localhost:8080/v1/auth/me -H "Authorization: Bearer $TOKEN"
# want roles ["admin"], service false
curl -X POST http://localhost:8080/v1/workflow/definition \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data-binary @workflow.json
```

401 = bad/expired token or issuer unreachable at boot (check logs for `oidc discovery`). 403 = token valid, role missing from `SIMPWF_AUTH_ROLE_PERMISSIONS` or lacking route action.
