#!/usr/bin/env python3
"""Mock OIDC provider for the e2e harness.

A real signing key and a real JWKS, so the app performs discovery and
verifies signatures exactly as it would against Zitadel or Keycloak. Nothing
here is a stub: a token minted here is a genuine RS256 JWT.

    python3 scripts/mock_oidc_provider.py <issuer-url>

MOCK_OIDC_HOST overrides the bind address (default 127.0.0.1); the compose
service sets it to 0.0.0.0 so the published port is reachable.

Serves:
    /.well-known/openid-configuration   the discovery document
    /keys                               the JWKS
    /token/<who>                        a pre-minted token for "admin",
                                        "auditor", "finance", "manager",
                                        or "noroles"
"""
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def generate_key():
    """Generate an ephemeral RSA key, so no key material is ever committed."""
    with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as f:
        path = f.name
    subprocess.run(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    with open(path, "rb") as fh:
        return serialization.load_pem_private_key(fh.read(), password=None)


def b64(n: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes((n.bit_length() + 7) // 8, "big")).rstrip(b"=").decode()


def main() -> None:
    issuer = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9099"
    port = int(issuer.rsplit(":", 1)[-1])
    bind = os.environ.get("MOCK_OIDC_HOST", "127.0.0.1")
    key = generate_key()
    pub = key.public_key().public_numbers()

    def mint(claims: dict) -> str:
        header = {"alg": "RS256", "typ": "JWT", "kid": "e2e-key"}

        def seg(obj):
            return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

        signing_input = f"{seg(header)}.{seg(claims)}".encode()
        sig = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        return f"{signing_input.decode()}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"

    now = int(time.time())

    def token(subject, roles):
        return mint({
            "iss": issuer, "aud": "simpwf", "sub": subject, "iat": now - 60, "exp": now + 3600,
            "name": subject, "email": f"{subject}@example.test", "roles": roles,
        })

    tokens = {
        # The shipped catalog's full-access role.
        "admin": token("amy-admin", ["admin"]),
        # The shipped catalog's read-only role.
        "auditor": token("andy-auditor", ["auditor"]),
        # Holds input:deliver but is not the role the example node lists.
        "finance": token("ada-finance", ["finance"]),
        # Holds input:deliver and is the role the example node lists.
        "manager": token("mo-manager", ["manager"]),
        # No roles at all: passes neither gate.
        "noroles": token("nina-nobody", []),
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == "/.well-known/openid-configuration":
                body = json.dumps({
                    "issuer": issuer,
                    "authorization_endpoint": f"{issuer}/authorize",
                    "token_endpoint": f"{issuer}/token",
                    "jwks_uri": f"{issuer}/keys",
                    "id_token_signing_alg_values_supported": ["RS256"],
                    "subject_types_supported": ["public"],
                }).encode()
            elif self.path == "/keys":
                body = json.dumps({"keys": [{
                    "kty": "RSA", "use": "sig", "alg": "RS256", "kid": "e2e-key",
                    "n": b64(pub.n), "e": b64(pub.e),
                }]}).encode()
            elif self.path.startswith("/token/"):
                name = self.path.rsplit("/", 1)[-1]
                if name not in tokens:
                    self.send_error(404)
                    return
                body = tokens[name].encode()
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    HTTPServer((bind, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
