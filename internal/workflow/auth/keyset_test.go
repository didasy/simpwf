package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

// jwksServer is a mock provider endpoint that publishes one RSA key and
// counts how often it was read, so a test can assert on outbound traffic
// rather than only on the verification result.
type jwksServer struct {
	url     string
	keyID   string
	fetches int64
	// server is kept so a test can shut the provider down mid-run to
	// simulate an outage.
	server *httptest.Server

	mu  sync.RWMutex
	key *rsa.PrivateKey
}

// snapshot is a consistent read of the published key and its id. Reading
// the two fields under one lock is what lets a rotation that changes both
// stay in step with the token signed from that same pair.
func (s *jwksServer) snapshot() (*rsa.PrivateKey, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.key, s.keyID
}

func newJWKSServer(t *testing.T) *jwksServer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	s := &jwksServer{keyID: "kid-1", key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&s.fetches, 1)
		key, keyID := s.snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA",
				"use": "sig",
				"alg": "RS256",
				"kid": keyID,
				"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
			}},
		})
	})
	srv := httptest.NewServer(mux)
	s.server = srv
	t.Cleanup(srv.Close)
	s.url = srv.URL + "/keys"
	return s
}

// rotate swaps the published key material while keeping the key id, the way
// a provider re-signs with a refreshed key pair.
func (s *jwksServer) rotate(t *testing.T) {
	t.Helper()
	s.rotateTo(t, s.keyID)
}

// rotateTo publishes new key material under a new key id, the way a
// provider adds a key rather than replacing one in place.
func (s *jwksServer) rotateTo(t *testing.T, keyID string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rotated key: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.key = key
	s.keyID = keyID
}

func (s *jwksServer) fetchCount() int64 { return atomic.LoadInt64(&s.fetches) }

func (s *jwksServer) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	key, keyID := s.snapshot()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: keyID}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	object, err := signer.Sign(payload)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	signed, err := object.CompactSerialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	return signed
}

// rogueToken signs with a key the provider never published, so verification
// fails at the signature step specifically.
func rogueToken(t *testing.T, keyID string, claims map[string]any) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rogue key: %v", err)
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: keyID}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	object, err := signer.Sign(payload)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	signed, err := object.CompactSerialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	return signed
}

// testClock is a hand-wound clock so a test can cross the refetch window
// without sleeping through it.
type testClock struct{ at time.Time }

func newTestClock() *testClock { return &testClock{at: time.Now()} }

func (c *testClock) Now() time.Time          { return c.at }
func (c *testClock) Advance(d time.Duration) { c.at = c.at.Add(d) }

// TestBadTokensDoNotAmplifyToProviderRequests is the reason ttlKeySet
// exists. go-oidc's own remote key set re-reads the JWKS on every failed
// verification, so anyone who can reach the API can force the identity
// provider to serve unbounded traffic. A rejected request must not become a
// request to the provider.
func TestBadTokensDoNotAmplifyToProviderRequests(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}

	// A good token warms the cache.
	if _, err := ks.VerifySignature(ctx, srv.sign(t, claims)); err != nil {
		t.Fatalf("VerifySignature(good) error = %v", err)
	}
	warm := srv.fetchCount()
	if warm != 1 {
		t.Fatalf("JWKS fetches after warm-up = %d, want 1", warm)
	}

	// A flood of unverifiable tokens must not reach the provider at all.
	for i := 0; i < 25; i++ {
		if _, err := ks.VerifySignature(ctx, rogueToken(t, srv.keyID, claims)); err == nil {
			t.Fatal("VerifySignature(rogue) succeeded")
		}
	}
	if got := srv.fetchCount(); got != warm {
		t.Fatalf("JWKS fetches = %d, want %d: rejected tokens reached the provider", got, warm)
	}
}

// TestConcurrentVerificationOnAStaleCacheCollapsesToOneFetch: a burst of
// requests arriving together on an expired cache must produce one outbound
// fetch, not one per request.
func TestConcurrentVerificationOnAStaleCacheCollapsesToOneFetch(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}
	token := srv.sign(t, claims)

	if _, err := ks.VerifySignature(ctx, token); err != nil {
		t.Fatalf("VerifySignature() error = %v", err)
	}
	before := srv.fetchCount()

	// Age the cache, then have everyone arrive at once.
	clk.Advance(2 * time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ks.VerifySignature(ctx, token); err != nil {
				t.Errorf("VerifySignature() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if got := srv.fetchCount() - before; got != 1 {
		t.Fatalf("JWKS fetches after the window = %d, want 1", got)
	}
}

// TestRefetchAfterTheWindowStillPicksUpARotatedKey keeps the bound from
// becoming a security regression: a key the provider has withdrawn must
// stop working once the TTL passes. The refresh is proactive, so this does
// not even need a failing request to notice the rotation.
func TestRefetchAfterTheWindowStillPicksUpARotatedKey(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}
	oldToken := srv.sign(t, claims)

	if _, err := ks.VerifySignature(ctx, oldToken); err != nil {
		t.Fatalf("VerifySignature(old key) error = %v", err)
	}

	// The provider re-signs with new material, same kid, and withdraws the
	// old public key.
	srv.rotate(t)

	// Inside the window the cached set is authoritative, so the old key
	// still verifies. That is what a cache is for.
	if _, err := ks.VerifySignature(ctx, oldToken); err != nil {
		t.Fatalf("VerifySignature() inside the window error = %v, want the cached key to still verify", err)
	}

	// Past the window the keys are re-read, and the withdrawn key is
	// refused without anyone having to submit a bad token first.
	clk.Advance(2 * time.Hour)
	if _, err := ks.VerifySignature(ctx, oldToken); err == nil {
		t.Fatal("VerifySignature() past the window accepted a withdrawn key")
	}
	if srv.fetchCount() < 2 {
		t.Fatalf("JWKS fetches = %d, want a refetch past the window", srv.fetchCount())
	}

	// The new material is accepted, because the refresh already happened as
	// part of the same call.
	if _, err := ks.VerifySignature(ctx, srv.sign(t, claims)); err != nil {
		t.Fatalf("VerifySignature(new key) error = %v", err)
	}
}

// TestRefetchIntervalIsAFloor: a configured TTL below the floor cannot make
// the engine re-read the JWKS more aggressively than the guard allows.
func TestRefetchIntervalIsAFloor(t *testing.T) {
	ks := newTTLKeySet("http://127.0.0.1:1/never", time.Second)
	if ks.ttl != minKeySetTTL {
		t.Fatalf("ttl = %v, want the %v floor", ks.ttl, minKeySetTTL)
	}
	wide := newTTLKeySet("http://127.0.0.1:1/never", 2*time.Hour)
	if wide.ttl != 2*time.Hour {
		t.Fatalf("ttl = %v, want the configured 2h", wide.ttl)
	}
}

// TestUnreachableProviderKeepsServingCachedKeys: a provider outage must not
// take authentication down for everyone holding a still-valid token.
func TestUnreachableProviderKeepsServingCachedKeys(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}
	token := srv.sign(t, claims)

	if _, err := ks.VerifySignature(ctx, token); err != nil {
		t.Fatalf("VerifySignature() error = %v", err)
	}

	// The provider disappears and the window passes, so the refresh that
	// now runs will fail.
	srv.server.Close()
	clk.Advance(2 * time.Hour)
	if _, err := ks.VerifySignature(ctx, rogueToken(t, srv.keyID, claims)); err == nil {
		t.Fatal("VerifySignature(rogue) succeeded against an unreachable provider")
	}
	// The good token still verifies on the cached keys, so an outage does
	// not take authentication down for everyone holding a valid token.
	if _, err := ks.VerifySignature(ctx, token); err != nil {
		t.Fatalf("VerifySignature() with the provider down error = %v, want the cached keys to still work", err)
	}
}

// TestMalformedTokenIsRejectedWithoutFetching: parsing fails before any key
// lookup, so a syntactically broken token must not cost a provider request.
func TestMalformedTokenIsRejectedWithoutFetching(t *testing.T) {
	srv := newJWKSServer(t)
	ks := newTTLKeySetWithClock(srv.url, time.Hour, newTestClock().Now)
	if _, err := ks.VerifySignature(context.Background(), "not-a-jwt"); err == nil {
		t.Fatal("VerifySignature(garbage) succeeded")
	}
	if got := srv.fetchCount(); got != 0 {
		t.Fatalf("JWKS fetches = %d, want 0 for an unparseable token", got)
	}
}

// TestUnknownKidPicksUpARotatedKeyWithoutWaitingForTheTTL: a provider that
// publishes a new key id mid-window would otherwise refuse every token
// signed with it until the cache aged out. One throttled re-read turns that
// window of refusals into a single extra fetch.
func TestUnknownKidPicksUpARotatedKeyWithoutWaitingForTheTTL(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}

	if _, err := ks.VerifySignature(ctx, srv.sign(t, claims)); err != nil {
		t.Fatalf("VerifySignature(warm-up) error = %v", err)
	}
	warm := srv.fetchCount()

	// The provider rotates to a new key id, still well inside the window.
	srv.rotateTo(t, "kid-2")
	token := srv.sign(t, claims)
	if _, err := ks.VerifySignature(ctx, token); err != nil {
		t.Fatalf("VerifySignature(rotated key) error = %v, want the key picked up without waiting for the TTL", err)
	}
	if got := srv.fetchCount() - warm; got != 1 {
		t.Errorf("JWKS fetches after the rotation = %d, want exactly one re-read", got)
	}
	// The retry is not repeated per request: the second call rides the
	// refreshed keys.
	if _, err := ks.VerifySignature(ctx, token); err != nil {
		t.Errorf("VerifySignature(rotated key, second call) error = %v", err)
	}
	if got := srv.fetchCount() - warm; got != 1 {
		t.Errorf("JWKS fetches after the second call = %d, want still 1", got)
	}
}

// A key id no cached key holds is what a rotation looks like, so it earns
// the re-read. A token that names a key the cache does hold but fails
// verification does not: that is a forged or withdrawn token, and giving it
// a second chance would spend a provider request on every one of them.
func TestRotationRereadIsLimitedToUnknownKids(t *testing.T) {
	srv := newJWKSServer(t)
	clk := newTestClock()
	ks := newTTLKeySetWithClock(srv.url, time.Hour, clk.Now)
	ctx := context.Background()
	claims := map[string]any{"sub": "user-1"}

	if _, err := ks.VerifySignature(ctx, srv.sign(t, claims)); err != nil {
		t.Fatalf("VerifySignature(warm-up) error = %v", err)
	}
	warm := srv.fetchCount()

	// The kid is one the cache holds, but the material is a rogue key.
	if _, err := ks.VerifySignature(ctx, rogueToken(t, "kid-1", claims)); err == nil {
		t.Fatal("VerifySignature(rogue key, known kid) succeeded")
	}
	if got := srv.fetchCount() - warm; got != 0 {
		t.Errorf("JWKS fetches after a bad signature on a known kid = %d, want 0", got)
	}

	// An unknown kid does earn the re-read, and it is throttled: the whole
	// flood still costs one fetch.
	for i := 0; i < 10; i++ {
		if _, err := ks.VerifySignature(ctx, rogueToken(t, "kid-unknown", claims)); err == nil {
			t.Fatal("VerifySignature(rogue key, unknown kid) succeeded")
		}
	}
	if got := srv.fetchCount() - warm; got != 1 {
		t.Errorf("JWKS fetches after the unknown-kid flood = %d, want 1", got)
	}
}

// A JWS carrying no signature has nothing to verify. The length is what the
// verify path indexes into, so it is checked rather than assumed.
func TestZeroSignatureTokenIsRejectedWithoutPanicking(t *testing.T) {
	srv := newJWKSServer(t)
	ks := newTTLKeySetWithClock(srv.url, time.Hour, newTestClock().Now)
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-1"}`))
	// A detached-shaped object: a payload, and no signature over it.
	detached := `{"payload":"` + payload + `","signatures":[]}`

	_, err := ks.VerifySignature(context.Background(), detached)
	if err == nil {
		t.Fatal("VerifySignature(no signatures) succeeded, want a rejection")
	}
	if got := srv.fetchCount(); got != 0 {
		t.Errorf("JWKS fetches = %d, want 0: an unverifiable token reached the provider", got)
	}
}

// The parser's accept list is asymmetric only. An HS* token is verified with
// a shared secret the caller supplies, so accepting one would mean verifying
// a token against a key the attacker chose.
func TestHMACSignedTokensAreRejectedAtParse(t *testing.T) {
	srv := newJWKSServer(t)
	ks := newTTLKeySetWithClock(srv.url, time.Hour, newTestClock().Now)
	secret := make([]byte, 64) // long enough for HS384 and HS512
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("read secret: %v", err)
	}
	for _, alg := range []jose.SignatureAlgorithm{jose.HS256, jose.HS384, jose.HS512} {
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: secret}, nil)
		if err != nil {
			t.Fatalf("new signer (%v): %v", alg, err)
		}
		object, err := signer.Sign([]byte(`{"sub":"user-1"}`))
		if err != nil {
			t.Fatalf("sign (%v): %v", alg, err)
		}
		token, err := object.CompactSerialize()
		if err != nil {
			t.Fatalf("serialize (%v): %v", alg, err)
		}
		if _, err := ks.VerifySignature(context.Background(), token); err == nil {
			t.Errorf("VerifySignature(%v) succeeded, want the symmetric algorithm refused at parse", alg)
		}
	}
	if got := srv.fetchCount(); got != 0 {
		t.Errorf("JWKS fetches = %d, want 0: an unparseable token reached the provider", got)
	}
}
