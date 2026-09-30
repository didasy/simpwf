package handler

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/configuration"
)

// TestInputThreadTheServicePrincipal is the input path's counterpart to
// TestControlsThreadTheServicePrincipal. Delivery is the one write whose
// caller is recorded in the workflow context, so a handler that dropped the
// principal would still return 202 and the run would simply record nobody.
func TestInputThreadTheServicePrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	r := NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Instances:    svc,
		Auth:         &fakeAuthSvc{},
		AuthSettings: configuration.Auth{Enabled: true, APIToken: "secret"},
		Catalog:      auth.NewCatalog(map[string][]string{"op": {"*"}}),
		SystemUserID: testSystemUserID,
	})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{"approved":true}`, map[string]string{
			APIKeyHeader:      "secret",
			"Idempotency-Key": "input-1",
			"Content-Type":    "application/json",
		})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if svc.deliverReq.Principal == nil {
		t.Error("principal = nil, want the authenticated service principal")
	} else if !svc.deliverReq.Principal.Service {
		t.Errorf("principal.Service = false, want true for an API token")
	}
	if svc.deliverReq.Actor != testSystemUserID {
		t.Errorf("actor = %q, want the system user %q", svc.deliverReq.Actor, testSystemUserID)
	}
	if svc.deliverReq.IdempotencyKey != "input-1" {
		t.Errorf("idempotency key = %q, want input-1", svc.deliverReq.IdempotencyKey)
	}
}

// A bearer token is a human, so the delivery must carry that human's own
// id rather than the system user the API token maps to. This is the path
// the role gate and the attribution envelope both read.
func TestInputThreadTheBearerPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const userID = "44444444-4444-7444-8444-444444444444"
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	r := NewRouter(Deps{
		Health:    NewHealth(fakePinger{}),
		Instances: svc,
		Auth:      &fakeAuthSvc{},
		OIDC: stubVerifier{principal: auth.Principal{
			Subject: "ada-1", Issuer: testIssuer, UserID: userID, Roles: []string{"finance"},
		}},
		Catalog: auth.NewCatalog(map[string][]string{"finance": {auth.ActionInputDeliver}}),
	})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{"approved":true}`, map[string]string{
			BearerHeader:      "Bearer signed",
			"Idempotency-Key": "input-2",
			"Content-Type":    "application/json",
		})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if svc.deliverReq.Principal == nil {
		t.Fatal("principal = nil, want the bearer principal")
	}
	if svc.deliverReq.Principal.Service {
		t.Error("principal.Service = true, want false for a human caller")
	}
	if svc.deliverReq.Principal.Subject != "ada-1" {
		t.Errorf("principal.Subject = %q, want ada-1", svc.deliverReq.Principal.Subject)
	}
	if svc.deliverReq.Actor != userID {
		t.Errorf("actor = %q, want the caller's own id %q", svc.deliverReq.Actor, userID)
	}
}

// With authentication off there is no principal at all, and the services
// read a nil principal as the service principal. A handler that invented an
// empty-but-present principal here would change what an unauthenticated
// deployment means.
func TestInputWithoutAuthLeavesThePrincipalNil(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	r := NewRouter(Deps{
		Health:    NewHealth(fakePinger{}),
		Instances: svc,
	})
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input",
		`{"approved":true}`, map[string]string{
			"Idempotency-Key": "input-3",
			"Content-Type":    "application/json",
		})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if svc.deliverReq.Principal != nil {
		t.Errorf("principal = %+v, want nil when authentication is off", svc.deliverReq.Principal)
	}
	if svc.deliverReq.Actor != "" {
		t.Errorf("actor = %q, want empty when authentication is off", svc.deliverReq.Actor)
	}
}
