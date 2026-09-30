package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// anonRouter wires a router with authentication on and a delivery the
// service accepts, so these tests observe only the routing and the principal
// the handler threaded through. The service's own public-versus-private
// decision is covered in the service package.
func anonRouter(svc *fakeInstanceSvc) *gin.Engine {
	return NewRouter(Deps{
		Health:       NewHealth(fakePinger{}),
		Instances:    svc,
		Auth:         &fakeAuthSvc{},
		AuthSettings: authConfigForTest(),
		Catalog: auth.NewCatalog(map[string][]string{
			"finance": {auth.ActionInputDeliver},
		}),
		SystemUserID: testSystemUserID,
	})
}

func putInput(r *gin.Engine, headers map[string]string) *httptest.ResponseRecorder {
	h := map[string]string{"Idempotency-Key": "key-1"}
	for k, v := range headers {
		h[k] = v
	}
	return performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/input", `{"ok":true}`, h)
}

// A public input node is the one caller the global authentication gate does
// not stop: with no credential at all the request still reaches the handler
// and is threaded through as anonymous, which is what the service needs in
// order to admit it on a public node.
func TestAnonymousPutInputReachesTheHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	r := anonRouter(svc)

	w := putInput(r, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if !svc.deliverReq.Anonymous {
		t.Error("anonymous = false, want the credential-less delivery flagged anonymous")
	}
	if svc.deliverReq.Principal != nil {
		t.Errorf("principal = %+v, want nil: nothing authenticated", svc.deliverReq.Principal)
	}
	if svc.deliverReq.Actor != "" {
		t.Errorf("actor = %q, want empty: an anonymous delivery attributes nobody", svc.deliverReq.Actor)
	}
}

// A credential that is present but wrong is refused before the handler runs,
// even though the route serves anonymous callers. An invalid credential must
// never quietly become an accepted anonymous delivery.
func TestInvalidCredentialOnPutInputIsUnauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for name, headers := range map[string]map[string]string{
		"wrong api token":  {APIKeyHeader: "not-the-token"},
		"wrong bearer":     {BearerHeader: "Bearer not-a-token"},
		"malformed bearer": {BearerHeader: "signed"},
		"wrong scheme":     {BearerHeader: "Basic signed"},
	} {
		t.Run(name, func(t *testing.T) {
			svc := &fakeInstanceSvc{delivery: &model.InputDelivery{Accepted: true}}
			w := putInput(anonRouter(svc), headers)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (%s)", w.Code, w.Body.String())
			}
			if svc.deliverReq != nil {
				t.Error("DeliverInput was called, want the request refused before the handler")
			}
		})
	}
}

// A header carrying nothing but whitespace is not a credential: no client
// means anything by it, so on the route that serves anonymous callers it is
// an anonymous request rather than a refused one. A header with any content
// in it is a credential and is checked, as the case above pins.
func TestBlankCredentialOnPutInputIsAnonymous(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	w := putInput(anonRouter(svc), map[string]string{APIKeyHeader: "   "})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil || !svc.deliverReq.Anonymous {
		t.Errorf("deliverReq = %+v, want an anonymous delivery", svc.deliverReq)
	}
}

// A valid credential on the same route is authenticated and attributed
// normally: the public-node path does not swallow the identity of a caller
// that did authenticate.
func TestAuthenticatedPutInputStaysAttributed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	w := putInput(anonRouter(svc), map[string]string{APIKeyHeader: "test-token"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if svc.deliverReq.Anonymous {
		t.Error("anonymous = true, want false for a caller that presented a valid credential")
	}
	if svc.deliverReq.Principal == nil || !svc.deliverReq.Principal.Service {
		t.Errorf("principal = %+v, want the service principal", svc.deliverReq.Principal)
	}
	if svc.deliverReq.Actor != testSystemUserID {
		t.Errorf("actor = %q, want the system user %q", svc.deliverReq.Actor, testSystemUserID)
	}
}

// Exempting PUT input must not exempt anything else. The status and form
// reads stay behind the global gate, so an anonymous caller cannot use a
// public node to discover the workflow's shape; it learns the contract
// out-of-band instead.
func TestAnonymousReadsOtherInstanceRoutesAreUnauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := anonRouter(&fakeInstanceSvc{
		detail: &service.StatusDetail{Instance: model.WorkflowInstance{ID: instanceID}},
	})
	for _, path := range []string{
		"/v1/workflow/instance",
		"/v1/workflow/instance/" + instanceID + "/status",
		"/v1/workflow/instance/" + instanceID + "/status/node/" + occurrenceID,
		"/v1/workflow/instance/" + instanceID + "/context",
		"/v1/auth/me",
	} {
		w := performJSON(r, http.MethodGet, path, "", nil)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("GET %s status = %d, want 401 (%s)", path, w.Code, w.Body.String())
		}
	}
	w := performJSON(r, http.MethodPut, "/v1/workflow/instance/"+instanceID+"/context", `{}`, nil)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("PUT context status = %d, want 401", w.Code)
	}
	// Creating an instance is a write an anonymous caller must not reach.
	create := performJSON(r, http.MethodPost, "/v1/workflow/instance", `{}`, nil)
	if create.Code != http.StatusUnauthorized {
		t.Errorf("POST instance status = %d, want 401", create.Code)
	}
	for _, path := range []string{
		"/v1/workflow/instance/" + instanceID + "/pause",
		"/v1/workflow/instance/" + instanceID + "/resume",
		"/v1/workflow/instance/" + instanceID + "/stop",
		"/v1/workflow/instance/" + instanceID + "/rollback",
	} {
		w := performJSON(r, http.MethodPost, path, `{}`, nil)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("POST %s status = %d, want 401", path, w.Code)
		}
	}
}

// With authentication off there is no optional auth in play at all, so a
// delivery is not marked anonymous: it keeps the trusted service-principal
// meaning a missing principal always had in an unauthenticated deployment.
func TestPutInputWithoutAuthIsNotMarkedAnonymous(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{delivery: &model.InputDelivery{ID: occurrenceID, Accepted: true}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Instances: svc})
	w := putInput(r, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", w.Code, w.Body.String())
	}
	if svc.deliverReq == nil {
		t.Fatal("DeliverInput was not called")
	}
	if svc.deliverReq.Anonymous {
		t.Error("anonymous = true, want false when authentication is off")
	}
	if svc.deliverReq.Principal != nil {
		t.Errorf("principal = %+v, want nil when authentication is off", svc.deliverReq.Principal)
	}
}

// A service refusal on the anonymous route keeps the existing status mapping:
// the service is what decides public versus private, and a private node
// answers 403 the same way it does for an authenticated caller.
func TestAnonymousPutInputMapsServiceRefusal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeInstanceSvc{deliveryErr: model.ErrForbidden}
	w := putInput(anonRouter(svc), nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (%s)", w.Code, w.Body.String())
	}
	var p Problem
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Detail == "" {
		t.Error("problem detail is empty, want the service reason")
	}
}
