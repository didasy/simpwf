package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// A role name that is blank after trimming names nobody, which is a
// malformed request rather than a missing resource: the role does not exist
// for any caller, so answering 404 would point the client at a lookup that
// could never succeed. The documented 422 is pinned here so the annotation
// and the handler cannot drift apart.
func TestRoleGetBlankNameIsUnprocessable(t *testing.T) {
	// emptyAuthSvc answers exactly what the real service answers for a
	// blank name, so the status under test is the production mapping.
	r := NewRouter(Deps{
		Health: NewHealth(fakePinger{}),
		Auth:   &blankNameAuthSvc{},
		OIDC:   stubVerifier{principal: auth.Principal{Subject: "s1", Issuer: testIssuer, UserID: testSystemUserID, Roles: []string{"auditor"}}},
		Catalog: auth.NewCatalog(map[string][]string{
			"auditor": {auth.ActionRolesRead},
		}),
	})
	for _, name := range []string{"%20", "%20%20%20"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/roles/"+name, nil)
		req.Header.Set(BearerHeader, "Bearer signed")
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("GET /v1/roles/%s status = %d, want 422 (body: %s)", name, w.Code, w.Body.String())
		}
	}
	// An unknown role is still a missing resource, not a malformed request.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/roles/nosuchrole", nil)
	req.Header.Set(BearerHeader, "Bearer signed")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("GET /v1/roles/nosuchrole status = %d, want 404", w.Code)
	}
}

// blankNameAuthSvc mirrors the real service's distinction: a name that is
// blank once trimmed is ErrInvalid, an unknown name is ErrNotFound. The
// service test covers the real trim; this pins the status each maps to.
type blankNameAuthSvc struct{ fakeAuthSvc }

func (b *blankNameAuthSvc) GetRole(_ context.Context, name string) (model.Role, []string, error) {
	if strings.TrimSpace(name) == "" {
		return model.Role{}, nil, fmt.Errorf("%w: role name is required", model.ErrInvalid)
	}
	return b.fakeAuthSvc.GetRole(context.Background(), name)
}
