package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// fakeStatisticsSvc is an in-memory statistics service for handler tests.
type fakeStatisticsSvc struct {
	summaryQuery     repository.StatisticsQuery
	summaryOrder     string
	summaryPrincipal auth.Principal
	summaryRes       repository.StatisticsResult
	summaryErr       error
}

func (f *fakeStatisticsSvc) Summary(_ context.Context, q repository.StatisticsQuery, p auth.Principal) (repository.StatisticsResult, error) {
	f.summaryQuery = q
	f.summaryOrder = q.Order
	f.summaryPrincipal = p
	return f.summaryRes, f.summaryErr
}

func mustRFC3339(t *testing.T, s string) string {
	t.Helper()
	if _, err := time.Parse(time.RFC3339, s); err != nil {
		t.Fatalf("bad fixture time %q: %v", s, err)
	}
	return s
}

func TestStatisticsSummaryDelegatesParsedQuery(t *testing.T) {
	svc := &fakeStatisticsSvc{summaryRes: repository.StatisticsResult{
		TotalRuns: 9, ActiveRuns: 7,
		RunsPerDay: []repository.RunsPerDayPoint{},
	}}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: svc})
	from := mustRFC3339(t, "2026-09-01T00:00:00Z")
	to := mustRFC3339(t, "2026-09-20T00:00:00Z")
	w := performJSON(r, http.MethodGet,
		"/v1/statistics?created_from="+from+"&created_to="+to+"&order=-date", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	if svc.summaryQuery.From == nil || svc.summaryQuery.To == nil {
		t.Fatalf("query window = %+v, want both bounds", svc.summaryQuery)
	}
	if got := svc.summaryQuery.From.Format(time.RFC3339); !strings.HasPrefix(got, "2026-09-01") {
		t.Errorf("from = %v", got)
	}
	if svc.summaryOrder != "-date" {
		t.Errorf("order = %q, want -date", svc.summaryOrder)
	}
	var resp StatisticsSummaryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalRuns != 9 || resp.ActiveRuns != 7 {
		t.Errorf("total/active = %d/%d, want 9/7", resp.TotalRuns, resp.ActiveRuns)
	}
	if len(resp.RunsPerDay) != 0 {
		t.Errorf("runs_per_day = %+v, want empty", resp.RunsPerDay)
	}
}

func TestStatisticsSummaryDefaults(t *testing.T) {
	svc := &fakeStatisticsSvc{}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: svc})
	w := performJSON(r, http.MethodGet, "/v1/statistics", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	if svc.summaryQuery.From != nil || svc.summaryQuery.To != nil {
		t.Errorf("window = %+v, want nil/nil", svc.summaryQuery)
	}
	if svc.summaryOrder != "-date" {
		t.Errorf("order = %q, want default -date", svc.summaryOrder)
	}
}

// A Bearer [REDACTED] is scoped to their own runs, so the handler must thread
// the authenticated principal through to the service. A handler that dropped
// it would still return 200 and the aggregate would silently cover every row.
func TestStatisticsSummaryThreadsTheBearerPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const userID = "44444444-4444-7444-8444-444444444444"
	svc := &fakeStatisticsSvc{}
	r := NewRouter(Deps{
		Health:     NewHealth(fakePinger{}),
		Statistics: svc,
		Auth:       &fakeAuthSvc{},
		OIDC: stubVerifier{principal: auth.Principal{
			Subject: "ada-1", Issuer: testIssuer, UserID: userID, Roles: []string{"auditor"},
		}},
		Catalog: auth.NewCatalog(map[string][]string{"auditor": {auth.ActionStatisticsRead}}),
	})
	w := performJSON(r, http.MethodGet, "/v1/statistics", "", map[string]string{
		BearerHeader: "Bearer [REDACTED]",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	if svc.summaryPrincipal.UserID != userID {
		t.Errorf("principal.UserID = %q, want %q", svc.summaryPrincipal.UserID, userID)
	}
	if svc.summaryPrincipal.Service {
		t.Error("principal.Service = true, want false for a human caller")
	}
}

// With authentication off there is no principal at all, and the service
// reads the zero principal as authentication disabled (unscoped).
func TestStatisticsSummaryWithoutAuthYieldsZeroPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeStatisticsSvc{}
	r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: svc})
	w := performJSON(r, http.MethodGet, "/v1/statistics", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	if p := svc.summaryPrincipal; p.UserID != "" || p.Service || len(p.Roles) != 0 {
		t.Errorf("principal = %+v, want zero when authentication is off", p)
	}
}

func TestStatisticsSummaryInvalidQuery(t *testing.T) {
	for _, suffix := range []string{
		"?created_from=not-a-date",
		"?created_to=2026-13-99",
		"?created_from=2026-09-20T00:00:00Z&created_to=2026-09-01T00:00:00Z",
		"?order=total",
	} {
		r := NewRouter(Deps{Health: NewHealth(fakePinger{}), Statistics: &fakeStatisticsSvc{}})
		w := performJSON(r, http.MethodGet, "/v1/statistics"+suffix, "", nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400", suffix, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
			t.Errorf("%s content-type = %q, want application/problem+json", suffix, ct)
		}
	}
}
