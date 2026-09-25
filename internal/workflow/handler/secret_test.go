package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

type fakeSecretSvc struct {
	created    repository.Secret
	createErr  error
	get        repository.Secret
	getErr     error
	items      []repository.Secret
	total      int64
	listErr    error
	deleteErr  error
	lastGetKey string
	lastDelKey string
}

func (f *fakeSecretSvc) Create(_ context.Context, key, _ string) (repository.Secret, error) {
	if f.createErr != nil {
		return repository.Secret{}, f.createErr
	}
	f.created = repository.Secret{Key: key, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	return f.created, nil
}
func (f *fakeSecretSvc) Get(_ context.Context, key string) (repository.Secret, error) {
	f.lastGetKey = key
	return f.get, f.getErr
}
func (f *fakeSecretSvc) List(_ context.Context, _, _ int) ([]repository.Secret, int64, error) {
	return f.items, f.total, f.listErr
}
func (f *fakeSecretSvc) Delete(_ context.Context, key string) error {
	f.lastDelKey = key
	return f.deleteErr
}

func secretRouter(f *fakeSecretSvc) *gin.Engine {
	return NewRouter(Deps{Health: NewHealth(fakePinger{}), Secrets: f})
}

func TestSecretCreateMasksValue(t *testing.T) {
	f := &fakeSecretSvc{}
	r := secretRouter(f)
	w := performJSON(r, http.MethodPost, "/v1/secrets", `{"key":"API_KEY","value":"plain-value"}`, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body %s", w.Code, w.Body.String())
	}
	if body := w.Body.String(); body == "" || containsSecretValue(body, "plain-value") {
		t.Fatalf("response body leaked plaintext: %s", body)
	}
	var resp SecretResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Key != "API_KEY" || resp.ValueMasked != service.SecretMask {
		t.Fatalf("response = %+v", resp)
	}
}

func TestSecretCreateValidationAndConflict(t *testing.T) {
	for name, fake := range map[string]*fakeSecretSvc{
		"invalid":  {createErr: model.ErrInvalid},
		"conflict": {createErr: model.ErrConflict},
	} {
		t.Run(name, func(t *testing.T) {
			w := performJSON(secretRouter(fake), http.MethodPost, "/v1/secrets", `{"key":"API_KEY","value":"value"}`, nil)
			want := http.StatusUnprocessableEntity
			if name == "conflict" {
				want = http.StatusConflict
			}
			if w.Code != want {
				t.Fatalf("status = %d, want %d", w.Code, want)
			}
		})
	}
}

func TestSecretListMasksValues(t *testing.T) {
	f := &fakeSecretSvc{items: []repository.Secret{{Key: "API_KEY"}}, total: 1}
	w := performJSON(secretRouter(f), http.MethodGet, "/v1/secrets?page=1&per_page=10", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if containsSecretValue(w.Body.String(), "plain-value") {
		t.Fatalf("list body leaked plaintext: %s", w.Body.String())
	}
	var resp ListResponse[SecretResponse]
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ValueMasked != service.SecretMask || resp.Page != 1 || resp.PerPage != 10 {
		t.Fatalf("list response = %+v", resp)
	}
}

func TestSecretGetAndDeleteErrors(t *testing.T) {
	f := &fakeSecretSvc{getErr: model.ErrNotFound, deleteErr: model.ErrNotFound}
	r := secretRouter(f)
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		w := performJSON(r, method, "/v1/secrets/API_KEY", "", nil)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", method, w.Code)
		}
	}
	if f.lastGetKey != "API_KEY" || f.lastDelKey != "API_KEY" {
		t.Fatalf("service keys = get %q delete %q", f.lastGetKey, f.lastDelKey)
	}
}

func TestSecretListRejectsBadPagination(t *testing.T) {
	for _, path := range []string{"/v1/secrets?page=0", "/v1/secrets?per_page=999", "/v1/secrets?per_page=nope"} {
		w := performJSON(secretRouter(&fakeSecretSvc{}), http.MethodGet, path, "", nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400", path, w.Code)
		}
	}
}

func TestSecretGetInvalidKey(t *testing.T) {
	f := &fakeSecretSvc{getErr: model.ErrInvalid}
	w := performJSON(secretRouter(f), http.MethodGet, "/v1/secrets/bad-key", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %s", w.Code, w.Body.String())
	}
}

func TestSecretDeleteNoContent(t *testing.T) {
	w := performJSON(secretRouter(&fakeSecretSvc{}), http.MethodDelete, "/v1/secrets/API_KEY", "", nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
}

func TestSecretHandlerImplementsService(t *testing.T) {
	var _ service.SecretService = (*fakeSecretSvc)(nil)
}

func containsSecretValue(body, value string) bool {
	return len(value) > 0 && strings.Contains(body, value)
}
