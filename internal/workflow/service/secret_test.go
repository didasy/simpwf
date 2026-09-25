package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

type fakeSecretRepo struct {
	secrets map[string]string
}

func (f *fakeSecretRepo) Create(_ context.Context, key, value string) (repository.Secret, error) {
	if _, exists := f.secrets[key]; exists {
		return repository.Secret{}, model.ErrConflict
	}
	f.secrets[key] = value
	return repository.Secret{Key: key}, nil
}
func (f *fakeSecretRepo) GetByKey(_ context.Context, key string) (repository.Secret, error) {
	if _, ok := f.secrets[key]; !ok {
		return repository.Secret{}, model.ErrNotFound
	}
	return repository.Secret{Key: key}, nil
}
func (f *fakeSecretRepo) List(_ context.Context, _, _ int) ([]repository.Secret, int64, error) {
	items := make([]repository.Secret, 0, len(f.secrets))
	for key := range f.secrets {
		items = append(items, repository.Secret{Key: key})
	}
	return items, int64(len(items)), nil
}
func (f *fakeSecretRepo) Delete(_ context.Context, key string) error {
	if _, ok := f.secrets[key]; !ok {
		return model.ErrNotFound
	}
	delete(f.secrets, key)
	return nil
}
func (f *fakeSecretRepo) GetAll(_ context.Context) (map[string]string, error) {
	values := make(map[string]string, len(f.secrets))
	for key, value := range f.secrets {
		values[key] = value
	}
	return values, nil
}

func newSecretService() (service.SecretService, *fakeSecretRepo) {
	repo := &fakeSecretRepo{secrets: map[string]string{}}
	return service.NewSecretService(repo), repo
}

func TestSecretServiceRejectsInvalidKey(t *testing.T) {
	svc, _ := newSecretService()
	for _, key := range []string{"", "bad-key", "bad key", strings.Repeat("A", 129)} {
		if _, err := svc.Create(context.Background(), key, "value"); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("Create(%q) error = %v, want ErrInvalid", key, err)
		}
	}
}

func TestSecretServiceAcceptsDigitLeadingKey(t *testing.T) {
	svc, _ := newSecretService()
	created, err := svc.Create(context.Background(), "1API_KEY", "value")
	if err != nil {
		t.Fatal(err)
	}
	if created.Key != "1API_KEY" {
		t.Fatalf("created = %+v", created)
	}
}

func TestSecretServiceRejectsInvalidValue(t *testing.T) {
	svc, _ := newSecretService()
	for _, value := range []string{"", strings.Repeat("x", 8193)} {
		if _, err := svc.Create(context.Background(), "API_KEY", value); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("Create() value length %d error = %v, want ErrInvalid", len(value), err)
		}
	}
}

func TestSecretServicePropagatesConflict(t *testing.T) {
	svc, _ := newSecretService()
	if _, err := svc.Create(context.Background(), "API_KEY", "value"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Create(context.Background(), "API_KEY", "replacement"); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("duplicate Create() error = %v, want ErrConflict", err)
	}
}
