package service

import (
	"context"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

var secretKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)

// SecretService is the use-case boundary for secret management.
type SecretService interface {
	Create(ctx context.Context, key, value string) (repository.Secret, error)
	Get(ctx context.Context, key string) (repository.Secret, error)
	List(ctx context.Context, page, perPage int) ([]repository.Secret, int64, error)
	Delete(ctx context.Context, key string) error
}

type secretService struct {
	repo repository.SecretRepository
}

// NewSecretService builds the secret service.
func NewSecretService(repo repository.SecretRepository) SecretService {
	return &secretService{repo: repo}
}

func (s *secretService) Create(ctx context.Context, key, value string) (repository.Secret, error) {
	if !secretKeyPattern.MatchString(key) {
		return repository.Secret{}, fmt.Errorf("%w: secret key must match ^[A-Za-z0-9_]{1,128}$", model.ErrInvalid)
	}
	valueLen := utf8.RuneCountInString(value)
	if valueLen < 1 || valueLen > 8192 {
		return repository.Secret{}, fmt.Errorf("%w: secret value must be 1-8192 characters", model.ErrInvalid)
	}
	return s.repo.Create(ctx, key, value)
}

func (s *secretService) Get(ctx context.Context, key string) (repository.Secret, error) {
	if !secretKeyPattern.MatchString(key) {
		return repository.Secret{}, fmt.Errorf("%w: secret key must match ^[A-Za-z0-9_]{1,128}$", model.ErrInvalid)
	}
	return s.repo.GetByKey(ctx, key)
}

func (s *secretService) List(ctx context.Context, page, perPage int) ([]repository.Secret, int64, error) {
	return s.repo.List(ctx, page, perPage)
}

func (s *secretService) Delete(ctx context.Context, key string) error {
	if !secretKeyPattern.MatchString(key) {
		return fmt.Errorf("%w: secret key must match ^[A-Za-z0-9_]{1,128}$", model.ErrInvalid)
	}
	return s.repo.Delete(ctx, key)
}
