package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// AuthService resolves authenticated identities onto user rows, seeds the
// role catalog from configuration, and reads the catalog back. Authorization
// itself is decided from the live token claims by the auth package; nothing
// here grants access.
type AuthService interface {
	// ResolvePrincipal maps a verified principal onto its users.id uuid,
	// inserting the identity on first sight. A service principal already
	// carries the configured system user id and is returned unchanged.
	ResolvePrincipal(ctx context.Context, p auth.Principal) (auth.Principal, error)
	// SeedRoles writes the configured catalog into the role and
	// role_permissions tables. It is called once at startup.
	SeedRoles(ctx context.Context) error
	// ListRoles returns every role with its permissions.
	ListRoles(ctx context.Context) ([]model.Role, map[string][]string, error)
	// GetRole returns one role with its permissions.
	GetRole(ctx context.Context, name string) (model.Role, []string, error)
	// Me returns the identity row behind a principal, for GET /v1/auth/me.
	Me(ctx context.Context, p auth.Principal) (model.User, error)
}

type authService struct {
	users   repository.UserRepository
	roles   repository.RoleRepository
	catalog auth.Catalog
	// systemUserID is the audit actor a service principal acts as.
	systemUserID string
	// now is injectable so tests get deterministic seed timestamps.
	now func() time.Time
}

// NewAuthService builds the auth service. systemUserID is the configured
// system user the API-token and broker bypasses act as.
func NewAuthService(
	users repository.UserRepository,
	roles repository.RoleRepository,
	catalog auth.Catalog,
	systemUserID string,
) AuthService {
	return &authService{
		users:        users,
		roles:        roles,
		catalog:      catalog,
		systemUserID: systemUserID,
		now:          nowUTC,
	}
}

func (s *authService) ResolvePrincipal(ctx context.Context, p auth.Principal) (auth.Principal, error) {
	if p.Service {
		// The bypass always acts as the configured system user, so the
		// audit trail of a service call is the same as an engine-side
		// effect.
		p.UserID = s.systemUserID
		return p, nil
	}
	// Failing closed: (subject, issuer) is the identity key, so a
	// principal missing either cannot be persisted. Writing it anyway would
	// mint a row that every later anonymous request collides on, and would
	// attribute audit rows to a user id nobody owns.
	if p.Subject == "" || p.Issuer == "" {
		return p, fmt.Errorf("service: resolve principal: missing subject or issuer: %w", model.ErrInvalid)
	}
	now := s.now()
	id, err := s.users.UpsertBySubject(ctx, model.User{
		// The uuid is allocated before the write so a retry after a failed
		// upsert resolves to the same row instead of forking the identity.
		ID:        mustNewID(),
		Subject:   p.Subject,
		Issuer:    p.Issuer,
		Name:      p.Name,
		Email:     p.Email,
		Metadata:  auth.MarshalPrincipalUser(p),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return p, fmt.Errorf("service: resolve principal: %w", err)
	}
	p.UserID = id
	return p, nil
}

func (s *authService) SeedRoles(ctx context.Context) error {
	seed := s.catalog.Seed()
	if len(seed.Roles) == 0 {
		return nil
	}
	now := s.now()
	roles := make([]model.Role, 0, len(seed.Roles))
	permissions := make([]model.RolePermission, 0, len(seed.Roles))
	for _, name := range seed.Roles {
		roles = append(roles, model.Role{
			Name:        name,
			Description: roleDescription(name),
			CreatedAt:   now,
			UpdatedAt:   now,
		})
		for _, action := range seed.Permissions[name] {
			permissions = append(permissions, model.RolePermission{
				Role:      name,
				Action:    action,
				CreatedAt: now,
				UpdatedAt: now,
			})
		}
	}
	if err := s.roles.Seed(ctx, roles, permissions); err != nil {
		return fmt.Errorf("service: seed roles: %w", err)
	}
	return nil
}

// ListRoles answers from the live catalog rather than the role tables. The
// catalog is what the request gates consult, so serving anything else lets
// the read API describe permissions the engine would never grant, or omit
// ones it would. The tables stay as an audit copy of what was configured.
//
// The timestamps are the read time, not the time the role was configured.
// The role has no independent creation: it exists because configuration
// names it, and configuration is replaced on deploy rather than edited row by
// row, so any timestamp read back out of the tables would be the moment of
// the last restart rather than anything the operator can act on. now is
// injectable so that contract is testable instead of asserted.
func (s *authService) ListRoles(ctx context.Context) ([]model.Role, map[string][]string, error) {
	seed := s.catalog.Seed()
	now := s.now()
	roles := make([]model.Role, 0, len(seed.Roles))
	perms := make(map[string][]string, len(seed.Roles))
	for _, name := range seed.Roles {
		roles = append(roles, model.Role{
			Name:        name,
			Description: roleDescription(name),
			CreatedAt:   now,
			UpdatedAt:   now,
		})
		actions := seed.Permissions[name]
		if actions == nil {
			actions = []string{}
		}
		perms[name] = actions
	}
	return roles, perms, nil
}

// GetRole answers from the live catalog for the same reason ListRoles does.
// A name the catalog does not carry is ErrNotFound, not a leftover row.
func (s *authService) GetRole(ctx context.Context, name string) (model.Role, []string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Role{}, nil, fmt.Errorf("%w: role name is required", model.ErrInvalid)
	}
	if !s.catalog.HasRole(name) {
		return model.Role{}, nil, fmt.Errorf("%w: role %q", model.ErrNotFound, name)
	}
	seed := s.catalog.Seed()
	actions := seed.Permissions[name]
	if actions == nil {
		actions = []string{}
	}
	now := s.now()
	return model.Role{
		Name:        name,
		Description: roleDescription(name),
		CreatedAt:   now,
		UpdatedAt:   now,
	}, actions, nil
}

// Me returns the identity row behind a principal, for GET /v1/auth/me.
// A principal that never resolved to a user row is a credential problem,
// not a missing resource: ErrInvalid so the handler answers 401, because a
// caller that cannot name a user is not a 404 for a user the client asked
// about.
func (s *authService) Me(ctx context.Context, p auth.Principal) (model.User, error) {
	if p.UserID == "" {
		return model.User{}, fmt.Errorf("%w: principal is not resolved to a user", model.ErrInvalid)
	}
	user, err := s.users.GetByID(ctx, p.UserID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return model.User{}, fmt.Errorf("%w: user %s", model.ErrNotFound, p.UserID)
		}
		return model.User{}, err
	}
	return user, nil
}

// roleDescription renders the seeded description of a role. Roles are
// config-defined, so the description is derived rather than authored: a
// role is an operator-facing bundle of resource-actions.
func roleDescription(name string) string {
	return "Configured role " + name
}
