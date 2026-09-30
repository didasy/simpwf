package service

import (
	"context"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// A role served from the live catalog has no creation of its own: it exists
// because configuration names it. The read APIs therefore stamp the read
// time, and pin the clock so the contract is testable rather than asserted in
// a comment that can rot.
//
// The role tables keep a real created_at, and that is deliberate: they are
// the audit copy of what was seeded, and the test below shows the two
// disagreeing rather than assuming they agree.
func TestRoleReadsCarryTheReadTime(t *testing.T) {
	seeded := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	read := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

	roles := &countingRoleRepo{}
	svc := NewAuthService(nil, roles, auth.NewCatalog(map[string][]string{
		"finance": {auth.ActionInputDeliver},
	}), "sys").(*authService)
	svc.now = func() time.Time { return read }

	list, _, err := svc.ListRoles(context.Background())
	if err != nil {
		t.Fatalf("ListRoles() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListRoles() returned %d roles, want 1", len(list))
	}
	if !list[0].CreatedAt.Equal(read) || !list[0].UpdatedAt.Equal(read) {
		t.Errorf("ListRoles timestamps = %v/%v, want the read time %v",
			list[0].CreatedAt, list[0].UpdatedAt, read)
	}

	role, _, err := svc.GetRole(context.Background(), "finance")
	if err != nil {
		t.Fatalf("GetRole() error = %v", err)
	}
	if !role.CreatedAt.Equal(read) || !role.UpdatedAt.Equal(read) {
		t.Errorf("GetRole timestamps = %v/%v, want the read time %v",
			role.CreatedAt, role.UpdatedAt, read)
	}

	// The seed written to the audit tables keeps its own timestamp, so the
	// two views are visibly different rather than silently aliased.
	if err := svc.SeedRoles(context.Background()); err != nil {
		t.Fatalf("SeedRoles() error = %v", err)
	}
	if !roles.seededAt.Equal(read) {
		t.Errorf("seeded created_at = %v, want the injected clock %v", roles.seededAt, read)
	}
	if !seeded.Before(read) {
		t.Error("the fixture timestamps should differ from the read time, or this proves nothing")
	}
}

// countingRoleRepo records the rows the startup seed would have written.
type countingRoleRepo struct {
	seededAt time.Time
}

func (c *countingRoleRepo) Seed(_ context.Context, roles []model.Role, _ []model.RolePermission) error {
	if len(roles) > 0 {
		c.seededAt = roles[0].CreatedAt
	}
	return nil
}

func (c *countingRoleRepo) List(context.Context) ([]model.Role, map[string][]string, error) {
	return nil, nil, nil
}

func (c *countingRoleRepo) Get(context.Context, string) (model.Role, []string, error) {
	return model.Role{}, nil, repository.ErrRoleNotFound
}
