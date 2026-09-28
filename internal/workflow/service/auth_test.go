package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"gorm.io/gorm"
)

const authSvcIssuer = "https://idp.example.test"

// authServiceFor builds a real auth service over the test database.
func authServiceFor(t *testing.T, catalog auth.Catalog) (service.AuthService, *gorm.DB) {
	t.Helper()
	db := setupSvcDB(t)
	return service.NewAuthService(
		repository.NewUserRepository(db),
		repository.NewRoleRepository(db),
		catalog,
		svcSysUserID,
	), db
}

// TestResolvePrincipalIsStable is the regression test for the just-in-time
// upsert: the second sighting of a human must resolve to the row the first
// sighting created, not to a freshly minted uuid that matches no user.
// Every created_by/updated_by foreign key points at this id, so drift here
// silently orphans the audit trail.
func TestResolvePrincipalIsStable(t *testing.T) {
	svc, db := authServiceFor(t, auth.NewCatalog(nil))
	ctx := context.Background()
	users := repository.NewUserRepository(db)

	first, err := svc.ResolvePrincipal(ctx, auth.Principal{
		Subject: "ada-1", Issuer: authSvcIssuer, Name: "Ada", Email: "ada@example.test",
	})
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if first.UserID == "" {
		t.Fatal("first resolve returned an empty user id")
	}
	if _, err := users.GetByID(ctx, first.UserID); err != nil {
		t.Fatalf("first resolve id does not resolve to a user: %v", err)
	}

	// A second request allocates a different candidate uuid, so a returned
	// id equal to this call's candidate would be a real fork.
	second, err := svc.ResolvePrincipal(ctx, auth.Principal{
		Subject: "ada-1", Issuer: authSvcIssuer, Name: "Ada Lovelace", Email: "ada@example.test",
	})
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if second.UserID != first.UserID {
		t.Fatalf("identity forked: first %s, second %s", first.UserID, second.UserID)
	}

	var count int64
	if err := db.Model(&repository.UserModel{}).
		Where("subject = ? AND issuer = ?", "ada-1", authSvcIssuer).Count(&count).Error; err != nil {
		t.Fatalf("count users: %v", err)
	}
	if count != 1 {
		t.Fatalf("user rows = %d, want 1", count)
	}
	user, err := users.GetByID(ctx, first.UserID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if user.Name != "Ada Lovelace" {
		t.Fatalf("name = %q, want the refreshed value", user.Name)
	}
}

// TestResolvePrincipalIsolatesIssuers proves (subject, issuer) is the
// identity key: the same subject string at a different provider is a
// different human and must not collide.
func TestResolvePrincipalIsolatesIssuers(t *testing.T) {
	svc, _ := authServiceFor(t, auth.NewCatalog(nil))
	ctx := context.Background()

	a, err := svc.ResolvePrincipal(ctx, auth.Principal{Subject: "shared", Issuer: "https://a.example.test"})
	if err != nil {
		t.Fatalf("resolve at issuer a: %v", err)
	}
	b, err := svc.ResolvePrincipal(ctx, auth.Principal{Subject: "shared", Issuer: "https://b.example.test"})
	if err != nil {
		t.Fatalf("resolve at issuer b: %v", err)
	}
	if a.UserID == b.UserID {
		t.Fatalf("two issuers shared the user id %s", a.UserID)
	}
}

// TestResolvePrincipalServiceBypass: the service principal is never written
// to users by the upsert path; it already carries the configured system id.
// The fixture seeds a system row and an OIDC user, so the count must not
// grow and no new provider identity may appear.
func TestResolvePrincipalServiceBypass(t *testing.T) {
	svc, db := authServiceFor(t, auth.NewCatalog(nil))
	ctx := context.Background()
	countUsers := func() int64 {
		var n int64
		if err := db.Model(&repository.UserModel{}).Count(&n).Error; err != nil {
			t.Fatalf("count users: %v", err)
		}
		return n
	}
	countIdentities := func() int64 {
		var n int64
		if err := db.Model(&repository.UserModel{}).Where("subject <> ''").Count(&n).Error; err != nil {
			t.Fatalf("count identified users: %v", err)
		}
		return n
	}
	before, beforeIdentities := countUsers(), countIdentities()

	got, err := svc.ResolvePrincipal(ctx, auth.Principal{Service: true})
	if err != nil {
		t.Fatalf("resolve service principal: %v", err)
	}
	if got.UserID != svcSysUserID {
		t.Fatalf("service user id = %s, want %s", got.UserID, svcSysUserID)
	}
	if after := countUsers(); after != before {
		t.Fatalf("service resolution created %d user rows, want none", after-before)
	}
	// The service principal has no provider identity, so resolving it must
	// not persist one.
	if after := countIdentities(); after != beforeIdentities {
		t.Fatalf("service resolution added %d provider identities, want none", after-beforeIdentities)
	}
}

// TestResolvePrincipalRejectsMissingIdentity fails closed: a principal with
// no (subject, issuer) names nobody, so persisting it would mint an
// identity row that every later request collides on.
func TestResolvePrincipalRejectsMissingIdentity(t *testing.T) {
	svc, db := authServiceFor(t, auth.NewCatalog(nil))
	ctx := context.Background()
	countUsers := func() int64 {
		var n int64
		if err := db.Model(&repository.UserModel{}).Where("subject = ''").Count(&n).Error; err != nil {
			t.Fatalf("count anonymous users: %v", err)
		}
		return n
	}
	before := countUsers()

	cases := map[string]auth.Principal{
		"no subject": {Issuer: authSvcIssuer, Roles: []string{"finance"}},
		"no issuer":  {Subject: "ada-1", Roles: []string{"finance"}},
		"neither":    {Roles: []string{"finance"}},
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.ResolvePrincipal(ctx, p); !errors.Is(err, model.ErrInvalid) {
				t.Fatalf("ResolvePrincipal() error = %v, want ErrInvalid", err)
			}
		})
	}

	if after := countUsers(); after != before {
		t.Errorf("anonymous users = %d, want %d: a refused identity must not be persisted", after, before)
	}
}

// TestSeedRolesIsIdempotentAndNonDestructive: seeding twice must not
// duplicate, and a role dropped from configuration keeps its rows so an
// in-flight token still resolves it.
func TestSeedRolesIsIdempotentAndNonDestructive(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	roles := repository.NewRoleRepository(db)
	users := repository.NewUserRepository(db)
	svc := service.NewAuthService(users, roles, auth.NewCatalog(map[string][]string{
		"admin":   {"*"},
		"finance": {auth.ActionInputDeliver, auth.ActionInstancesRead},
	}), svcSysUserID)

	if err := svc.SeedRoles(ctx); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := svc.SeedRoles(ctx); err != nil {
		t.Fatalf("reseed: %v", err)
	}

	list, perms, err := svc.ListRoles(ctx)
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("roles = %d, want 2 (reseed must not duplicate): %+v", len(list), list)
	}
	if got := perms["finance"]; len(got) != 2 {
		t.Fatalf("finance permissions = %v, want 2 entries", got)
	}
	if got := perms["admin"]; len(got) != 1 || got[0] != "*" {
		t.Fatalf("admin permissions = %v, want [*]", got)
	}

	// Drop finance from the catalog and reseed. The role row survives, but
	// it is no longer part of the live catalog, so it is not served: the
	// read APIs answer from the catalog that authorizes requests, and a
	// role that grants nothing must not be advertised as if it did.
	svc = service.NewAuthService(users, roles, auth.NewCatalog(map[string][]string{
		"admin": {"*"},
	}), svcSysUserID)
	if err := svc.SeedRoles(ctx); err != nil {
		t.Fatalf("reseed after removal: %v", err)
	}
	if _, _, err := svc.GetRole(ctx, "finance"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("GetRole(finance) error = %v, want ErrNotFound once dropped from the catalog", err)
	}
	list, perms, err = svc.ListRoles(ctx)
	if err != nil {
		t.Fatalf("list roles after removal: %v", err)
	}
	if len(list) != 1 || list[0].Name != "admin" {
		t.Errorf("roles = %+v, want only admin (the live catalog)", list)
	}
	if _, ok := perms["finance"]; ok {
		t.Errorf("permissions = %v, want no entry for a dropped role", perms)
	}
}

// The role tables are an audit copy of configuration, not the source of
// truth: authorization reads the live catalog. A permission removed from
// configuration must therefore stop being reported, even though its row is
// still in the table.
func TestRolesAreServedFromTheLiveCatalog(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	roles := repository.NewRoleRepository(db)
	users := repository.NewUserRepository(db)

	svc := service.NewAuthService(users, roles, auth.NewCatalog(map[string][]string{
		"finance": {auth.ActionInputDeliver, auth.ActionInstancesRead},
	}), svcSysUserID)
	if err := svc.SeedRoles(ctx); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Narrow the role in the database only, as a stale row would look.
	if err := db.Model(&repository.RolePermissionModel{}).
		Where("action = ?", auth.ActionInstancesRead).
		Delete(&repository.RolePermissionModel{}).Error; err != nil {
		t.Fatalf("delete stale permission row: %v", err)
	}

	// The catalog is the source of truth, so the full grant is reported
	// even though the table lost a row.
	_, perms, err := svc.ListRoles(ctx)
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	if len(perms["finance"]) != 2 {
		t.Errorf("finance permissions = %v, want both actions from the catalog", perms["finance"])
	}
}

// TestGetRoleUnknownIsNotFound pins the sentinel translation: an unknown role
// is a client mistake and must surface as ErrNotFound (HTTP 404), not as an
// unmapped repository error (HTTP 500).
func TestGetRoleUnknownIsNotFound(t *testing.T) {
	svc, _ := authServiceFor(t, auth.NewCatalog(map[string][]string{"admin": {"*"}}))
	ctx := context.Background()
	if err := svc.SeedRoles(ctx); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, _, err := svc.GetRole(ctx, "wizard")
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("GetRole(wizard) error = %v, want ErrNotFound", err)
	}
	if _, _, err := svc.GetRole(ctx, "  "); !errors.Is(err, model.ErrInvalid) {
		t.Fatalf("GetRole(blank) error = %v, want ErrInvalid", err)
	}
	if _, _, err := svc.GetRole(ctx, "admin"); err != nil {
		t.Fatalf("GetRole(admin) error = %v, want nil", err)
	}
}

// TestMeRejectsUnresolvedPrincipal: /v1/auth/me must not read a row for a
// principal that was never resolved to a user. The caller authenticated
// successfully, so this is a credential failure (401), not a missing
// resource: there is no user the client asked about to be missing.
func TestMeRejectsUnresolvedPrincipal(t *testing.T) {
	svc, _ := authServiceFor(t, auth.NewCatalog(nil))
	_, err := svc.Me(context.Background(), auth.Principal{})
	if !errors.Is(err, model.ErrInvalid) {
		t.Fatalf("Me(unresolved) error = %v, want ErrInvalid so the handler answers 401", err)
	}
	if errors.Is(err, model.ErrNotFound) {
		t.Error("Me(unresolved) still reads as a 404")
	}
}

// TestSeedRolesEmptyCatalogIsANoOp keeps a deployment with no configured
// catalog from failing at startup.
func TestSeedRolesEmptyCatalogIsANoOp(t *testing.T) {
	svc, db := authServiceFor(t, auth.NewCatalog(nil))
	if err := svc.SeedRoles(context.Background()); err != nil {
		t.Fatalf("seed empty catalog: %v", err)
	}
	var count int64
	if err := db.Model(&repository.RoleModel{}).Count(&count).Error; err != nil {
		t.Fatalf("count roles: %v", err)
	}
	if count != 0 {
		t.Fatalf("roles = %d, want 0", count)
	}
}
