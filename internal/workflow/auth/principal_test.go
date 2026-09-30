package auth_test

import (
	"context"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
)

// testCatalog is the catalog the RBAC matrix runs against. The union of a
// caller's roles is what grants a permission, and a role the catalog does
// not know grants nothing.
var testCatalog = auth.NewCatalog(map[string][]string{
	"admin": {
		auth.ActionDefinitionsRead, auth.ActionDefinitionsWrite,
		auth.ActionSecretsRead, auth.ActionSecretsWrite,
		auth.ActionInstancesCreate, auth.ActionInstancesRead,
		auth.ActionInstancesUpdateContext, auth.ActionInputDeliver,
		auth.ActionInstancesControl, auth.ActionStatisticsRead,
		auth.ActionRolesRead,
	},
	"finance": {auth.ActionInstancesRead, auth.ActionInputDeliver},
	"auditor": {auth.ActionDefinitionsRead, auth.ActionInstancesRead, auth.ActionStatisticsRead, auth.ActionRolesRead},
})

func TestHasPermissionAcrossRoles(t *testing.T) {
	cases := []struct {
		name      string
		principal auth.Principal
		action    string
		want      bool
	}{
		{"admin holds every action", auth.Principal{Roles: []string{"admin"}}, auth.ActionSecretsWrite, true},
		{"finance may deliver", auth.Principal{Roles: []string{"finance"}}, auth.ActionInputDeliver, true},
		{"finance may not write definitions", auth.Principal{Roles: []string{"finance"}}, auth.ActionDefinitionsWrite, false},
		{"finance may not read secrets", auth.Principal{Roles: []string{"finance"}}, auth.ActionSecretsRead, false},
		// The union across two roles is what counts, not any single role.
		{"union across roles", auth.Principal{Roles: []string{"finance", "auditor"}}, auth.ActionRolesRead, true},
		{"union across roles second half", auth.Principal{Roles: []string{"finance", "auditor"}}, auth.ActionInputDeliver, true},
		{"no roles grants nothing", auth.Principal{}, auth.ActionInstancesRead, false},
		{"unknown role grants nothing", auth.Principal{Roles: []string{"wizard"}}, auth.ActionInstancesRead, false},
		// A role that exists but is configured with no actions grants nothing.
		{"empty role grants nothing", auth.Principal{Roles: []string{"nobody"}}, auth.ActionInstancesRead, false},
		{"service bypasses every action", auth.SystemPrincipal("u"), auth.ActionSecretsWrite, true},
		{"service bypasses an unknown action", auth.SystemPrincipal("u"), "made:up", true},
		{"empty action is never granted", auth.Principal{Roles: []string{"admin"}}, "", false},
	}
	catalog := auth.NewCatalog(map[string][]string{
		"admin":   {auth.ActionSecretsWrite, auth.ActionInstancesRead},
		"finance": {auth.ActionInputDeliver},
		"nobody":  {},
		"auditor": {auth.ActionRolesRead},
	})
	for _, tc := range cases {
		if got := tc.principal.HasPermission(tc.action, catalog); got != tc.want {
			t.Errorf("%s: HasPermission(%q) = %v, want %v", tc.name, tc.action, got, tc.want)
		}
	}
}

// A role configured with the wildcard authorizes every action, which is the
// same bypass the service principal gets. An empty action is still refused.
func TestHasPermissionWildcard(t *testing.T) {
	catalog := auth.NewCatalog(map[string][]string{
		"admin":   {auth.WildcardAction},
		"finance": {auth.ActionInputDeliver},
	})
	cases := []struct {
		name      string
		principal auth.Principal
		action    string
		want      bool
	}{
		{"wildcard role reads definitions", auth.Principal{Roles: []string{"admin"}}, auth.ActionDefinitionsRead, true},
		{"wildcard role delivers input", auth.Principal{Roles: []string{"admin"}}, auth.ActionInputDeliver, true},
		{"wildcard role runs an arbitrary action", auth.Principal{Roles: []string{"admin"}}, "made:up", true},
		{"wildcard alongside a narrow role", auth.Principal{Roles: []string{"admin", "finance"}}, auth.ActionSecretsWrite, true},
		{"narrow role keeps its own action", auth.Principal{Roles: []string{"finance"}}, auth.ActionInputDeliver, true},
		{"narrow role gains nothing from the wildcard role", auth.Principal{Roles: []string{"finance"}}, auth.ActionDefinitionsRead, false},
		{"unknown role still denies", auth.Principal{Roles: []string{"wizard"}}, auth.ActionDefinitionsRead, false},
		{"empty action is never granted, even by the wildcard", auth.Principal{Roles: []string{"admin"}}, "", false},
	}
	for _, tc := range cases {
		if got := tc.principal.HasPermission(tc.action, catalog); got != tc.want {
			t.Errorf("%s: HasPermission(%q) = %v, want %v", tc.name, tc.action, got, tc.want)
		}
	}
}

func TestCanSatisfyRoles(t *testing.T) {
	cases := []struct {
		name      string
		principal auth.Principal
		allowed   []string
		want      bool
	}{
		{"intersecting role", auth.Principal{Roles: []string{"finance", "manager"}}, []string{"manager"}, true},
		{"disjoint roles", auth.Principal{Roles: []string{"finance"}}, []string{"manager"}, false},
		{"empty allow list is open", auth.Principal{Roles: nil}, []string{}, true},
		{"no allow list at all is open", auth.Principal{Roles: nil}, nil, true},
		{"no roles against a closed gate", auth.Principal{}, []string{"manager"}, false},
		{"no roles against an empty gate", auth.Principal{}, []string{}, true},
		// An unknown role still matches a node that lists it: the gate is
		// about identity, not about what the catalog grants.
		{"unknown role matches a listed name", auth.Principal{Roles: []string{"wizard"}}, []string{"wizard"}, true},
		{"service always satisfies", auth.SystemPrincipal("u"), []string{"manager"}, true},
	}
	for _, tc := range cases {
		if got := tc.principal.CanSatisfyRoles(tc.allowed); got != tc.want {
			t.Errorf("%s: CanSatisfyRoles(%v) = %v, want %v", tc.name, tc.allowed, got, tc.want)
		}
	}
}

// The two gates are independent: holding the endpoint permission does not
// imply satisfying the node gate, and the reverse is not even checked.
func TestGatesAreIndependent(t *testing.T) {
	catalog := testCatalog
	finance := auth.Principal{Roles: []string{"finance"}}

	if !finance.HasPermission(auth.ActionInputDeliver, catalog) {
		t.Fatal("finance must pass the endpoint gate")
	}
	if finance.CanSatisfyRoles([]string{"manager"}) {
		t.Error("finance must still fail a node gate listing only manager")
	}
	// A role with no permissions at all still satisfies a node gate that
	// names it, because the endpoint gate would have refused it already.
	wizard := auth.Principal{Roles: []string{"wizard"}}
	if wizard.HasPermission(auth.ActionInputDeliver, catalog) {
		t.Error("an unknown role must not pass the endpoint gate")
	}
	if !wizard.CanSatisfyRoles([]string{"wizard"}) {
		t.Error("an unknown role must still match a node listing it")
	}
}

func TestCatalogLookups(t *testing.T) {
	if !testCatalog.HasRole("finance") {
		t.Error("HasRole(finance) = false, want true")
	}
	if testCatalog.HasRole("wizard") {
		t.Error("HasRole(wizard) = true, want false")
	}
	roles := testCatalog.Roles()
	if len(roles) != 3 || roles[0] != "admin" || roles[1] != "auditor" || roles[2] != "finance" {
		t.Errorf("Roles() = %v, want a sorted [admin auditor finance]", roles)
	}
	if got := testCatalog.PermissionsFor("wizard"); got != nil {
		t.Errorf("PermissionsFor(wizard) = %v, want nil for an unknown role", got)
	}
}

func TestCatalogSeedIsDeterministicAndDeduped(t *testing.T) {
	catalog := auth.NewCatalog(map[string][]string{
		"finance": {auth.ActionInputDeliver, auth.ActionInstancesRead, auth.ActionInputDeliver},
		"admin":   {auth.ActionRolesRead},
	})
	seed := catalog.Seed()
	if len(seed.Roles) != 2 || seed.Roles[0] != "admin" || seed.Roles[1] != "finance" {
		t.Errorf("Seed().Roles = %v, want sorted [admin finance]", seed.Roles)
	}
	perms := seed.Permissions["finance"]
	if len(perms) != 2 {
		t.Fatalf("finance permissions = %v, want the duplicate removed", perms)
	}
	if perms[0] != auth.ActionInputDeliver || perms[1] != auth.ActionInstancesRead {
		t.Errorf("finance permissions = %v, want them sorted", perms)
	}
}

// Mutating the map the catalog was built from must not change the catalog:
// it is read on every request and shared across goroutines.
func TestCatalogIsImmutableAfterConstruction(t *testing.T) {
	source := map[string][]string{"finance": {auth.ActionInputDeliver}}
	catalog := auth.NewCatalog(source)
	source["finance"] = nil
	source["admin"] = []string{auth.ActionRolesRead}
	if !(auth.Principal{Roles: []string{"finance"}}).HasPermission(auth.ActionInputDeliver, catalog) {
		t.Error("catalog changed after the source map was mutated")
	}
	if catalog.HasRole("admin") {
		t.Error("a role added to the source map appeared in the catalog")
	}
}

// A YAML block scalar or a careless copy leaves whitespace around a role
// name. Actions were already trimmed; the role keys were not, so " finance "
// was a role no token could ever assert. Trimming the keys is what makes the
// role name in config.yaml and the one in a token the same string.
func TestNewCatalogTrimsRoleNames(t *testing.T) {
	catalog := auth.NewCatalog(map[string][]string{
		" finance ":  {auth.ActionInputDeliver},
		"\tadmin\n":  {auth.ActionRolesRead},
		"   ":        {auth.ActionRolesRead},
		"auditor   ": {auth.ActionInstancesRead, "  ", auth.ActionStatisticsRead},
	})
	if !catalog.HasRole("finance") {
		t.Error(`HasRole("finance") = false, want true`)
	}
	if !catalog.HasRole("admin") {
		t.Error(`HasRole("admin") = false, want true`)
	}
	// A key that is only whitespace names no one and grants nothing, so it
	// is dropped rather than stored as an unreachable role.
	if catalog.HasRole("   ") {
		t.Error(`HasRole("   ") = true, want false`)
	}
	if roles := catalog.Roles(); len(roles) != 3 {
		t.Errorf("Roles() = %v, want the 3 named roles", roles)
	}
	// Actions keep the same trimming they already had, so a blank entry
	// never becomes a permission a gate could match.
	if got := catalog.PermissionsFor("auditor"); len(got) != 2 {
		t.Errorf("PermissionsFor(auditor) = %v, want the two real actions", got)
	}
}

func TestContextRoundTrip(t *testing.T) {
	p := auth.Principal{Subject: "u-1", Issuer: "https://issuer.test", Roles: []string{"finance"}, UserID: "u-id"}
	ctx := auth.ContextWithPrincipal(context.Background(), p, testCatalog)

	got, ok := auth.FromContext(ctx)
	if !ok {
		t.Fatal("FromContext() ok = false, want the principal")
	}
	if got.Subject != "u-1" || got.UserID != "u-id" {
		t.Errorf("round trip lost fields: %+v", got)
	}
	perms, ok := auth.PermissionsFromContext(ctx)
	if !ok {
		t.Fatal("PermissionsFromContext() ok = false")
	}
	if !perms[auth.ActionInputDeliver] {
		t.Error("permissions do not carry input:deliver for the finance role")
	}
	if _, ok := auth.FromContext(context.Background()); ok {
		t.Error("FromContext on a bare context returned a principal")
	}
}

func TestSystemPrincipalIsService(t *testing.T) {
	p := auth.SystemPrincipal("11111111-1111-7111-8111-111111111111")
	if !p.Service {
		t.Error("Service = false, want true")
	}
	if p.UserID != "11111111-1111-7111-8111-111111111111" {
		t.Errorf("UserID = %q, want the configured system user", p.UserID)
	}
	perms := p.Permissions(testCatalog)
	if len(perms) != 1 || !perms[auth.WildcardAction] {
		t.Errorf("Permissions() = %v, want only the wildcard", perms)
	}
}
