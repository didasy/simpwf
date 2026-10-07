// Package auth resolves the caller of a request into a Principal and answers
// authorization questions about it. It is transport-independent: the Gin
// middleware only moves the resolved Principal in and out of the request
// context, so the same rules apply to a bearer token, an API token, and the
// broker consumers that never carry a credential at all.
package auth

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/simpwf/workflow-engine/pkg/configuration"
)

// Context keys used to carry the principal and the resolved permissions
// through a request context.
type contextKey int

const (
	principalContextKey contextKey = iota
	permissionsContextKey
)

// WildcardAction is a catalog spelling of the bypass: a role granted it
// authorizes every resource-action. The service principal bypasses on its
// own flag instead.
const WildcardAction = "*"

// Resource-action permissions. One action per route, so a role grants a
// capability rather than a path. They are configured, so an operator may
// bind whatever subset of these an endpoint exposes; a role naming an action
// the build does not know still resolves, it simply never matches a gate.
const (
	ActionDefinitionsRead        = "definitions:read"
	ActionDefinitionsWrite       = "definitions:write"
	ActionSecretsRead            = "secrets:read"
	ActionSecretsWrite           = "secrets:write"
	ActionInstancesCreate        = "instances:create"
	ActionInstancesRead          = "instances:read"
	ActionInstancesUpdateContext = "instances:update-context"
	// ActionInputDeliver is the endpoint half of the input authorization.
	// The input node's allowed_roles is the independent second half, so a
	// caller may hold this and still be refused by a node that does not
	// list one of its roles.
	ActionInputDeliver     = "input:deliver"
	ActionInstancesControl = "instances:control"
	ActionStatisticsRead   = "statistics:read"
	ActionRolesRead        = "roles:read"
	ActionSchedulesRead    = "schedules:read"
	ActionSchedulesWrite   = "schedules:write"
)

// Principal is the authenticated caller of a request.
type Principal struct {
	// Subject and Issuer identify the principal at the provider. Both are
	// empty for the service principal.
	Subject string
	Issuer  string
	Name    string
	Email   string
	// Roles are the roles the token asserted. They drive both the endpoint
	// permission union and the input-node role gate.
	Roles []string
	// Service marks the API-token / broker principal: it bypasses the
	// endpoint permission and the per-node role gate, and is recorded as
	// the system user on every audit write it causes.
	Service bool
	// UserID is the users.id uuid this principal is recorded as. It is
	// resolved from the identity cache, so it is empty only for a principal
	// that has never been seen.
	UserID string
}

// Permissions returns the union of the permissions granted by the
// principal's roles. A service principal holds every known action, so a
// listing of its permissions shows what the bypass covers. The bypass
// itself lives in HasPermission, not in this set.
func (p Principal) Permissions(catalog Catalog) map[string]bool {
	out := make(map[string]bool)
	if p.Service {
		for _, action := range KnownActions() {
			out[action] = true
		}
		return out
	}
	for _, role := range p.Roles {
		for _, action := range catalog.PermissionsFor(role) {
			out[action] = true
		}
	}
	return out
}

// HasPermission reports whether the principal may perform action. A service
// principal may perform anything. A role granted the wildcard in the
// catalog may also perform anything, so admin: ["*"] is a configuration
// spelling of the same bypass.
// A role name that is not in the catalog grants nothing, so an unknown
// claim role denies by default while still matching an input node's
// allowed_roles.
func (p Principal) HasPermission(action string, catalog Catalog) bool {
	if action == "" {
		return false
	}
	if p.Service {
		return true
	}
	perms := p.Permissions(catalog)
	if perms[WildcardAction] {
		return true
	}
	return perms[action]
}

// CanSatisfyRoles reports whether any of the principal's roles appears in
// allowed. An empty allowed list is open to every caller, including one
// without roles. A service principal always satisfies the gate.
func (p Principal) CanSatisfyRoles(allowed []string) bool {
	if p.Service || len(allowed) == 0 {
		return true
	}
	for _, role := range p.Roles {
		for _, want := range allowed {
			if role == want {
				return true
			}
		}
	}
	return false
}

// ContextWithPrincipal returns a context carrying the principal and its
// resolved permissions.
func ContextWithPrincipal(ctx context.Context, p Principal, catalog Catalog) context.Context {
	ctx = context.WithValue(ctx, principalContextKey, p)
	return context.WithValue(ctx, permissionsContextKey, p.Permissions(catalog))
}

// FromContext returns the principal carried by ctx, if any.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey).(Principal)
	return p, ok
}

// PermissionsFromContext returns the permissions carried by ctx, if any.
func PermissionsFromContext(ctx context.Context) (map[string]bool, bool) {
	perms, ok := ctx.Value(permissionsContextKey).(map[string]bool)
	return perms, ok
}

// Catalog is the read-only role catalog. It is built once from
// configuration and consulted on every request; it is never written to at
// runtime, so it is safe for concurrent use.
type Catalog struct {
	byRole map[string][]string
}

// NewCatalog builds a catalog from the configured role permissions. Actions
// are copied so later mutation of the input map cannot affect the catalog.
//
// Role names are trimmed like actions are. A role name is compared against
// the names a token asserts, so a name carrying stray whitespace is a role no
// token can ever reach, and the operator sees a configured role that silently
// grants nothing. A name that is blank once trimmed names nobody and is
// dropped.
func NewCatalog(rolePermissions map[string][]string) Catalog {
	byRole := make(map[string][]string, len(rolePermissions))
	for role, actions := range rolePermissions {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		trimmed := make([]string, 0, len(actions))
		for _, action := range actions {
			action = strings.TrimSpace(action)
			if action == "" {
				continue
			}
			trimmed = append(trimmed, action)
		}
		byRole[role] = trimmed
	}
	return Catalog{byRole: byRole}
}

// CatalogFromConfig builds the catalog from the loaded configuration.
func CatalogFromConfig(cfg configuration.Auth) Catalog {
	return NewCatalog(cfg.RolePermissions)
}

// PermissionsFor returns the actions granted by a role. A role that is not
// in the catalog returns nil.
func (c Catalog) PermissionsFor(role string) []string {
	return c.byRole[role]
}

// HasRole reports whether the catalog knows a role.
func (c Catalog) HasRole(role string) bool {
	_, ok := c.byRole[role]
	return ok
}

// Roles returns every role name in the catalog, sorted for stable output.
func (c Catalog) Roles() []string {
	out := make([]string, 0, len(c.byRole))
	for role := range c.byRole {
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

// KnownActions returns every resource-action this build gates on, so a
// deployment can be checked against the same list the gates use rather than
// a second copy that drifts. The wildcard is not included: it is the
// deliberate bypass, not an endpoint.
func KnownActions() []string {
	actions := []string{
		ActionDefinitionsRead,
		ActionDefinitionsWrite,
		ActionSecretsRead,
		ActionSecretsWrite,
		ActionInstancesCreate,
		ActionInstancesRead,
		ActionInstancesUpdateContext,
		ActionInputDeliver,
		ActionInstancesControl,
		ActionStatisticsRead,
		ActionRolesRead,
		ActionSchedulesRead,
		ActionSchedulesWrite,
	}
	sort.Strings(actions)
	return actions
}

// Seed describes one role and the actions it grants, as stored in the
// role_permissions table. The catalog is the source of truth; the table is
// an identity cache seeded from it.
type Seed struct {
	Roles       []string
	Permissions map[string][]string
}

// Seed returns the role/action rows to upsert at startup. Permissions are
// deduplicated and sorted so the seed is deterministic.
func (c Catalog) Seed() Seed {
	roles := c.Roles()
	perms := make(map[string][]string, len(roles))
	for _, role := range roles {
		seen := make(map[string]bool, len(c.byRole[role]))
		var actions []string
		for _, action := range c.byRole[role] {
			if seen[action] {
				continue
			}
			seen[action] = true
			actions = append(actions, action)
		}
		sort.Strings(actions)
		perms[role] = actions
	}
	return Seed{Roles: roles, Permissions: perms}
}

// SystemPrincipal is the service principal used by the API-token path and by
// the broker input consumers, which carry no credential of their own. The
// caller supplies the audit actor the bypass acts as. It carries the admin
// role so identity listings name a role; authorization still bypasses on
// the Service flag rather than on that role.
func SystemPrincipal(userID string) Principal {
	return Principal{
		Name:    "system",
		Roles:   []string{"admin"},
		Service: true,
		UserID:  userID,
	}
}

// UserMetadata is the identity-cache metadata row for a principal: the
// provider subject plus the last-seen roles. It is debug-only and is never
// read back for authorization, which always reads the live token claims.
type UserMetadata struct {
	Subject string   `json:"subject,omitempty"`
	Issuer  string   `json:"issuer,omitempty"`
	Roles   []string `json:"roles,omitempty"`
}

// MarshalPrincipalUser renders the identity-cache metadata for a principal.
func MarshalPrincipalUser(p Principal) json.RawMessage {
	raw, err := json.Marshal(UserMetadata{Subject: p.Subject, Issuer: p.Issuer, Roles: p.Roles})
	if err != nil {
		return json.RawMessage("{}")
	}
	return raw
}
