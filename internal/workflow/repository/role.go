package repository

import (
	"context"
	"errors"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrRoleNotFound is returned when no role row matches the lookup.
var ErrRoleNotFound = errors.New("repository: role not found")

// RoleRepository reads the role catalog and seeds it from configuration.
type RoleRepository interface {
	// Seed inserts or refreshes every configured role and its permissions
	// in one transaction, then prunes the permission rows of those roles
	// that the configuration no longer grants. It never deletes a role: a
	// role removed from the configuration keeps its row, so an in-flight
	// token referencing it still resolves, it simply grants nothing.
	Seed(ctx context.Context, roles []model.Role, permissions []model.RolePermission) error
	// List returns every role with its permissions, ordered by name.
	List(ctx context.Context) ([]model.Role, map[string][]string, error)
	// Get returns one role with its permissions.
	Get(ctx context.Context, name string) (model.Role, []string, error)
}

type roleRepository struct {
	db *gorm.DB
}

// NewRoleRepository builds the role repository.
func NewRoleRepository(db *gorm.DB) RoleRepository {
	return &roleRepository{db: db}
}

func (r *roleRepository) Seed(ctx context.Context, roles []model.Role, permissions []model.RolePermission) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, role := range roles {
			m := RoleToModel(role)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "name"}},
				DoUpdates: clause.AssignmentColumns([]string{"description", "updated_at"}),
			}).Create(&m).Error; err != nil {
				return err
			}
		}
		// wanted is the exact permission set this seed claims, so the prune
		// below can tell a stale row from a current one without re-reading
		// configuration.
		wanted := make(map[string]map[string]bool, len(roles))
		for _, role := range roles {
			wanted[role.Name] = make(map[string]bool)
		}
		for _, p := range permissions {
			perm := RolePermissionToModel(p)
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "role"}, {Name: "action"}},
				DoUpdates: clause.AssignmentColumns([]string{"updated_at"}),
			}).Create(&perm).Error; err != nil {
				return err
			}
			if actions, ok := wanted[perm.Role]; ok {
				actions[perm.Action] = true
			}
		}
		return pruneRolePermissions(tx, wanted)
	})
}

// pruneRolePermissions deletes the permission rows of the seeded roles that
// this seed did not grant, so the audit copy of the catalog cannot keep
// advertising a grant an operator has removed from a role that still exists.
//
// The delete is scoped to the roles named in wanted. A role this seed does
// not mention keeps its rows entirely, including when it was dropped from
// configuration: the plan keeps the role row so an in-flight token still
// resolves the name, and its stale grants are equally part of that history.
// Nothing consults the table to authorize a request, so a leftover row
// grants nothing, and the read APIs serve the catalog, so it is not
// advertised either. A role named with no actions is the explicit "this role
// now grants nothing" case and has all of its rows removed.
func pruneRolePermissions(tx *gorm.DB, wanted map[string]map[string]bool) error {
	for role, actions := range wanted {
		q := tx.Where("role = ?", role)
		if len(actions) > 0 {
			placeholders := make([]string, 0, len(actions))
			args := make([]any, 0, len(actions)+1)
			for action := range actions {
				placeholders = append(placeholders, "?")
				args = append(args, action)
			}
			args = append(args, role)
			q = q.Where("action NOT IN ("+strings.Join(placeholders, ",")+") AND role = ?", args...)
		}
		if err := q.Delete(&RolePermissionModel{}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (r *roleRepository) List(ctx context.Context) ([]model.Role, map[string][]string, error) {
	var rows []RoleModel
	if err := r.db.WithContext(ctx).Order("name").Find(&rows).Error; err != nil {
		return nil, nil, err
	}
	perms, err := r.permissionMap(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	out := make([]model.Role, 0, len(rows))
	for _, m := range rows {
		out = append(out, RoleFromModel(m))
	}
	return out, perms, nil
}

func (r *roleRepository) Get(ctx context.Context, name string) (model.Role, []string, error) {
	var m RoleModel
	if err := r.db.WithContext(ctx).Where("name = ?", name).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Role{}, nil, ErrRoleNotFound
		}
		return model.Role{}, nil, err
	}
	perms, err := r.permissionMap(ctx, name)
	if err != nil {
		return model.Role{}, nil, err
	}
	return RoleFromModel(m), perms[name], nil
}

// permissionMap returns role name to ordered actions, restricted to one
// role when name is non-empty.
func (r *roleRepository) permissionMap(ctx context.Context, name string) (map[string][]string, error) {
	q := r.db.WithContext(ctx).Model(&RolePermissionModel{}).Order("role, action")
	if name != "" {
		q = q.Where("role = ?", name)
	}
	var rows []RolePermissionModel
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(rows))
	for _, row := range rows {
		out[row.Role] = append(out[row.Role], row.Action)
	}
	return out, nil
}
