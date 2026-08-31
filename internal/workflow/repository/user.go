package repository

import (
	"context"
	"errors"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
)

// UpsertSystemUser inserts the configured audit actor, or updates its name,
// email, and metadata if it already exists. It never duplicates.
func UpsertSystemUser(ctx context.Context, db *gorm.DB, u model.User) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing UserModel
		err := tx.Where("id = ?", u.ID).First(&existing).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			m := UserToModel(u)
			return tx.Create(&m).Error
		case err != nil:
			return err
		default:
			existing.Name = u.Name
			existing.Email = u.Email
			existing.Metadata = jsonCol(u.Metadata, "{}")
			return tx.Save(&existing).Error
		}
	})
}
