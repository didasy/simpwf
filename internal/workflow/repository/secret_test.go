package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

func TestSecretRoundTripAndDelete(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewSecretRepository(db)

	created, err := repo.Create(ctx, "API_KEY", "plain-value")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Key != "API_KEY" {
		t.Fatalf("Create() = %+v", created)
	}
	got, err := repo.GetByKey(ctx, "API_KEY")
	if err != nil {
		t.Fatalf("GetByKey() error = %v", err)
	}
	if got.Key != "API_KEY" {
		t.Fatalf("GetByKey() = %+v", got)
	}
	if err := repo.Delete(ctx, "API_KEY"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.GetByKey(ctx, "API_KEY"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("GetByKey() after delete error = %v, want ErrNotFound", err)
	}
}

func TestSecretDuplicateCreateConflict(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewSecretRepository(db)

	if _, err := repo.Create(ctx, "API_KEY", "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Create(ctx, "API_KEY", "second"); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("duplicate Create() error = %v, want ErrConflict", err)
	}
}

func TestSecretMissingOperationsNotFound(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewSecretRepository(db)

	if _, err := repo.GetByKey(ctx, "MISSING"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("GetByKey() error = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, "MISSING"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("Delete() error = %v, want ErrNotFound", err)
	}
}

func TestSecretListPaginationAndOrder(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewSecretRepository(db)

	for _, key := range []string{"ZED", "ALPHA", "MID"} {
		if _, err := repo.Create(ctx, key, "value-"+key); err != nil {
			t.Fatal(err)
		}
	}

	items, total, err := repo.List(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(items) != 2 || items[0].Key != "ALPHA" || items[1].Key != "MID" {
		t.Fatalf("List() = keys %q, total %d", []string{items[0].Key, items[1].Key}, total)
	}
	items, total, err = repo.List(ctx, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(items) != 1 || items[0].Key != "ZED" {
		t.Fatalf("second List() = %+v, total %d", items, total)
	}
}

func TestSecretGetAllSnapshot(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := repository.NewSecretRepository(db)

	if _, err := repo.Create(ctx, "B", "two"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Create(ctx, "A", "one"); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["A"] != "one" || got["B"] != "two" {
		t.Fatalf("GetAll() = %#v", got)
	}
}
