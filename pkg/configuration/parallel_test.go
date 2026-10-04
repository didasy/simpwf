package configuration_test

import (
	"testing"

	"github.com/simpwf/workflow-engine/pkg/configuration"
)

func TestLoadParallelDefaults(t *testing.T) {
	setenv(t, "SIMPWF_INFRA_POSTGRESQL_DSN", testDSN)

	cfg, err := configuration.Load(configuration.WithConfigFile(missingPath(t)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	p := cfg.Engine.Parallel
	if p.MaxDepth != 4 || p.MaxBranchesPerParallel != 32 || p.MaxActiveBranchesPerInstance != 128 {
		t.Fatalf("parallel = %+v, want 4/32/128", p)
	}
}

func TestLoadParallelFromFile(t *testing.T) {
	path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  parallel:
    max_depth: 2
    max_branches_per_parallel: 8
    max_active_branches_per_instance: 16
`)
	cfg, err := configuration.Load(configuration.WithConfigFile(path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	p := cfg.Engine.Parallel
	if p.MaxDepth != 2 || p.MaxBranchesPerParallel != 8 || p.MaxActiveBranchesPerInstance != 16 {
		t.Fatalf("parallel = %+v, want 2/8/16", p)
	}
}

func TestLoadParallelRejectsNonPositive(t *testing.T) {
	for _, key := range []string{"max_depth", "max_branches_per_parallel", "max_active_branches_per_instance"} {
		path := writeConfig(t, `
infra:
  postgresql:
    dsn: "file-dsn"
engine:
  parallel:
    max_depth: 4
    max_branches_per_parallel: 32
    max_active_branches_per_instance: 128
    `+key+`: 0
`)
		if _, err := configuration.Load(configuration.WithConfigFile(path)); err == nil {
			t.Errorf("%s: expected error, got nil", key)
		}
	}
}
