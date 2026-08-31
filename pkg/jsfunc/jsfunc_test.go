package jsfunc_test

import (
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/pkg/jsfunc"
)

func TestRegisterAndGet(t *testing.T) {
	r := jsfunc.New()
	double := func(x int) int { return x * 2 }
	if err := r.Register("double", double); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, ok := r.Get("double")
	if !ok {
		t.Fatal("Get(double) not found")
	}
	if got == nil {
		t.Fatal("Get(double) = nil")
	}
}

func TestRegisterRejectsInvalidNames(t *testing.T) {
	r := jsfunc.New()
	fn := func() {}
	for _, name := range []string{"", "not a name", "1starts-with-digit", "dash-name", "has space"} {
		if err := r.Register(name, fn); err == nil {
			t.Errorf("Register(%q) error = nil, want error", name)
		}
	}
	if err := r.Register("ok_name_1", fn); err != nil {
		t.Errorf("Register(ok_name_1) error = %v, want nil", err)
	}
}

func TestRegisterRejectsNonFunction(t *testing.T) {
	r := jsfunc.New()
	if err := r.Register("nope", 42); err == nil {
		t.Error("Register(non-func) error = nil, want error")
	}
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	r := jsfunc.New()
	fn := func() {}
	if err := r.Register("dup", fn); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("dup", fn); err == nil {
		t.Error("Register(dup) second time error = nil, want error")
	}
}

func TestAll(t *testing.T) {
	r := jsfunc.New()
	_ = r.Register("a", func() {})
	_ = r.Register("b", func() {})
	all := r.All()
	if len(all) != 2 {
		t.Errorf("All() len = %d, want 2", len(all))
	}
	for _, name := range []string{"a", "b"} {
		if _, ok := all[name]; !ok {
			t.Errorf("All() missing %q", name)
		}
		if !strings.HasPrefix(name, "") {
			t.Error("unreachable")
		}
	}
}
