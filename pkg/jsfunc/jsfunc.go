// Package jsfunc is a registry of Go functions exposed to Goja scripts under
// the "go" root object. Services register their own functions; none are
// registered by default.
package jsfunc

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"sync"
)

var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Registry maps function names to Go functions.
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]any
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{funcs: map[string]any{}}
}

// Register adds fn under name. The name must be a valid JavaScript
// identifier and fn must be a function; duplicates are rejected.
func (r *Registry) Register(name string, fn any) error {
	if !identifierRe.MatchString(name) {
		return fmt.Errorf("jsfunc: %q is not a valid function name", name)
	}
	if fn == nil || reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("jsfunc: %q must be a function", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.funcs[name]; exists {
		return fmt.Errorf("jsfunc: function %q already registered", name)
	}
	r.funcs[name] = fn
	return nil
}

// Get returns the registered function by name.
func (r *Registry) Get(name string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.funcs[name]
	return fn, ok
}

// All returns a copy of the registry, suitable for exposing to Goja.
func (r *Registry) All() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]any, len(r.funcs))
	for k, v := range r.funcs {
		out[k] = v
	}
	return out
}

// Names lists the registered function names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.funcs))
	for k := range r.funcs {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
