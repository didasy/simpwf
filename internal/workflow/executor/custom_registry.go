// Package executor runs workflow node content: scripts, condition groups,
// input validation, outbound HTTP calls and external commands, each under
// the security limits configured for the engine.
package executor

import (
	"fmt"
	"sort"
	"sync"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
	"github.com/sirupsen/logrus"
)

// CustomDeps carries the injected dependencies available to custom node
// executors: the same power builtin executors get, no new iface to learn.
type CustomDeps struct {
	// Limits mirrors the executor security limits (allowlist, output cap,
	// redirects).
	Limits Limits
	// Logger is the application logger; may be nil in tests.
	Logger *logrus.Logger
	// HTTP is the shared outbound HTTP client enforcing the executor
	// security policy. Built once by NewExecutors, shared with pollers.
	HTTP *HTTPExecutor
	// Funcs is the script function registry; may be nil.
	Funcs *jsfunc.Registry
}

// CustomFactory builds the executor for one custom node type from the
// injected deps. Factory errors are startup errors, not per-request errors.
type CustomFactory func(CustomDeps) (Executor, error)

var customFactoryMu sync.RWMutex

// customFactories maps custom node type names to their executor factories.
var customFactories = map[string]CustomFactory{}

// RegisterCustomFactory adds a custom node executor factory. Duplicate
// registrations, builtin collisions, empty names, and nil factories are
// rejected. Name-shape validation lives in the customnode facade; this
// registry only guards emptiness and builtin collisions.
func RegisterCustomFactory(nodeType string, factory CustomFactory) error {
	if factory == nil {
		return fmt.Errorf("custom executor factory for %q must be non-nil", nodeType)
	}
	if nodeType == "" {
		return fmt.Errorf("custom executor factory requires a non-empty node type")
	}
	if model.IsBuiltinNodeType(nodeType) {
		return fmt.Errorf("custom executor factory %q collides with a builtin node type", nodeType)
	}
	customFactoryMu.Lock()
	defer customFactoryMu.Unlock()
	if _, exists := customFactories[nodeType]; exists {
		return fmt.Errorf("custom executor factory %q is already registered", nodeType)
	}
	customFactories[nodeType] = factory
	return nil
}

// UnregisterCustomFactory removes a custom executor factory. It exists for
// facade rollback and tests; production code never unregisters.
func UnregisterCustomFactory(nodeType string) {
	customFactoryMu.Lock()
	defer customFactoryMu.Unlock()
	delete(customFactories, nodeType)
}

// LookupCustomFactory returns the factory for a registered custom node type.
func LookupCustomFactory(nodeType string) (CustomFactory, bool) {
	customFactoryMu.RLock()
	defer customFactoryMu.RUnlock()
	f, ok := customFactories[nodeType]
	return f, ok
}

// CustomFactoryTypes lists registered custom executor factory names in
// sorted order.
func CustomFactoryTypes() []string {
	customFactoryMu.RLock()
	defer customFactoryMu.RUnlock()
	out := make([]string, 0, len(customFactories))
	for t := range customFactories {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// BuildCustomExecutors instantiates one executor per registered custom type.
// A factory error aborts the build and names the failing type.
func BuildCustomExecutors(deps CustomDeps) (map[model.NodeType]Executor, error) {
	return BuildCustomExecutorsExcept(deps, nil)
}

// BuildCustomExecutorsExcept instantiates executors for the registered
// custom types, skipping any type named in exclude. Exclude exists so
// tests can build a subset without interference from other packages'
// globally registered factories.
func BuildCustomExecutorsExcept(deps CustomDeps, exclude []string) (map[model.NodeType]Executor, error) {
	skip := make(map[string]bool, len(exclude))
	for _, t := range exclude {
		skip[t] = true
	}
	customFactoryMu.RLock()
	factories := make(map[string]CustomFactory, len(customFactories))
	for t, f := range customFactories {
		if !skip[t] {
			factories[t] = f
		}
	}
	customFactoryMu.RUnlock()
	names := make([]string, 0, len(factories))
	for t := range factories {
		names = append(names, t)
	}
	sort.Strings(names)
	out := make(map[model.NodeType]Executor, len(names))
	for _, t := range names {
		ex, err := factories[t](deps)
		if err != nil {
			return nil, fmt.Errorf("custom node type %q: build executor: %w", t, err)
		}
		if ex == nil {
			return nil, fmt.Errorf("custom node type %q: factory returned nil executor", t)
		}
		out[model.NodeType(t)] = ex
	}
	return out, nil
}
