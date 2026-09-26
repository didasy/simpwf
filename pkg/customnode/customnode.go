// Package customnode is the facade for custom workflow node types: it owns
// the naming rules and registers each node atomically in both the model
// validator registry and the executor factory registry.
//
// A node author creates a leaf package under pkg/customnode/<name> that
// imports this package and self-registers in init:
//
//	func init() {
//		customnode.MustRegister(customnode.Definition{
//			Type:     "s3fetch",
//			Validate: ValidateConfig,
//			Schema:   configSchema, // draft 2020-12 schema of the config object
//			New: func(d customnode.Deps) (executor.Executor, error) {
//				return &Executor{...}, nil
//			},
//		})
//	}
//
// This package imports model and executor but no node packages, so leaf
// packages can import it without creating a Go import cycle.
package customnode

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/jsfunc"
	"github.com/sirupsen/logrus"
)

// Definition describes one custom node type.
type Definition struct {
	// Type is the lowercase-identifier node type name (max 64 chars).
	Type string
	// Validate owns the node's config validation: it validates the raw
	// config object and returns its parsed form, stored on
	// NodeContent.Custom.
	Validate func(json.RawMessage) (any, error)
	// Schema is the mandatory draft 2020-12 JSON Schema of the config
	// object alone. The core wraps it into the full node envelope
	// (type const, config, shared common fields) and serves it on
	// definition reads so a frontend can render a generic form. It is
	// documentation only: Validate stays authoritative. An empty,
	// non-object, or non-compiling schema is rejected at registration.
	Schema json.RawMessage
	// New builds the node's executor from the injected deps.
	New func(Deps) (executor.Executor, error)
}

// Deps carries the injected dependencies available to custom node
// executors: the same power builtin executors get, no new iface to learn.
type Deps struct {
	// Limits mirrors the executor security limits (allowlist, output cap,
	// redirects).
	Limits executor.Limits
	// Logger is the application logger; may be nil in tests.
	Logger *logrus.Logger
	// HTTP is the shared outbound HTTP client enforcing the executor
	// security policy. Built once by NewExecutors, shared with pollers.
	HTTP *executor.HTTPExecutor
	// Funcs is the script function registry; may be nil.
	Funcs *jsfunc.Registry
}

// validate checks the definition fields before any registration.
func (d Definition) validate() error {
	if err := model.ValidateCustomTypeName(d.Type); err != nil {
		return err
	}
	if d.Validate == nil {
		return fmt.Errorf("custom node type %q requires a Validate function", d.Type)
	}
	if d.New == nil {
		return fmt.Errorf("custom node type %q requires a New function", d.Type)
	}
	return nil
}

// Register adds a custom node type atomically to the model validator and
// schema registries and the executor factory registry. If the executor half
// fails, the model entries roll back so no half-registered type survives.
func Register(d Definition) error {
	if err := d.validate(); err != nil {
		return err
	}
	if err := model.RegisterCustomType(d.Type, d.Validate); err != nil {
		return err
	}
	if err := model.RegisterCustomSchema(d.Type, d.Schema); err != nil {
		model.UnregisterCustomType(d.Type)
		return err
	}
	factory := func(deps executor.CustomDeps) (executor.Executor, error) {
		return d.New(Deps{
			Limits: deps.Limits,
			Logger: deps.Logger,
			HTTP:   deps.HTTP,
			Funcs:  deps.Funcs,
		})
	}
	if err := executor.RegisterCustomFactory(d.Type, factory); err != nil {
		model.UnregisterCustomType(d.Type)
		model.UnregisterCustomSchema(d.Type)
		return err
	}
	return nil
}

// MustRegister registers a custom node type and panics on error. It is
// intended for leaf-package init() functions, where an error is a
// programming bug that must fail fast at startup.
func MustRegister(d Definition) {
	if err := Register(d); err != nil {
		panic(fmt.Sprintf("customnode: register %q: %v", d.Type, err))
	}
}

// ByType returns the definition view for a registered custom node type.
func ByType(nodeType string) (Definition, bool) {
	validate, ok := model.LookupCustomType(nodeType)
	if !ok {
		return Definition{}, false
	}
	factory, ok := executor.LookupCustomFactory(nodeType)
	if !ok {
		return Definition{}, false
	}
	return Definition{
		Type:     nodeType,
		Validate: validate,
		Schema:   mustConfigSchema(nodeType),
		New: func(d Deps) (executor.Executor, error) {
			return factory(executor.CustomDeps{
				Limits: d.Limits,
				Logger: d.Logger,
				HTTP:   d.HTTP,
				Funcs:  d.Funcs,
			})
		},
	}, true
}

// mustConfigSchema returns the author-supplied config sub-schema of a
// registered type. Every type reaching ByType is registered, so a missing
// schema is a programming bug.
func mustConfigSchema(nodeType string) json.RawMessage {
	return model.LookupCustomConfigSchema(nodeType)
}

// Types lists registered custom node type names in sorted order.
func Types() []string {
	got := model.CustomTypes()
	sort.Strings(got)
	return got
}

// BuildExecutors instantiates one executor per registered custom type using
// the injected deps. A factory error aborts the build and names the failing
// type.
func BuildExecutors(d Deps) (map[model.NodeType]executor.Executor, error) {
	return executor.BuildCustomExecutors(executor.CustomDeps{
		Limits: d.Limits,
		Logger: d.Logger,
		HTTP:   d.HTTP,
		Funcs:  d.Funcs,
	})
}
