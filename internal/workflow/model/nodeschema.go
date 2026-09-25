package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// nodeSchemaDialect is the JSON Schema dialect every node schema declares.
const nodeSchemaDialect = "https://json-schema.org/draft/2020-12/schema"

// builtinNodeSchemas holds one hand-authored full node-object schema per
// builtin type, built once at package init. The Go parser (nodecontent.go)
// stays authoritative: these schemas document the accepted shape so a
// generic frontend can render forms without per-type hardcoding. They
// describe the inline workflow-occurrence shape; a node_definition_id
// reference carries only graph fields (see docs/integration/fields.md).
var builtinNodeSchemas = mustBuildBuiltinNodeSchemas()

// NodeSchema returns the full node-object JSON Schema for a node type,
// resolving registered custom types to their wrapped envelope. The bool is
// false for an unknown type, in which case reads degrade to a null schema
// instead of failing. The returned bytes are shared and must not be
// modified.
func NodeSchema(nodeType string) (json.RawMessage, bool) {
	if IsBuiltinNodeType(nodeType) {
		s, ok := builtinNodeSchemas[nodeType]
		return s, ok
	}
	entry, ok := lookupCustomSchemaEntry(nodeType)
	if !ok {
		return nil, false
	}
	return entry.full, true
}

// BuiltinNodeSchemas returns a copy of the builtin type to schema map, for
// documentation and tests. Custom types are resolved through NodeSchema.
func BuiltinNodeSchemas() map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(builtinNodeSchemas))
	for t, s := range builtinNodeSchemas {
		out[t] = s
	}
	return out
}

// sharedNodeDefs are the $defs injected into every node schema. They keep
// the hand-authored per-type schemas from repeating the same fragments.
func sharedNodeDefs() map[string]any {
	return map[string]any{
		"nodeId": map[string]any{
			"type":        "string",
			"format":      "uuid",
			"description": "Canonical lowercase uuid.",
		},
		"duration": map[string]any{
			"type":        "string",
			"minLength":   1,
			"description": "Go duration string such as 30s or 5m; must parse and be positive.",
		},
		"contextKey": map[string]any{
			"type":        "string",
			"description": "Bare context key receiving the node output; blank means the graph node id.",
		},
		"keys": map[string]any{
			"description": "Scope key table on a group node; the same shape is accepted at the workflow top level.",
			"type":        "object",
			"additionalProperties": map[string]any{
				"oneOf": []any{
					map[string]any{"$ref": "#/$defs/nodeId"},
					map[string]any{"type": "null"},
				},
				"description": "Target sibling node id, or null/empty to exit the scope when the key is selected.",
			},
		},
		"hook": map[string]any{
			"description": "Lifecycle hook script. An explicit null disables a hook inherited through node_definition_id.",
			"type":        []any{"object", "null"},
			"properties": map[string]any{
				"script":  map[string]any{"type": "string", "minLength": 1},
				"timeout": map[string]any{"$ref": "#/$defs/duration"},
			},
			"required":             []any{"script"},
			"additionalProperties": false,
		},
		"failureRoute": map[string]any{
			"description": "Fallback route used instead of failing the workflow when the node errors.",
			"type":        "object",
			"properties": map[string]any{
				"next_node":       map[string]any{"$ref": "#/$defs/nodeId"},
				"output_property": map[string]any{"type": "string", "minLength": 1},
			},
			"required":             []any{"next_node", "output_property"},
			"additionalProperties": false,
		},
	}
}

// mustBuildBuiltinNodeSchemas decodes the hand-authored per-type schemas,
// injects the shared $defs, expands the group children, and compile-checks
// every result. A broken hand-authored schema is a programming bug, so it
// panics at startup instead of serving bad documentation.
func mustBuildBuiltinNodeSchemas() map[string]json.RawMessage {
	authored := map[string]string{
		string(NodeTypeScript):       scriptNodeSchemaJSON,
		string(NodeTypeConditions):   conditionsNodeSchemaJSON,
		string(NodeTypeInput):        inputNodeSchemaJSON,
		string(NodeTypeGroup):        groupNodeSchemaJSON,
		string(NodeTypeExternalCall): externalCallNodeSchemaJSON,
		string(NodeTypeOutput):       outputNodeSchemaJSON,
		string(NodeTypePoller):       pollerNodeSchemaJSON,
	}
	docs := make(map[string]map[string]any, len(authored))
	for nodeType, raw := range authored {
		doc, err := decodeSchemaObject(json.RawMessage(raw))
		if err != nil {
			panic(fmt.Sprintf("model: %s node schema: %v", nodeType, err))
		}
		docs[nodeType] = doc
	}

	out := make(map[string]json.RawMessage, len(docs))

	// A group nests full node objects. The children are the same schemas
	// referenced through $defs, so the served document stays shallow
	// while nested groups remain describable through the groupNode ref.
	// groupBody is the bare group document; the served group document is a
	// copy of it carrying $defs, which keeps the two from forming a cycle
	// once groupNode sits among those same $defs.
	groupBody := docs[string(NodeTypeGroup)]
	groupBody["properties"].(map[string]any)["nodes"].(map[string]any)["items"] = map[string]any{"$ref": "#/$defs/childNode"}
	groupDefs := sharedNodeDefs()

	for nodeType, doc := range docs {
		if nodeType == string(NodeTypeGroup) {
			continue
		}
		// Each type is self-contained: it carries its own copy of the
		// shared defs, so a frontend fetching one type's schema never
		// resolves refs into another schema.
		doc["$schema"] = nodeSchemaDialect
		doc["$defs"] = sharedNodeDefs()
		raw, err := json.Marshal(doc)
		if err != nil {
			panic(fmt.Sprintf("model: marshal %s node schema: %v", nodeType, err))
		}
		if err := compileJSONSchema(raw); err != nil {
			panic(fmt.Sprintf("model: %s node schema: %v", nodeType, err))
		}
		out[nodeType] = raw
		groupDefs[nodeType+"Node"] = doc
	}

	groupDefs["groupNode"] = groupBody
	groupDefs["customNode"] = customNodeArm()
	groupDefs["childNode"] = map[string]any{
		"description": "Any node accepted inside a group.",
		"anyOf":       groupChildRefs(),
	}
	served := make(map[string]any, len(groupBody)+2)
	for k, v := range groupBody {
		served[k] = v
	}
	served["$schema"] = nodeSchemaDialect
	served["$defs"] = groupDefs
	groupRaw, err := json.Marshal(served)
	if err != nil {
		panic(fmt.Sprintf("model: marshal group node schema: %v", err))
	}
	if err := compileJSONSchema(groupRaw); err != nil {
		panic(fmt.Sprintf("model: group node schema: %v", err))
	}
	out[string(NodeTypeGroup)] = groupRaw
	return out
}

// customNodeArm is the generic catch-all for registered custom node types
// nested in a group. Custom type names are only known at runtime, so the
// arm documents the shared envelope and leaves the config object open;
// a frontend fetches the concrete schema from the definition read.
func customNodeArm() map[string]any {
	return map[string]any{
		"description": "Registered custom node type. Fetch the type's own schema from the node or workflow definition read for its config fields.",
		"type":        "object",
		"properties": map[string]any{
			"type": map[string]any{
				"type":        "string",
				"description": "A registered custom node type name.",
				"not":         map[string]any{"enum": builtinTypeNames()},
			},
			"config":     map[string]any{"type": "object", "description": "Custom node config object."},
			"timeout":    map[string]any{"$ref": "#/$defs/duration"},
			"on_failure": map[string]any{"$ref": "#/$defs/failureRoute"},
		},
		"required": []any{"type", "config"},
	}
}

// groupChildRefs lists one ref per builtin type plus the custom arm.
func groupChildRefs() []any {
	return []any{
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeScript) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeConditions) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeInput) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeGroup) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeExternalCall) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypeOutput) + "Node"},
		map[string]any{"$ref": "#/$defs/" + string(NodeTypePoller) + "Node"},
		map[string]any{"$ref": "#/$defs/customNode"},
	}
}

// builtinTypeNames lists the builtin type names in declaration order.
func builtinTypeNames() []string {
	return []string{
		string(NodeTypeScript), string(NodeTypeConditions), string(NodeTypeInput),
		string(NodeTypeGroup), string(NodeTypeExternalCall), string(NodeTypeOutput),
		string(NodeTypePoller),
	}
}

// configDefPrefix namespaces the author schema's own $defs once they are
// hoisted into the envelope, so a node type can use plain names like
// "question" without colliding with the shared defs.
const configDefPrefix = "config_"

// buildCustomNodeSchema wraps an author-supplied config sub-schema into the
// full node envelope: type const, config carrying the author schema, the
// shared common fields, and no builtin executable field. The envelope is
// built once at registration and served from the cache afterwards.
func buildCustomNodeSchema(nodeType string, config json.RawMessage) (json.RawMessage, error) {
	cfg, err := decodeSchemaObject(config)
	if err != nil {
		return nil, fmt.Errorf("config schema must be a JSON object: %w", err)
	}
	defs := sharedNodeDefs()
	hoistConfigDefs(cfg, defs)
	doc := map[string]any{
		"$schema": nodeSchemaDialect,
		"type":    "object",
		"title":   fmt.Sprintf("simpwf %s node", nodeType),
		"description": fmt.Sprintf(
			"Registered custom node type %q. Custom nodes carry only type and config plus the shared common fields; "+
				"every builtin executable field is rejected. Go parsers stay authoritative.", nodeType),
		"properties": map[string]any{
			"type":              map[string]any{"const": nodeType},
			"config":            cfg,
			"timeout":           map[string]any{"$ref": "#/$defs/duration"},
			"output_property":   map[string]any{"$ref": "#/$defs/contextKey"},
			"next_node":         map[string]any{"$ref": "#/$defs/nodeId"},
			"on_failure":        map[string]any{"$ref": "#/$defs/failureRoute"},
			"retry_on_recovery": map[string]any{"type": "boolean"},
			"id":                map[string]any{"$ref": "#/$defs/nodeId"},
			"name":              map[string]any{"type": "string"},
			"input_data":        map[string]any{"type": "string", "description": "Context path selecting the value exposed to the node as input."},
			"metadata":          map[string]any{"type": "object", "description": "Opaque author metadata; never read by the engine."},
			"pre_script":        map[string]any{"$ref": "#/$defs/hook"},
			"post_script":       map[string]any{"$ref": "#/$defs/hook"},
		},
		"required":             []any{"type", "config"},
		"additionalProperties": false,
		"$defs":                defs,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal node schema: %w", err)
	}
	if err := compileJSONSchema(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// hoistConfigDefs moves an author schema's own $defs into the envelope under
// a namespaced name and rewrites the refs that pointed at them. A config
// sub-schema is nested one level down in the envelope, where its own
// #/$defs no longer resolve; hoisting keeps internal refs working. Sub-
// schemas that declare no $defs are left untouched.
func hoistConfigDefs(cfg map[string]any, into map[string]any) {
	authorDefs, ok := cfg["$defs"].(map[string]any)
	if !ok || len(authorDefs) == 0 {
		return
	}
	hoisted := make(map[string]bool, len(authorDefs))
	for name, def := range authorDefs {
		into[configDefPrefix+name] = def
		hoisted[name] = true
	}
	rewriteConfigRefs(cfg, hoisted)
	delete(cfg, "$defs")
}

// rewriteConfigRefs repoints every local #/$defs/<name> ref in sub at the
// hoisted envelope entry. Author refs are always document-root relative,
// so the walk needs no path tracking.
func rewriteConfigRefs(node any, hoisted map[string]bool) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			if key == "$ref" {
				if ref, ok := value.(string); ok {
					if name, isLocal := strings.CutPrefix(ref, "#/$defs/"); isLocal && hoisted[name] {
						typed[key] = "#/$defs/" + configDefPrefix + name
					}
				}
				continue
			}
			rewriteConfigRefs(value, hoisted)
		}
	case []any:
		for _, value := range typed {
			rewriteConfigRefs(value, hoisted)
		}
	}
}

// decodeSchemaObject decodes raw JSON and requires a JSON object.
func decodeSchemaObject(raw json.RawMessage) (map[string]any, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("must be valid JSON: %w", err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("must be an object, got %T", doc)
	}
	return obj, nil
}

// compileJSONSchema compile-checks raw as a draft 2020-12 schema. It
// resolves $refs from the document alone, so a schema depending on a remote
// resource is rejected.
func compileJSONSchema(raw json.RawMessage) error {
	doc, err := decodeSchemaObject(raw)
	if err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource("node.json", doc); err != nil {
		return fmt.Errorf("not a valid JSON Schema: %w", err)
	}
	if _, err := c.Compile("node.json"); err != nil {
		return fmt.Errorf("not a valid JSON Schema: %w", err)
	}
	return nil
}

// sortedSchemaTypes lists node types in sorted order, for stable tests and
// documentation output.
func sortedSchemaTypes(schemas map[string]json.RawMessage) []string {
	out := make([]string, 0, len(schemas))
	for t := range schemas {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
