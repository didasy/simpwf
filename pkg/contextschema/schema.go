// Package contextschema infers a JSON Schema (draft 2020-12) from a
// decoded context snapshot and renders it as a TypeScript declaration for
// frontend autocomplete. It never emits literal values or `any`: unknown
// shapes collapse to `unknown`.
package contextschema

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Caps keep the rendered declaration bounded: deeper levels collapse to
// unknown first, and the final string never exceeds maxTypeScriptBytes.
const (
	maxDepth           = 8
	maxKeysPerObject   = 50
	maxTypeScriptBytes = 32 * 1024
)

// DecodeSnapshot decodes a raw context snapshot into a generic value.
func DecodeSnapshot(raw json.RawMessage) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// InferJSONSchema infers a draft 2020-12 JSON Schema from a decoded
// snapshot. The top-level required list carries the present keys; a
// top-level non-object yields an empty schema.
func InferJSONSchema(v any) map[string]any {
	out := inferType(v, 0)
	obj, ok := out.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	if _, ok := obj["properties"]; !ok {
		return map[string]any{}
	}
	return obj
}

// RenderTypeScript renders the snapshot as an inline structural
// `declare const context` declaration. A top-level non-object renders as
// unknown.
func RenderTypeScript(v any) string {
	body := renderType(v, 0)
	if _, ok := v.(map[string]any); !ok {
		body = "unknown"
	}
	out := "declare const context: " + body + ";"
	if len(out) > maxTypeScriptBytes {
		out = collapseToFit(v)
	}
	return out
}

func inferType(v any, depth int) any {
	switch typed := v.(type) {
	case nil:
		return map[string]any{"type": "null"}
	case string:
		return map[string]any{"type": "string"}
	case bool:
		return map[string]any{"type": "boolean"}
	case float64, int, int64, json.Number:
		return map[string]any{"type": "number"}
	case []any:
		if len(typed) == 0 {
			return map[string]any{"type": "array", "items": map[string]any{}}
		}
		if depth >= maxDepth {
			return map[string]any{"type": "array", "items": map[string]any{}}
		}
		seen := map[string]any{}
		order := []string{}
		for _, item := range typed {
			s := inferType(item, depth+1)
			key := schemaKey(s)
			if _, ok := seen[key]; !ok {
				seen[key] = s
				order = append(order, key)
			}
		}
		sort.Strings(order)
		items := make([]any, 0, len(order))
		for _, key := range order {
			items = append(items, seen[key])
		}
		if len(items) == 1 {
			return map[string]any{"type": "array", "items": items[0]}
		}
		return map[string]any{"type": "array", "items": map[string]any{"anyOf": items}}
	case map[string]any:
		if depth >= maxDepth || len(typed) == 0 {
			return map[string]any{"type": "object"}
		}
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > maxKeysPerObject {
			keys = keys[:maxKeysPerObject]
		}
		props := make(map[string]any, len(keys))
		for _, k := range keys {
			props[k] = inferType(typed[k], depth+1)
		}
		return map[string]any{"type": "object", "properties": props, "required": append([]string(nil), keys...)}
	default:
		return map[string]any{}
	}
}

func schemaKey(s any) string {
	raw, err := json.Marshal(s)
	if err != nil {
		return fmt.Sprintf("%T", s)
	}
	return string(raw)
}

func renderType(v any, depth int) string {
	switch typed := v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64, int, int64, json.Number:
		return "number"
	case []any:
		if len(typed) == 0 {
			return "unknown[]"
		}
		if depth >= maxDepth {
			return "unknown[]"
		}
		seen := map[string]bool{}
		order := []string{}
		for _, item := range typed {
			t := renderType(item, depth+1)
			if !seen[t] {
				seen[t] = true
				order = append(order, t)
			}
		}
		sort.Strings(order)
		if len(order) == 1 {
			return order[0] + "[]"
		}
		return "(" + strings.Join(order, " | ") + ")[]"
	case map[string]any:
		return renderObject(typed, depth)
	default:
		return "unknown"
	}
}

func renderObject(obj map[string]any, depth int) string {
	if len(obj) == 0 || depth >= maxDepth {
		if depth >= maxDepth && len(obj) > 0 {
			return "unknown"
		}
		return "Record<string, unknown>"
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > maxKeysPerObject {
		keys = keys[:maxKeysPerObject]
	}
	var sb strings.Builder
	sb.WriteString("{ ")
	for _, k := range keys {
		writeKey(&sb, k)
		sb.WriteString(": ")
		sb.WriteString(renderType(obj[k], depth+1))
		sb.WriteString("; ")
	}
	sb.WriteString("}")
	return sb.String()
}

func writeKey(sb *strings.Builder, key string) {
	if isIdentifier(key) {
		sb.WriteString(key)
		return
	}
	sb.WriteString(strconvQuote(key))
}

// isIdentifier reports whether key is a valid TypeScript identifier.
func isIdentifier(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		switch {
		case r == '_' || r == '$':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func strconvQuote(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			sb.WriteByte('\\')
			sb.WriteRune(r)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// collapseToFit re-renders with progressively shallower depth until the
// output fits the byte cap, collapsing the deepest levels first.
func collapseToFit(v any) string {
	for depth := maxDepth - 1; depth >= 0; depth-- {
		body := renderTypeAtDepth(v, 0, depth)
		if _, ok := v.(map[string]any); !ok {
			body = "unknown"
		}
		out := "declare const context: " + body + ";"
		if len(out) <= maxTypeScriptBytes {
			return out
		}
	}
	return "declare const context: unknown;"
}

func renderTypeAtDepth(v any, depth, limit int) string {
	if depth >= limit {
		switch v.(type) {
		case map[string]any:
			return "unknown"
		case []any:
			return "unknown[]"
		}
	}
	switch typed := v.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return "Record<string, unknown>"
		}
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > maxKeysPerObject {
			keys = keys[:maxKeysPerObject]
		}
		var sb strings.Builder
		sb.WriteString("{ ")
		for _, k := range keys {
			writeKey(&sb, k)
			sb.WriteString(": ")
			sb.WriteString(renderTypeAtDepth(typed[k], depth+1, limit))
			sb.WriteString("; ")
		}
		sb.WriteString("}")
		return sb.String()
	default:
		return renderType(v, depth)
	}
}
