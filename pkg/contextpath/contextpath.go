// Package contextpath parses and navigates relative JSON paths such as
// "data1", "user.name", and "items[0]", and renders typed templates that
// interpolate those paths.
package contextpath

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// ErrPathNotFound is returned when a path does not resolve in the context.
var ErrPathNotFound = errors.New("contextpath: path not found")

// Segment is one step of a path: a map key, or an array index (Index set).
type Segment struct {
	Key   string
	Index *int
}

// Path is a parsed sequence of segments.
type Path []Segment

var (
	keyRe   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	tokenRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(\[([0-9]+)\])?$`)
	tplRe   = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)
)

// Parse splits a relative context path into segments.
func Parse(path string) (Path, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("contextpath: empty path")
	}
	parts := strings.Split(path, ".")
	var out Path
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("contextpath: empty segment in %q", path)
		}
		m := tokenRe.FindStringSubmatch(part)
		if m == nil {
			return nil, fmt.Errorf("contextpath: invalid segment %q in %q", part, path)
		}
		out = append(out, Segment{Key: m[1]})
		if m[3] != "" {
			idx, err := strconv.Atoi(m[3])
			if err != nil {
				return nil, fmt.Errorf("contextpath: invalid index %q in %q", m[3], path)
			}
			out = append(out, Segment{Key: m[3], Index: &idx})
		}
	}
	return out, nil
}

// Get resolves path against ctx.
func Get(ctx map[string]any, path string) (any, error) {
	p, err := Parse(path)
	if err != nil {
		return nil, err
	}
	return getPath(ctx, p)
}

func getPath(v any, p Path) (any, error) {
	if len(p) == 0 {
		return v, nil
	}
	seg := p[0]
	switch current := v.(type) {
	case map[string]any:
		if seg.Index != nil {
			return nil, fmt.Errorf("%w: %q is not an array", ErrPathNotFound, seg.Key)
		}
		next, ok := current[seg.Key]
		if !ok {
			return nil, fmt.Errorf("%w: key %q", ErrPathNotFound, seg.Key)
		}
		return getPath(next, p[1:])
	case []any:
		if seg.Index == nil {
			return nil, fmt.Errorf("%w: %q is not a map", ErrPathNotFound, seg.Key)
		}
		if *seg.Index < 0 || *seg.Index >= len(current) {
			return nil, fmt.Errorf("%w: index %d out of range", ErrPathNotFound, *seg.Index)
		}
		return getPath(current[*seg.Index], p[1:])
	default:
		return nil, fmt.Errorf("%w: cannot descend into %T", ErrPathNotFound, v)
	}
}

// Set writes value at path, creating missing map keys. Array index segments
// must already exist within range.
func Set(ctx map[string]any, path string, value any) error {
	p, err := Parse(path)
	if err != nil {
		return err
	}
	if len(p) == 0 {
		return errors.New("contextpath: empty path")
	}
	if err := setPath(ctx, p, value); err != nil {
		return err
	}
	return nil
}

func setPath(v any, p Path, value any) error {
	seg := p[0]
	switch current := v.(type) {
	case map[string]any:
		if seg.Index != nil {
			return fmt.Errorf("contextpath: %q is not an array", seg.Key)
		}
		if len(p) == 1 {
			current[seg.Key] = value
			return nil
		}
		next, ok := current[seg.Key]
		if !ok {
			if p[1].Index != nil {
				next = make([]any, *p[1].Index+1)
			} else {
				next = map[string]any{}
			}
			current[seg.Key] = next
		}
		return setPath(next, p[1:], value)
	case []any:
		if seg.Index == nil {
			return fmt.Errorf("contextpath: %q is not a map", seg.Key)
		}
		if *seg.Index < 0 || *seg.Index >= len(current) {
			return fmt.Errorf("contextpath: index %d out of range (len %d)", *seg.Index, len(current))
		}
		if len(p) == 1 {
			current[*seg.Index] = value
			return nil
		}
		return setPath(current[*seg.Index], p[1:], value)
	default:
		return fmt.Errorf("contextpath: cannot descend into %T", v)
	}
}

// RenderTemplate interpolates {{ path }} placeholders. When the whole string
// is a single placeholder the typed value is returned; otherwise the value is
// stringified into the surrounding text.
func RenderTemplate(s string, ctx map[string]any) (any, error) {
	matches := tplRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return s, nil
	}

	// single placeholder covering the whole string -> typed value
	if len(matches) == 1 {
		trimmed := strings.TrimSpace(s)
		if strings.HasPrefix(trimmed, "{{") && strings.HasSuffix(trimmed, "}}") && tplRe.MatchString(trimmed) {
			v, err := Get(ctx, strings.TrimSpace(matches[0][1]))
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}

	var sb strings.Builder
	rest := s
	for {
		loc := tplRe.FindStringIndex(rest)
		if loc == nil {
			sb.WriteString(rest)
			break
		}
		sb.WriteString(rest[:loc[0]])
		placeholder := rest[loc[0]:loc[1]]
		inner := tplRe.FindStringSubmatch(placeholder)[1]
		v, err := Get(ctx, strings.TrimSpace(inner))
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&sb, "%v", v)
		rest = rest[loc[1]:]
	}
	return sb.String(), nil
}

// RenderJSON renders {{ path }} placeholders inside a JSON document. Leaf
// strings that are a single placeholder keep their typed value; otherwise
// they are stringified into the surrounding text.
func RenderJSON(raw []byte, ctx map[string]any) ([]byte, error) {
	var v any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&v); err != nil {
		return nil, fmt.Errorf("contextpath: render json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, fmt.Errorf("contextpath: render json: %w", err)
	}
	rendered, err := renderValue(v, ctx)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("contextpath: marshal rendered json: %w", err)
	}
	return out, nil
}

func renderValue(v any, ctx map[string]any) (any, error) {
	switch val := v.(type) {
	case string:
		return RenderTemplate(val, ctx)
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, item := range val {
			rendered, err := renderValue(item, ctx)
			if err != nil {
				return nil, err
			}
			out[k] = rendered
		}
		return out, nil
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			rendered, err := renderValue(item, ctx)
			if err != nil {
				return nil, err
			}
			out[i] = rendered
		}
		return out, nil
	default:
		return v, nil
	}
}

// ValidKey reports whether s is a valid bare map key for a path.
func ValidKey(s string) bool { return keyRe.MatchString(s) }

// HasTemplate reports whether s contains a {{ path }} placeholder.
func HasTemplate(s string) bool { return tplRe.MatchString(s) }
