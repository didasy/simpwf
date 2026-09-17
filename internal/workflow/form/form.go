// Package form validates input payloads against an input node's JSON Schema
// contract before script validation runs.
package form

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

// Validate checks payload against the node's form schema. It returns nil when
// the node carries no form (legacy path: script validation alone applies).
func Validate(node *model.NodeContent, payload []byte) error {
	if node == nil || node.Form == nil || len(node.Form.Schema) == 0 {
		return nil
	}
	var schemaDoc any
	if err := json.Unmarshal(node.Form.Schema, &schemaDoc); err != nil {
		return fmt.Errorf("form schema is invalid: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource("form.json", schemaDoc); err != nil {
		return fmt.Errorf("form schema is invalid: %w", err)
	}
	sch, err := c.Compile("form.json")
	if err != nil {
		return fmt.Errorf("form schema is invalid: %w", err)
	}
	var payloadDoc any
	if err := json.Unmarshal(payload, &payloadDoc); err != nil {
		return fmt.Errorf("input body must be valid JSON: %w", err)
	}
	if err := sch.Validate(payloadDoc); err != nil {
		return fmt.Errorf("schema validation failed: %s", joinMessages(err))
	}
	return nil
}

// joinMessages flattens nested validation errors into a human-readable list.
func joinMessages(err error) string {
	var ve *jsonschema.ValidationError
	if e, ok := err.(*jsonschema.ValidationError); ok {
		ve = e
	} else {
		return err.Error()
	}
	var msgs []string
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			msgs = append(msgs, stripRootLocation(e.Error()))
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	if len(msgs) == 0 {
		return err.Error()
	}
	return strings.Join(msgs, "; ")
}

// stripRootLocation drops the empty root JSON Pointer prefix emitted by
// the validator for errors on the payload root (e.g. a missing required
// property). The empty location carries no information; nested locations
// like "at '/address': ..." are kept.
func stripRootLocation(msg string) string {
	return strings.TrimPrefix(msg, "at '': ")
}
