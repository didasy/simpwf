package service

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

const SecretMask = "********"

// SecretSnapshotter is the internal-only source used to freeze secrets into
// newly created workflow instances. HTTP services never expose this method.
type SecretSnapshotter interface {
	GetAll(ctx context.Context) (map[string]string, error)
}

func mergeSecretSnapshot(obj map[string]any, secrets map[string]string) {
	if obj == nil {
		return
	}
	delete(obj, "secret")
	if len(secrets) == 0 {
		return
	}
	values := make(map[string]any, len(secrets))
	for key, value := range secrets {
		values[key] = value
	}
	obj["secret"] = values
}

func decodeContextObject(raw json.RawMessage) (map[string]any, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, false
	}
	return obj, true
}

func redactSecretRoot(obj map[string]any) {
	raw, exists := obj["secret"]
	if !exists {
		return
	}
	values, ok := raw.(map[string]any)
	if !ok {
		obj["secret"] = map[string]any{}
		return
	}
	for key := range values {
		values[key] = SecretMask
	}
}

func redactJSONValues(raw json.RawMessage, values map[string]string) json.RawMessage {
	if len(raw) == 0 || len(values) == 0 {
		return raw
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return json.RawMessage("null")
	}
	decoded = redactDecodedValues(decoded, values)
	out, err := json.Marshal(decoded)
	if err != nil {
		return raw
	}
	return out
}

func redactDecodedValues(value any, secrets map[string]string) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			typed[key] = redactDecodedValues(child, secrets)
		}
		return typed
	case []any:
		for i, child := range typed {
			typed[i] = redactDecodedValues(child, secrets)
		}
		return typed
	case string:
		return redactText(typed, secrets)
	default:
		return value
	}
}

func redactInstanceView(inst *model.WorkflowInstance) *model.WorkflowInstance {
	if inst == nil {
		return nil
	}
	copy := *inst
	if len(copy.Context) == 0 || bytes.Equal(bytes.TrimSpace(copy.Context), []byte("null")) {
		if copy.Error != "" {
			copy.Error = SecretMask
		}
		return &copy
	}
	obj, ok := decodeContextObject(copy.Context)
	if !ok {
		copy.Context = json.RawMessage(`{}`)
		if copy.Error != "" {
			copy.Error = SecretMask
		}
		return &copy
	}
	secrets, trusted := secretValuesFromObject(obj)
	redactSecretRoot(obj)
	redacted, err := json.Marshal(obj)
	if err != nil {
		copy.Context = json.RawMessage(`{}`)
		if copy.Error != "" {
			copy.Error = SecretMask
		}
		return &copy
	}
	if trusted {
		copy.Error = redactText(copy.Error, secrets)
	} else if copy.Error != "" {
		copy.Error = SecretMask
	}
	copy.Context = redacted
	return &copy
}

func secretValuesFromObject(obj map[string]any) (map[string]string, bool) {
	raw, exists := obj["secret"]
	if !exists {
		return nil, true
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		out[key] = text
	}
	return out, true
}

func secretValuesFromContext(raw json.RawMessage) (map[string]string, bool) {
	obj, ok := decodeContextObject(raw)
	if !ok {
		return nil, false
	}
	return secretValuesFromObject(obj)
}

func redactInstanceListItems(items []model.WorkflowInstance, contexts map[string]json.RawMessage) {
	for i := range items {
		if items[i].Error == "" {
			continue
		}
		secrets, trusted := secretValuesFromContext(contexts[items[i].ID])
		if trusted {
			items[i].Error = redactText(items[i].Error, secrets)
		} else {
			items[i].Error = SecretMask
		}
	}
}

func redactText(value string, secrets map[string]string) string {
	for _, plaintext := range secrets {
		if plaintext != "" {
			value = strings.ReplaceAll(value, plaintext, SecretMask)
		}
	}
	return value
}

func redactNodeDebugSecrets(detail *NodeDebugDetail, secrets map[string]string, trusted bool) {
	if detail == nil {
		return
	}
	if !trusted {
		detail.ContextBefore = json.RawMessage("null")
		detail.ContextAfter = json.RawMessage("null")
		detail.Input = json.RawMessage("null")
		detail.Output = json.RawMessage("null")
		if detail.Error != nil {
			*detail.Error = SecretMask
		}
		for _, text := range []*string{detail.RecoveryPolicy, detail.RecoveryResult} {
			if text != nil {
				*text = SecretMask
			}
		}
		return
	}
	detail.ContextBefore = redactJSONValues(detail.ContextBefore, secrets)
	detail.ContextAfter = redactJSONValues(detail.ContextAfter, secrets)
	detail.Input = redactJSONValues(detail.Input, secrets)
	detail.Output = redactJSONValues(detail.Output, secrets)
	if detail.Error != nil {
		*detail.Error = redactText(*detail.Error, secrets)
	}
	for _, text := range []*string{detail.RecoveryPolicy, detail.RecoveryResult} {
		if text == nil {
			continue
		}
		*text = redactText(*text, secrets)
	}
}
