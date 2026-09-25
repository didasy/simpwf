package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

func TestRedactContextSecrets(t *testing.T) {
	inst := &model.WorkflowInstance{Context: json.RawMessage(`{"run_id":"r1","secret":{"API_KEY":"plain-value","TOKEN":"other"}}`)}
	redacted := redactInstanceView(inst).Context
	if strings.Contains(string(redacted), "plain-value") || strings.Contains(string(redacted), "other") {
		t.Fatalf("plaintext remained: %s", redacted)
	}
	var got map[string]any
	if err := json.Unmarshal(redacted, &got); err != nil {
		t.Fatal(err)
	}
	secret := got["secret"].(map[string]any)
	if secret["API_KEY"] != SecretMask || secret["TOKEN"] != SecretMask || got["run_id"] != "r1" {
		t.Fatalf("redacted = %s", redacted)
	}
}

func TestRedactionMasksNonMapLegacySecretRoot(t *testing.T) {
	inst := &model.WorkflowInstance{Context: json.RawMessage(`{"secret":"plain-value"}`)}
	if got := string(redactInstanceView(inst).Context); got != `{"secret":{}}` {
		t.Fatalf("legacy secret root = %s", got)
	}
}

func TestMergeSecretSnapshot(t *testing.T) {
	obj := map[string]any{"secret": map[string]any{"API_KEY": "request-value"}}
	mergeSecretSnapshot(obj, map[string]string{"API_KEY": "stored-value"})
	secret := obj["secret"].(map[string]any)
	if secret["API_KEY"] != "stored-value" {
		t.Fatalf("secret = %#v", secret)
	}
	delete(obj, "secret")
	mergeSecretSnapshot(obj, nil)
	if _, exists := obj["secret"]; exists {
		t.Fatal("empty snapshot must not preserve caller-supplied secret root")
	}
}

func TestRedactionFailsClosedOnMalformedPayloads(t *testing.T) {
	inst := &model.WorkflowInstance{Context: json.RawMessage(`{"secret":{"API_KEY":"plain-value"}`)}
	if got := string(redactInstanceView(inst).Context); got != `{}` {
		t.Fatalf("malformed instance context = %s, want {}", got)
	}
	if got := string(redactJSONValues(json.RawMessage(`plain-value`), map[string]string{"API_KEY": "plain-value"})); got != `null` {
		t.Fatalf("malformed debug JSON = %s, want null", got)
	}
}

func TestRedactInstanceListErrors(t *testing.T) {
	items := []model.WorkflowInstance{{ID: "instance-1", Error: "Bearer plain-value"}}
	redactInstanceListItems(items, map[string]json.RawMessage{
		"instance-1": json.RawMessage(`{"secret":{"API_KEY":"plain-value"}}`),
	})
	if strings.Contains(items[0].Error, "plain-value") || !strings.Contains(items[0].Error, SecretMask) {
		t.Fatalf("list error leaked plaintext: %s", items[0].Error)
	}
}

func TestRedactInstanceViewMasksError(t *testing.T) {
	inst := &model.WorkflowInstance{
		Context: json.RawMessage(`{"secret":{"API_KEY":"plain-value"}}`),
		Error:   "request failed with Bearer plain-value",
	}
	redacted := redactInstanceView(inst)
	if strings.Contains(string(redacted.Context), "plain-value") || strings.Contains(redacted.Error, "plain-value") {
		t.Fatalf("instance view leaked plaintext: context=%s error=%s", redacted.Context, redacted.Error)
	}
	if !strings.Contains(redacted.Error, SecretMask) {
		t.Fatalf("instance error missing mask: %s", redacted.Error)
	}
}

func TestRedactInstanceViewFailsClosedOnMalformedContext(t *testing.T) {
	inst := &model.WorkflowInstance{
		Context: json.RawMessage(`{"secret":{"API_KEY":"plain-value"}`),
		Error:   "request failed with plain-value",
	}
	redacted := redactInstanceView(inst)
	if string(redacted.Context) != `{}` || redacted.Error != SecretMask {
		t.Fatalf("malformed context = %s, error = %q", redacted.Context, redacted.Error)
	}
}

func TestRedactInstanceViewFailsClosedOnMalformedSecretRoot(t *testing.T) {
	for _, contextRaw := range []json.RawMessage{
		json.RawMessage(`null`),
		json.RawMessage(`{"secret":"plain-value"}`),
		json.RawMessage(`{"secret":{"API_KEY":{"nested":"plain-value"}}}`),
	} {
		inst := &model.WorkflowInstance{Context: contextRaw, Error: "request failed with plain-value"}
		redacted := redactInstanceView(inst)
		if redacted.Error != SecretMask {
			t.Fatalf("context %s left error unmasked: %q", contextRaw, redacted.Error)
		}
	}
}

func TestRedactInstanceListErrorsFailsClosedOnMissingContext(t *testing.T) {
	items := []model.WorkflowInstance{
		{ID: "missing", Error: "request failed"},
		{ID: "malformed", Error: "request failed"},
		{ID: "null", Error: "request failed"},
		{ID: "root", Error: "request failed"},
		{ID: "value", Error: "request failed"},
		{ID: "ordinary", Error: "request failed"},
	}
	contexts := map[string]json.RawMessage{
		"malformed": json.RawMessage(`{"secret":`),
		"null":      json.RawMessage(`null`),
		"root":      json.RawMessage(`{"secret":"plain-value"}`),
		"value":     json.RawMessage(`{"secret":{"API_KEY":{"nested":"plain-value"}}}`),
		"ordinary":  json.RawMessage(`{"result":1}`),
	}
	redactInstanceListItems(items, contexts)
	for _, i := range []int{0, 1, 2, 3, 4} {
		if items[i].Error != SecretMask {
			t.Fatalf("untrusted %s list error = %q", items[i].ID, items[i].Error)
		}
	}
	if items[5].Error != "request failed" {
		t.Fatalf("ordinary list error = %q", items[5].Error)
	}
}

func TestRedactNodeDebugSecrets(t *testing.T) {
	d := &NodeDebugDetail{
		ContextBefore: json.RawMessage(`{"secret":{"API_KEY":"plain-value"}}`),
		ContextAfter:  json.RawMessage(`{"secret":{"API_KEY":"plain-value"},"out":1}`),
		Input:         json.RawMessage(`{"authorization":"Bearer plain-value"}`),
		Output:        json.RawMessage(`{"result":"plain-value"}`),
		Error:         stringPtr("request failed with plain-value"),
	}
	redactNodeDebugSecrets(d, map[string]string{"API_KEY": "plain-value"}, true)
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "plain-value") {
		t.Fatalf("node debug leaked plaintext: %s", encoded)
	}
	if !strings.Contains(string(encoded), "********") {
		t.Fatalf("node debug missing mask: %s", encoded)
	}
}

func TestRedactNodeDebugSecretsFailsClosedWithoutTrustedSnapshot(t *testing.T) {
	d := &NodeDebugDetail{
		ContextBefore:  json.RawMessage(`{"before":"plain-value"}`),
		ContextAfter:   json.RawMessage(`{"after":"plain-value"}`),
		Input:          json.RawMessage(`{"input":"plain-value"}`),
		Output:         json.RawMessage(`{"output":"plain-value"}`),
		Error:          stringPtr("request failed with plain-value"),
		RecoveryPolicy: stringPtr("policy plain-value"),
		RecoveryResult: stringPtr("result plain-value"),
	}
	redactNodeDebugSecrets(d, nil, false)
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "plain-value") || strings.Contains(string(encoded), "********") == false {
		t.Fatalf("node debug fail-closed output = %s", encoded)
	}
}

func TestUpdateContextRejectsUntrustedStoredSecretSnapshot(t *testing.T) {
	for _, contextRaw := range []json.RawMessage{
		json.RawMessage(`{"secret":`),
		json.RawMessage(`{"secret":"plain-value"}`),
		json.RawMessage(`{"secret":{"API_KEY":{"nested":"plain-value"}}}`),
	} {
		fake := &contextUpdateRepo{instance: &model.WorkflowInstance{ID: "instance-1", Context: contextRaw}}
		svc := &instanceService{instances: fake, actor: "system"}
		_, err := svc.UpdateContext(context.Background(), UpdateContext{
			InstanceID: "instance-1",
			Context:    json.RawMessage(`{"next":1}`),
		})
		if !errors.Is(err, model.ErrConflict) {
			t.Fatalf("context %s: UpdateContext() error = %v, want conflict", contextRaw, err)
		}
		if fake.replaceCalls != 0 {
			t.Fatalf("context %s: ReplaceContext() called %d times", contextRaw, fake.replaceCalls)
		}
	}
}

type contextUpdateRepo struct {
	repository.InstanceRepository
	instance     *model.WorkflowInstance
	replaceCalls int
}

func (r *contextUpdateRepo) GetByID(context.Context, string) (*model.WorkflowInstance, error) {
	return r.instance, nil
}

func (r *contextUpdateRepo) ReplaceContext(_ context.Context, update repository.ContextUpdate) (*model.WorkflowInstance, error) {
	r.replaceCalls++
	r.instance.Context = update.Context
	return r.instance, nil
}

func stringPtr(value string) *string { return &value }
