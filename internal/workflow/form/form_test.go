package form_test

import (
	"strings"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/form"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func formNode(t *testing.T, content string) *model.NodeContent {
	t.Helper()
	limits := model.NodeLimits{}
	nc, err := model.ParseNodeContent([]byte(content), limits)
	if err != nil {
		t.Fatalf("ParseNodeContent() error = %v", err)
	}
	return nc
}

func TestValidateAcceptsMatchingPayload(t *testing.T) {
	nc := formNode(t, `{"type":"input","channel":"http","output_property":"user",
		"form":{"schema":{"type":"object","required":["email"],"properties":{"email":{"type":"string"}}}}}`)
	if err := form.Validate(nc, []byte(`{"email":"a@b.c"}`)); err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRejectsMismatch(t *testing.T) {
	nc := formNode(t, `{"type":"input","channel":"http","output_property":"user",
		"form":{"schema":{"type":"object","required":["email"],"properties":{"email":{"type":"string"}}}}}`)
	err := form.Validate(nc, []byte(`{}`))
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "schema validation failed") {
		t.Errorf("error = %q, want schema validation failure", err)
	}
}

func TestValidateMissingPropertyOmitsRootLocation(t *testing.T) {
	nc := formNode(t, `{"type":"input","channel":"http","output_property":"user",
		"form":{"schema":{"type":"object","required":["title","body"],"properties":{"title":{"type":"string"},"body":{"type":"string"}}}}}`)
	err := form.Validate(nc, []byte(`{"title":"wow"}`))
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
	if strings.Contains(err.Error(), "at ''") {
		t.Errorf("error = %q, want no root location prefix", err)
	}
	if !strings.Contains(err.Error(), "missing property 'body'") {
		t.Errorf("error = %q, want missing property 'body'", err)
	}
}

func TestValidateNilWithoutForm(t *testing.T) {
	nc := formNode(t, `{"type":"input","channel":"http","output_property":"user"}`)
	if err := form.Validate(nc, []byte(`anything at all`)); err != nil {
		t.Errorf("Validate() error = %v, want nil for formless node", err)
	}
	if err := form.Validate(nil, []byte(`{}`)); err != nil {
		t.Errorf("Validate() error = %v, want nil for nil node", err)
	}
}
