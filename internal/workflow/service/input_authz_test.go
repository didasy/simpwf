package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// gateCatalog mirrors the seeded catalog: finance may deliver but holds
// nothing else, and manager holds no permissions at all so it can only ever
// reach a node that names it.
var gateCatalog = auth.NewCatalog(map[string][]string{
	"finance": {auth.ActionInputDeliver, auth.ActionInstancesRead},
})

const gateUserID = "22222222-2222-7222-8222-222222222222"

// parkedInstance creates a one-input workflow with the given extra node
// fields, starts an instance, and drives it until it parks on the input.
func parkedInstance(t *testing.T, extra map[string]any) (*gorm.DB, service.InstanceService, string) {
	t.Helper()
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	node := map[string]any{"channel": "http", "output_property": "webhook"}
	for k, v := range extra {
		node[k] = v
	}
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", node),
	)
	// The instance is created as the caller the gate tests deliver as, so
	// the object check the service applies on every delivery sees the
	// owner rather than a stranger.
	inst, err := svc.Create(ctx, service.CreateInstance{
		WorkflowDefinitionID: wfID,
		Context:              json.RawMessage(`{}`),
		Actor:                gateUserID,
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting || cur.WaitingReason != model.WaitingReasonInput {
		t.Fatalf("instance = %+v, want waiting on input", cur)
	}
	return db, svc, inst.ID
}

// deliverAs delivers a payload as the given principal.
func deliverAs(svc service.InstanceService, instanceID string, key string, payload string, p *auth.Principal) (*model.InputDelivery, error) {
	return svc.DeliverInput(context.Background(), service.DeliverInput{
		InstanceID:     instanceID,
		IdempotencyKey: key,
		Payload:        []byte(payload),
		Principal:      p,
	})
}

// svcInsertScriptNode adds a node the engine never executes for this
// instance, so a row can be made to point at a node that is in the graph
// but is not an input node. It returns the new node instance id.
func svcInsertScriptNode(t *testing.T, db *gorm.DB, instanceID string) string {
	t.Helper()
	now := time.Now().UTC()
	row := repository.NodeInstanceModel{
		ID:                 "22222222-2222-7222-8222-222222222299",
		WorkflowInstanceID: instanceID,
		// A node id that is deliberately absent from the definition, so the
		// graph lookup fails before the type is ever compared.
		NodeID:    "99999999-9999-7999-8999-999999999998",
		Name:      "helper",
		Type:      string(model.NodeTypeScript),
		Status:    string(model.NodeFinished),
		Input:     datatypes.JSON([]byte(`null`)),
		Output:    datatypes.JSON([]byte(`null`)),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("insert helper node instance: %v", err)
	}
	return row.ID
}

// TestInputDualGate walks the whole matrix: the two gates are independent,
// a service principal bypasses both, and a denial writes no delivery row.
func TestInputDualGate(t *testing.T) {
	finance := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	managerOnly := &auth.Principal{UserID: gateUserID, Roles: []string{"manager"}}
	roleLess := &auth.Principal{UserID: gateUserID}
	service1 := &auth.Principal{UserID: gateUserID, Service: true}

	cases := []struct {
		name        string
		allowed     []string
		principal   *auth.Principal
		wantErr     bool
		wantAllowed bool
	}{
		// Node open, caller may deliver: accepted.
		{"open node accepts a permitted caller", nil, finance, false, true},
		// The endpoint gate is required even on an open node: an open node
		// is not an open endpoint.
		{"open node refuses a role-less caller", nil, roleLess, true, false},
		// Node open, caller lacks the endpoint permission: refused there.
		{"open node refuses a caller without the permission", nil, managerOnly, true, false},
		// Node closed to manager, finance holds the permission but not the
		// role: this is the case the second gate exists for.
		{"closed node refuses a permitted caller with the wrong role", []string{"manager"}, finance, true, false},
		{"closed node accepts a listed role", []string{"manager"}, &auth.Principal{UserID: gateUserID, Roles: []string{"manager", "finance"}}, false, true},
		{"closed node accepts the endpoint permission holder when listed", []string{"finance"}, finance, false, true},
		// An unknown role still matches a node that names it, but it fails
		// the endpoint gate first, so the delivery is still refused.
		{"unknown role fails the endpoint gate", []string{"wizard"}, &auth.Principal{UserID: gateUserID, Roles: []string{"wizard"}}, true, false},
		// The service principal bypasses both gates, whatever the node says.
		{"service bypasses a closed node", []string{"manager"}, service1, false, true},
		{"service bypasses an open node", nil, service1, false, true},
		// No principal at all is the broker/internal path: the service
		// principal, so it bypasses both.
		{"absent principal is the service principal", []string{"manager"}, nil, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var extra map[string]any
			if tc.allowed != nil {
				extra = map[string]any{"allowed_roles": tc.allowed}
			}
			db, svc, instanceID := parkedInstance(t, extra)
			delivery, err := deliverAs(svc, instanceID, "key-1", `{"success":true}`, tc.principal)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("DeliverInput() error = nil, want a refusal")
				}
				if !errors.Is(err, model.ErrForbidden) {
					t.Fatalf("DeliverInput() error = %v, want ErrForbidden so the handler answers 403", err)
				}
				assertNoDelivery(t, db, instanceID)
				return
			}
			if err != nil {
				t.Fatalf("DeliverInput() error = %v, want accepted", err)
			}
			if delivery.Accepted != tc.wantAllowed {
				t.Errorf("Accepted = %v, want %v", delivery.Accepted, tc.wantAllowed)
			}
		})
	}
}

// A refusal must not leave a delivery row behind, and must leave an
// input_forbidden audit event naming the instance, node, and roles.
func TestInputGateDenialAuditsWithoutPayload(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{
			"channel":         "http",
			"output_property": "webhook",
			"allowed_roles":   []string{"manager"},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`), Actor: gateUserID})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting {
		t.Fatalf("instance = %+v, want waiting", cur)
	}

	const secret = "s3cr3t-value"
	p := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	if _, err := deliverAs(svc, inst.ID, "denied-1", `{"card":"`+secret+`"}`, p); !errors.Is(err, model.ErrForbidden) {
		t.Fatalf("DeliverInput() error = %v, want ErrForbidden", err)
	}
	assertNoDelivery(t, db, inst.ID)

	events := loadEvents(t, db, inst.ID)
	found := false
	for _, e := range events {
		if e.Type != "input_forbidden" {
			continue
		}
		found = true
		if e.CreatedBy != gateUserID {
			t.Errorf("event created_by = %q, want the caller %q", e.CreatedBy, gateUserID)
		}
		if !json.Valid(e.Data) {
			t.Errorf("event data is not valid JSON: %s", e.Data)
			continue
		}
		var data map[string]any
		_ = json.Unmarshal(e.Data, &data)
		if data["node_id"] != n1 {
			t.Errorf("event node_id = %v, want %s", data["node_id"], n1)
		}
		roles, _ := data["roles"].([]any)
		if len(roles) != 1 || roles[0] != "finance" {
			t.Errorf("event roles = %v, want the refused caller roles", data["roles"])
		}
		// The payload is what was refused; it must not be recorded.
		if containsAny(string(e.Data), secret) {
			t.Errorf("audit event leaked the refused payload: %s", e.Data)
		}
	}
	if !found {
		t.Error("no input_forbidden audit event was recorded")
	}
}

// The instance stays parked after a refusal: a later permitted delivery
// must still be accepted under a fresh idempotency key.
func TestInputGateDenialLeavesInstanceWaiting(t *testing.T) {
	_, svc, instanceID := parkedInstance(t, map[string]any{"allowed_roles": []string{"manager"}})

	refused := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	if _, err := deliverAs(svc, instanceID, "denied-1", `{"ok":true}`, refused); !errors.Is(err, model.ErrForbidden) {
		t.Fatalf("DeliverInput() error = %v, want ErrForbidden", err)
	}

	permitted := &auth.Principal{UserID: gateUserID, Roles: []string{"manager", "finance"}}
	delivery, err := deliverAs(svc, instanceID, "ok-1", `{"ok":true}`, permitted)
	if err != nil {
		t.Fatalf("DeliverInput() error = %v, want the retry to succeed", err)
	}
	if !delivery.Accepted {
		t.Errorf("delivery = %+v, want accepted", delivery)
	}
}

// record_actor writes the attribution envelope; without it the bare payload
// is written exactly as before. The flag is per node, so a definition that
// predates it keeps producing the shape its templates read.
func TestRecordActorEnvelope(t *testing.T) {
	cases := []struct {
		name        string
		recordActor bool
	}{
		{"legacy bare payload", false},
		{"attribution envelope", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, svc, instanceID := parkedInstance(t, map[string]any{"record_actor": tc.recordActor})
			p := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
			delivery, err := deliverAs(svc, instanceID, "key-1", `{"approved":true,"amount":10}`, p)
			if err != nil {
				t.Fatalf("DeliverInput() error = %v", err)
			}
			if !delivery.Accepted {
				t.Fatalf("delivery = %+v, want accepted", delivery)
			}

			got, err := svc.GetContext(context.Background(), instanceID, auth.Principal{})
			if err != nil {
				t.Fatalf("GetContext() error = %v", err)
			}
			var ctxMap map[string]any
			if err := json.Unmarshal(got.Context, &ctxMap); err != nil {
				t.Fatalf("decode context: %v", err)
			}
			value, ok := ctxMap["webhook"].(map[string]any)
			if !tc.recordActor {
				if !ok {
					t.Fatalf("webhook = %#v, want the bare payload as an object", ctxMap["webhook"])
				}
				if value["approved"] != true {
					t.Errorf("legacy context lost the payload fields: %#v", value)
				}
				return
			}
			if !ok {
				t.Fatalf("webhook = %#v, want the attribution envelope as an object", ctxMap["webhook"])
			}
			// Exactly two keys: the deliverer and the payload.
			if len(value) != 2 {
				t.Errorf("envelope has %d keys (%v), want exactly user_id and input_data", len(value), value)
			}
			if value["user_id"] != gateUserID {
				t.Errorf("envelope user_id = %v, want %q", value["user_id"], gateUserID)
			}
			payload, ok := value["input_data"].(map[string]any)
			if !ok {
				t.Fatalf("envelope input_data = %#v, want the raw payload", value["input_data"])
			}
			if payload["approved"] != true || payload["amount"] != float64(10) {
				t.Errorf("envelope input_data = %#v, want the untouched payload", payload)
			}
		})
	}
}

// A service delivery is still attributed: the envelope records the system
// user rather than being skipped.
func TestRecordActorForServicePrincipalUsesSystemUser(t *testing.T) {
	_, svc, instanceID := parkedInstance(t, map[string]any{"record_actor": true})
	// No principal is the broker path, which acts as the configured system
	// user.
	delivery, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, nil)
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted", delivery)
	}
	got, _ := svc.GetContext(context.Background(), instanceID, auth.Principal{})
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	envelope, ok := ctxMap["webhook"].(map[string]any)
	if !ok {
		t.Fatalf("webhook = %#v, want the envelope", ctxMap["webhook"])
	}
	if envelope["user_id"] != svcSysUserID {
		t.Errorf("envelope user_id = %v, want the system user %s", envelope["user_id"], svcSysUserID)
	}
}

// The form schema, the validation script, and the post hook all still see
// the raw payload under record_actor: only the context merge changes.
func TestRecordActorKeepsRawPayloadForValidationAndHooks(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	n2 := "11111111-1111-7111-8111-111111111102"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", n2, map[string]any{
			"channel":         "http",
			"output_property": "webhook",
			"record_actor":    true,
			"form": map[string]any{
				"schema": map[string]any{
					"type":     "object",
					"required": []string{"approved"},
					"properties": map[string]any{
						"approved": map[string]any{"type": "boolean"},
					},
				},
			},
			"validation": map[string]any{
				// The script sees the raw payload, not the envelope.
				"script": `var b = JSON.parse(input); if (typeof b.approved !== 'boolean') { return 'not a bool'; }`,
			},
			"post_script": map[string]any{
				// The frozen output global is the raw payload too.
				"script": `context.seen_approved = output.approved;`,
			},
		}),
		svcNodeJSON(n2, "script", "after", "return 1;", "", map[string]any{"output_property": "after"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`), Actor: gateUserID})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting {
		t.Fatalf("instance = %+v, want waiting", cur)
	}

	p := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	delivery, err := deliverAs(svc, inst.ID, "key-1", `{"approved":true}`, p)
	if err != nil {
		t.Fatalf("DeliverInput() error = %v", err)
	}
	if !delivery.Accepted {
		t.Fatalf("delivery = %+v, want accepted (validation saw the raw payload)", delivery)
	}
	cur = driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowFinished {
		t.Fatalf("status = %s, want finished (error %q)", cur.Status, cur.Error)
	}
	got, _ := svc.GetContext(ctx, inst.ID, auth.Principal{UserID: gateUserID})
	var ctxMap map[string]any
	_ = json.Unmarshal(got.Context, &ctxMap)
	if ctxMap["seen_approved"] != true {
		t.Errorf("post hook output global = %v, want the raw payload value true", ctxMap["seen_approved"])
	}
	envelope, _ := ctxMap["webhook"].(map[string]any)
	if envelope["user_id"] != gateUserID {
		t.Errorf("envelope user_id = %v, want %q", envelope["user_id"], gateUserID)
	}
}

// A schema rejection still answers 422 with accepted=false; the
// authorization gates must not change that.
func TestRecordActorKeepsSchemaRejectionAsUnprocessable(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{
			"channel":         "http",
			"output_property": "webhook",
			"record_actor":    true,
			"form": map[string]any{
				"schema": map[string]any{
					"type":       "object",
					"required":   []string{"approved"},
					"properties": map[string]any{"approved": map[string]any{"type": "boolean"}},
				},
			},
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`), Actor: gateUserID})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting {
		t.Fatalf("instance = %+v, want waiting", cur)
	}
	p := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	delivery, err := deliverAs(svc, inst.ID, "key-1", `{"approved":"yes"}`, p)
	if err != nil {
		t.Fatalf("DeliverInput() error = %v, want the rejection recorded, not returned", err)
	}
	if delivery.Accepted {
		t.Fatal("Accepted = true, want a rejected delivery")
	}
	if delivery.Error == "" {
		t.Error("Error is empty, want the schema failure message")
	}
}

// An idempotent replay returns the first writer's recorded delivery, so a
// retry by a different caller cannot rewrite the attribution. A stranger's
// replay never reaches the input gates: ownership is checked first, so the
// key alone does not hand one caller another caller's delivery.
func TestRecordActorReplayKeepsFirstWriter(t *testing.T) {
	_, svc, instanceID := parkedInstance(t, map[string]any{"record_actor": true})
	first := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	payload := `{"ok":true}`
	if _, err := deliverAs(svc, instanceID, "same-key", payload, first); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	second := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	delivery, err := deliverAs(svc, instanceID, "same-key", payload, second)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !delivery.Accepted {
		t.Errorf("replay delivery = %+v, want the stored accepted delivery", delivery)
	}
}

// A replay is still an authorization decision: reusing the same
// idempotency key with the same payload returns the stored delivery to a
// caller the gates permit, while a caller the gates refuse gets a refusal.
// The accepted delivery stays untouched.
func TestInputReplayReauthorizes(t *testing.T) {
	db, svc, instanceID := parkedInstance(t, nil)

	permitted := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
	first, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, permitted)
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if !first.Accepted {
		t.Fatalf("delivery = %+v, want accepted", first)
	}

	// Same key, same payload, but a caller the endpoint gate refuses.
	roleLess := &auth.Principal{UserID: gateUserID}
	replay, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, roleLess)
	if !errors.Is(err, model.ErrForbidden) {
		t.Fatalf("replay error = %v, want ErrForbidden so the handler answers 403", err)
	}
	if replay != nil {
		t.Errorf("replay = %+v, want no delivery returned to a refused caller", replay)
	}

	// A permitted replay still returns the stored delivery.
	again, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, permitted)
	if err != nil {
		t.Fatalf("permitted replay: %v", err)
	}
	if again.ID != first.ID {
		t.Errorf("permitted replay = %+v, want the stored delivery %s", again, first.ID)
	}

	// The refusal is audited and does not disturb the recorded delivery.
	events := loadEvents(t, db, instanceID)
	denied := 0
	for _, e := range events {
		if e.Type == "input_forbidden" {
			denied++
		}
	}
	if denied != 1 {
		t.Errorf("input_forbidden events = %d, want exactly one for the refused replay", denied)
	}
}

// A replay fails closed: when the row's target node no longer resolves, or
// resolves to something that is not an input node, the stored delivery is
// not handed back even to a caller the gates would otherwise permit. The
// node is the authorization input, so an unresolvable one denies.
func TestInputReplayFailsClosedOnUnresolvableNode(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(t *testing.T, db *gorm.DB, instanceID string)
	}{
		{
			name: "node instance row is gone",
			corrupt: func(t *testing.T, db *gorm.DB, instanceID string) {
				if err := db.Where("workflow_instance_id = ?", instanceID).
					Delete(&repository.NodeInstanceModel{}).Error; err != nil {
					t.Fatalf("delete node instances: %v", err)
				}
			},
		},
		{
			name: "node id is not in the graph",
			corrupt: func(t *testing.T, db *gorm.DB, instanceID string) {
				const ghost = "99999999-9999-7999-8999-999999999999"
				if err := db.Model(&repository.NodeInstanceModel{}).
					Where("workflow_instance_id = ?", instanceID).
					Update("node_id", ghost).Error; err != nil {
					t.Fatalf("repoint node instance: %v", err)
				}
			},
		},
		{
			name: "target node is not an input node",
			// The row records a node the delivery never really targeted, so
			// its target no longer names the input the gate authorizes.
			corrupt: func(t *testing.T, db *gorm.DB, instanceID string) {
				other := svcInsertScriptNode(t, db, instanceID)
				if err := db.Model(&repository.InputDeliveryModel{}).
					Where("workflow_instance_id = ?", instanceID).
					Update("node_instance_id", other).Error; err != nil {
					t.Fatalf("repoint delivery: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, svc, instanceID := parkedInstance(t, nil)
			permitted := &auth.Principal{UserID: gateUserID, Roles: []string{"finance"}}
			first, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, permitted)
			if err != nil {
				t.Fatalf("first delivery: %v", err)
			}
			if !first.Accepted {
				t.Fatalf("delivery = %+v, want accepted", first)
			}
			tc.corrupt(t, db, instanceID)

			replay, err := deliverAs(svc, instanceID, "key-1", `{"ok":true}`, permitted)
			if !errors.Is(err, model.ErrForbidden) {
				t.Fatalf("replay error = %v, want ErrForbidden so the handler answers 403", err)
			}
			if replay != nil {
				t.Errorf("replay = %+v, want no delivery returned when the target node is unresolvable", replay)
			}
		})
	}
}

// The status response tells the frontend about both new fields, so it can
// build the right form and explain a refusal before it happens.
func TestPendingInputExposesGateAndEnvelopeShape(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{
			"channel":         "http",
			"output_property": "webhook",
			"allowed_roles":   []string{"manager", "finance"},
			"record_actor":    true,
		}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`), Actor: gateUserID})
	if err != nil {
		t.Fatal(err)
	}
	cur := driveEngine(t, db, inst.ID)
	if cur.Status != model.WorkflowWaiting {
		t.Fatalf("instance = %+v, want waiting", cur)
	}
	detail, err := svc.GetStatusDetail(ctx, inst.ID, auth.Principal{UserID: gateUserID})
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	if detail.PendingInput == nil {
		t.Fatal("PendingInput = nil, want the waiting-input contract")
	}
	if len(detail.PendingInput.AllowedRoles) != 2 {
		t.Errorf("AllowedRoles = %v, want both roles", detail.PendingInput.AllowedRoles)
	}
	if !detail.PendingInput.RecordActor {
		t.Error("RecordActor = false, want true")
	}
}

// An open node reports an empty role list rather than null, so a frontend
// can read it without a nil check.
func TestPendingInputOpenNodeReportsEmptyRoles(t *testing.T) {
	db := setupSvcDB(t)
	ctx := context.Background()
	svc := svcInstanceServiceWithCatalog(db, gateCatalog)

	n1 := "11111111-1111-7111-8111-111111111101"
	wfID := svcCreateWorkflow(t, db, n1,
		svcNodeJSON(n1, "input", "ask", "", "", map[string]any{"channel": "http"}),
	)
	inst, err := svc.Create(ctx, service.CreateInstance{WorkflowDefinitionID: wfID, Context: json.RawMessage(`{}`), Actor: gateUserID})
	if err != nil {
		t.Fatal(err)
	}
	driveEngine(t, db, inst.ID)
	detail, err := svc.GetStatusDetail(ctx, inst.ID, auth.Principal{UserID: gateUserID})
	if err != nil {
		t.Fatalf("GetStatusDetail() error = %v", err)
	}
	if detail.PendingInput == nil {
		t.Fatal("PendingInput = nil")
	}
	if len(detail.PendingInput.AllowedRoles) != 0 {
		t.Errorf("AllowedRoles = %v, want empty for an open node", detail.PendingInput.AllowedRoles)
	}
	if detail.PendingInput.RecordActor {
		t.Error("RecordActor = true, want false for a node that did not opt in")
	}
}
