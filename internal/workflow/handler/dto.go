// Package handler owns Gin routes, HTTP DTOs, query parsing, health probes,
// and application/problem+json responses. HTTP DTOs live beside handlers;
// there is no global DTO package.
package handler

import (
	"encoding/json"
	"time"
)

// -- definitions --------------------------------------------------------------

// CreateWorkflowDefinitionRequest is the POST /v1/workflow/definition body.
type CreateWorkflowDefinitionRequest struct {
	Name              string          `json:"name"`
	PreviousVersionID *string         `json:"previous_version_id,omitempty"`
	Content           json.RawMessage `json:"content" swaggertype:"object"`
}

// WorkflowDefinitionResponse mirrors api/openapi.yaml.
type WorkflowDefinitionResponse struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Version           int             `json:"version"`
	PreviousVersionID *string         `json:"previous_version_id"`
	LineageID         string          `json:"lineage_id"`
	Content           json.RawMessage `json:"content" swaggertype:"object"`
	// Schemas holds the node-object JSON Schema of every node type this
	// definition actually uses, keyed by type. It is computed on read from
	// the current code, so it describes the current engine, not the code
	// the definition was authored against.
	Schemas   map[string]json.RawMessage `json:"schemas" swaggertype:"object"`
	CreatedBy string                     `json:"created_by"`
	UpdatedBy string                     `json:"updated_by"`
	CreatedAt time.Time                  `json:"created_at"`
	UpdatedAt time.Time                  `json:"updated_at"`
}

// CreateNodeDefinitionRequest is the POST /v1/node/definition body.
type CreateNodeDefinitionRequest struct {
	Name              string          `json:"name"`
	Type              string          `json:"type"`
	PreviousVersionID *string         `json:"previous_version_id,omitempty"`
	Content           json.RawMessage `json:"content" swaggertype:"object"`
}

// NodeDefinitionResponse mirrors api/openapi.yaml.
type NodeDefinitionResponse struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Version           int             `json:"version"`
	PreviousVersionID *string         `json:"previous_version_id"`
	LineageID         string          `json:"lineage_id"`
	Type              string          `json:"type"`
	Content           json.RawMessage `json:"content" swaggertype:"object"`
	// Schema is the full node-object JSON Schema for Type, or null when
	// the type is unknown to this build. It describes the inline
	// workflow-occurrence shape; content is the same object minus the
	// graph routing fields, which a frontend ignores here.
	Schema    json.RawMessage `json:"schema" swaggertype:"object"`
	CreatedBy string          `json:"created_by"`
	UpdatedBy string          `json:"updated_by"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// ListResponse is the envelope for definition lists.
type ListResponse[T any] struct {
	Items      []T   `json:"items"`
	Page       int   `json:"page"`
	PerPage    int   `json:"per_page"`
	Total      int64 `json:"total"`
	TotalPages int64 `json:"total_pages"`
}

// -- secrets ------------------------------------------------------------------

// CreateSecretRequest is the POST /v1/secrets body. The response never returns
// the plaintext value.
type CreateSecretRequest struct {
	Key   string `json:"key" validate:"required" pattern:"^[A-Za-z0-9_]{1,128}$"`
	Value string `json:"value" validate:"required" minLength:"1" maxLength:"8192"`
}

// SecretResponse is the constant-mask representation used by all secret reads.
type SecretResponse struct {
	Key         string    `json:"key" validate:"required" pattern:"^[A-Za-z0-9_]{1,128}$"`
	ValueMasked string    `json:"value_masked" validate:"required" enums:"********"`
	CreatedAt   time.Time `json:"created_at" validate:"required" format:"date-time"`
	UpdatedAt   time.Time `json:"updated_at" validate:"required" format:"date-time"`
}

// -- auth ---------------------------------------------------------------------

// AuthConfigResponse is the GET /v1/auth/config body. It is the public login
// contract a frontend needs to start a code+PKCE flow; it carries no secret.
// When Enabled is false the deployment has no OIDC login, and ApiTokenEnabled
// says whether the X-Api-Token service credential is the only way in.
type AuthConfigResponse struct {
	Enabled  bool   `json:"enabled"`
	Issuer   string `json:"issuer,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	// Audience is the value a token's aud claim must carry for this API. It
	// is the verifier's audience, so a frontend knows whether to request a
	// distinct resource audience or whether the client id already serves as
	// one.
	Audience         string   `json:"audience,omitempty"`
	AuthorizationURL string   `json:"authorization_url,omitempty"`
	TokenURL         string   `json:"token_url,omitempty"`
	RolesClaim       string   `json:"roles_claim,omitempty"`
	Scopes           []string `json:"scopes,omitempty"`
	ApiTokenEnabled  bool     `json:"api_token_enabled"`
	// Roles is every role name the configured catalog knows, so a frontend
	// can label a role before the caller has one.
	Roles []string `json:"roles"`
}

// AuthMeResponse is the GET /v1/auth/me body: the identity behind the
// credential, plus the effective permissions the UI can act on.
type AuthMeResponse struct {
	ID      string  `json:"id"`
	Subject *string `json:"subject"`
	Name    string  `json:"name"`
	Email   string  `json:"email"`
	// Roles are the roles the live token asserted. An empty array means the
	// caller is authenticated but holds no role.
	Roles []string `json:"roles"`
	// Service marks the API-token principal, which bypasses authorization.
	Service bool `json:"service"`
	// Permission is the union of the caller's role permissions, or ["*"]
	// for the service principal.
	Permission []string `json:"permissions"`
}

// RoleResponse is one catalog role with the actions it grants.
type RoleResponse struct {
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// RoleListResponse is the GET /v1/roles body.
type RoleListResponse struct {
	Items []RoleResponse `json:"items"`
}

// -- instances ----------------------------------------------------------------

// CreateInstanceRequest is the POST /v1/workflow/instance body.
type CreateInstanceRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Context              json.RawMessage `json:"context,omitempty"`
	// Debug starts a step-through run: paused at create, re-paused after
	// every node until termination. Immutable after create.
	Debug bool `json:"debug,omitempty"`
}

// CreateInstanceResponse is the 202 body.
type CreateInstanceResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// InstanceSummaryResponse is the compact list item for
// GET /v1/workflow/instance. It deliberately omits context, frame, counters,
// lease, and node-attempt fields.
type InstanceSummaryResponse struct {
	ID                   string     `json:"id"`
	WorkflowDefinitionID string     `json:"workflow_definition_id"`
	Status               string     `json:"status"`
	WaitingReason        *string    `json:"waiting_reason"`
	PauseRequested       bool       `json:"pause_requested"`
	TerminationPending   bool       `json:"termination_pending"`
	Debug                bool       `json:"debug"`
	Error                *string    `json:"error"`
	StartedAt            *time.Time `json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at"`
	CreatedBy            string     `json:"created_by"`
	UpdatedBy            string     `json:"updated_by"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// InstanceStatusResponse mirrors api/openapi.yaml.
type InstanceStatusResponse struct {
	ID                    string                            `json:"id"`
	WorkflowDefinitionID  string                            `json:"workflow_definition_id"`
	ContextMode           string                            `json:"context_mode"`
	Debug                 bool                              `json:"debug"`
	Status                string                            `json:"status"`
	WaitingReason         *string                           `json:"waiting_reason"`
	PauseRequested        bool                              `json:"pause_requested"`
	TerminationPending    bool                              `json:"termination_pending"`
	CurrentGroupID        *string                           `json:"current_group_id"`
	CurrentNodeID         *string                           `json:"current_node_id"`
	CurrentNodeInstanceID *string                           `json:"current_node_instance_id"`
	Attempt               int                               `json:"attempt"`
	Counters              json.RawMessage                   `json:"counters"`
	Nodes                 map[string]NodeOccurrenceResponse `json:"nodes,omitempty"`
	PendingInput          *PendingInputResponse             `json:"pending_input,omitempty"`
	Parallel              []ParallelExecutionResponse       `json:"parallel,omitempty"`
	Error                 *string                           `json:"error"`
	StartedAt             *time.Time                        `json:"started_at"`
	FinishedAt            *time.Time                        `json:"finished_at"`
	CreatedBy             string                            `json:"created_by"`
	UpdatedBy             string                            `json:"updated_by"`
	CreatedAt             time.Time                         `json:"created_at"`
	UpdatedAt             time.Time                         `json:"updated_at"`
}

// PendingInputResponse is the waiting-input contract on status: the frontend
// renders its dynamic form from Form. Form is null when the input node
// carries no form contract; pending_input itself is omitted unless the
// instance waits on an input node.
type PendingInputResponse struct {
	NodeID         string        `json:"node_id"`
	Channel        string        `json:"channel"`
	OutputProperty string        `json:"output_property"`
	Form           *InputFormDTO `json:"form"`
	// AllowedRoles is the node's role gate. Empty means the node is open to
	// any caller that holds input:deliver.
	AllowedRoles []string `json:"allowed_roles"`
	// RecordActor reports that an accepted delivery is written as the
	// attribution envelope {user_id, input_data} rather than the bare
	// payload, so the frontend knows which shape the context will hold.
	RecordActor bool `json:"record_actor"`
	// Public reports that the node accepts anonymous deliveries over HTTP
	// with no credential, even when auth is on. Status and form reads stay
	// authenticated, so an anonymous caller learns the shape out-of-band.
	Public bool `json:"public"`
}

// InputFormDTO carries the raw schema contract plus opaque ui render hints.
type InputFormDTO struct {
	Schema json.RawMessage `json:"schema" swaggertype:"object"`
	UI     json.RawMessage `json:"ui,omitempty" swaggertype:"object"`
}

// NodeOccurrenceResponse maps a workflow graph node id to its executed
// occurrence. Never-executed nodes carry a null occurrence_id and attempt
// with status "not_started". Rollbackable is advisory: the rollback
// endpoint stays the source of truth.
type NodeOccurrenceResponse struct {
	OccurrenceID *string `json:"occurrence_id"`
	Status       string  `json:"status"`
	Attempt      *int    `json:"attempt"`
	Rollbackable bool    `json:"rollbackable"`
}

// ParallelExecutionResponse is one parallel fork/join scope on status with
// its branches. Omitted unless the instance forked.
type ParallelExecutionResponse struct {
	ID             string                   `json:"id"`
	ParentBranchID *string                  `json:"parent_branch_id"`
	Depth          int                      `json:"depth"`
	StartNodeID    string                   `json:"start_node_id"`
	EndNodeID      string                   `json:"end_node_id"`
	Status         string                   `json:"status"`
	BranchCount    int                      `json:"branch_count"`
	CompletedCount int                      `json:"completed_count"`
	Branches       []ParallelBranchResponse `json:"branches"`
}

// ParallelBranchResponse is one branch on status: identity, lifecycle
// state, and the last error. Lease internals and branch contexts are
// never exposed here.
type ParallelBranchResponse struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	BranchIndex   int       `json:"branch_index"`
	StartNodeID   string    `json:"start_node_id"`
	Status        string    `json:"status"`
	WaitingReason string    `json:"waiting_reason"`
	Error         string    `json:"error"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// InstanceContextResponse is the GET .../context body.
type InstanceContextResponse struct {
	ID      string          `json:"id"`
	Context json.RawMessage `json:"context"`
}

// InputDeliveryResponse is the PUT .../input body.
type InputDeliveryResponse struct {
	Accepted bool    `json:"accepted"`
	Error    *string `json:"error"`
}

// PauseResponse is the POST .../pause body.
type PauseResponse struct {
	Status         string `json:"status"`
	PauseRequested bool   `json:"pause_requested"`
}

// ResumeResponse is the POST .../resume body.
type ResumeResponse struct {
	Status string `json:"status"`
}

// StopResponse is the POST .../stop body.
type StopResponse struct {
	Status             string `json:"status"`
	TerminationPending bool   `json:"termination_pending"`
}

// RollbackRequest is the POST .../rollback body. Reason is an optional
// audit annotation recorded on the rollback event only.
type RollbackRequest struct {
	TargetOccurrenceID string `json:"target_occurrence_id"`
	Reason             string `json:"reason,omitempty"`
}

// RollbackResponse is the POST .../rollback body. The instance is always
// paused after a rollback.
type RollbackResponse struct {
	Status        string `json:"status"`
	CurrentNodeID string `json:"current_node_id"`
}

// NodeDebugResponse mirrors api/openapi.yaml. Occurrences that never ran use
// status "not_started" with nil snapshots and attempt count 0.
type NodeDebugResponse struct {
	OccurrenceID           string          `json:"occurrence_id"`
	SourceNodeDefinitionID string          `json:"source_node_definition_id"`
	Name                   string          `json:"name"`
	Type                   string          `json:"type"`
	SelectedAttempt        *int            `json:"selected_attempt"`
	LatestAttempt          *int            `json:"latest_attempt"`
	AttemptCount           int             `json:"attempt_count"`
	Status                 string          `json:"status"`
	ContextBefore          json.RawMessage `json:"context_before"`
	ContextAfter           json.RawMessage `json:"context_after"`
	Input                  json.RawMessage `json:"input"`
	Output                 json.RawMessage `json:"output"`
	Error                  *string         `json:"error"`
	RecoveryPolicy         *string         `json:"recovery_policy"`
	RecoveryResult         *string         `json:"recovery_result"`
	Cancelled              bool            `json:"cancelled"`
	StartedAt              *time.Time      `json:"started_at"`
	FinishedAt             *time.Time      `json:"finished_at"`
	StoppedAt              *time.Time      `json:"stopped_at"`
	DurationMS             *int64          `json:"duration_ms"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}

// DebugContextResponse mirrors api/openapi.yaml. OccurrenceID and Attempt
// are null when the debug position has no occurrence (the live redacted
// instance context is the source). TypeScript is an inline structural
// declaration for Monaco autocomplete; it never carries literal values.
type DebugContextResponse struct {
	InstanceID    string  `json:"instance_id"`
	NodeID        string  `json:"node_id"`
	OccurrenceID  *string `json:"occurrence_id"`
	Attempt       *int    `json:"attempt"`
	IsDebugPaused bool    `json:"is_debug_paused"`
	TypeScript    string  `json:"typescript"`
}

// -- statistics ---------------------------------------------------------------

// StatisticsSummaryResponse is the GET /v1/statistics body.
// ActiveRuns counts waiting+running+paused instances in the same window.
type StatisticsSummaryResponse struct {
	TotalRuns         int64                `json:"total_runs"`
	FinishedRuns      int64                `json:"finished_runs"`
	FailedRuns        int64                `json:"failed_runs"`
	StoppedRuns       int64                `json:"stopped_runs"`
	TerminalRuns      int64                `json:"terminal_runs"`
	ActiveRuns        int64                `json:"active_runs"`
	SuccessRate       *float64             `json:"success_rate"`
	AverageDurationMS *float64             `json:"average_duration_ms"`
	RunsPerDay        []RunsPerDayResponse `json:"runs_per_day"`
}

// RunsPerDayResponse is one calendar-day bucket (Date is YYYY-MM-DD).
type RunsPerDayResponse struct {
	Date         string `json:"date"`
	TotalRuns    int64  `json:"total_runs"`
	FinishedRuns int64  `json:"finished_runs"`
	FailedRuns   int64  `json:"failed_runs"`
	StoppedRuns  int64  `json:"stopped_runs"`
}

// -- schedules ---------------------------------------------------------------

// CreateScheduleRequest is the POST /v1/workflow/schedules body.
type CreateScheduleRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Crontab              string          `json:"crontab"`
	Timezone             string          `json:"timezone,omitempty"`
	Context              json.RawMessage `json:"context,omitempty" swaggertype:"object"`
	// Enabled defaults to true when omitted.
	Enabled *bool `json:"enabled,omitempty"`
}

// ScheduleResponse mirrors api/openapi.yaml.
type ScheduleResponse struct {
	ID                   string          `json:"id"`
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Crontab              string          `json:"crontab"`
	Timezone             string          `json:"timezone"`
	Context              json.RawMessage `json:"context" swaggertype:"object"`
	Enabled              bool            `json:"enabled"`
	CreatedBy            string          `json:"created_by"`
	UpdatedBy            string          `json:"updated_by"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}
