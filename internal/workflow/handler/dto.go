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
	CreatedBy         string          `json:"created_by"`
	UpdatedBy         string          `json:"updated_by"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
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
	CreatedBy         string          `json:"created_by"`
	UpdatedBy         string          `json:"updated_by"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// ListResponse is the envelope for definition lists.
type ListResponse[T any] struct {
	Items      []T   `json:"items"`
	Page       int   `json:"page"`
	PerPage    int   `json:"per_page"`
	Total      int64 `json:"total"`
	TotalPages int64 `json:"total_pages"`
}

// -- instances ----------------------------------------------------------------

// CreateInstanceRequest is the POST /v1/workflow/instance body.
type CreateInstanceRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Context              json.RawMessage `json:"context,omitempty"`
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
	Error                 *string                           `json:"error"`
	StartedAt             *time.Time                        `json:"started_at"`
	FinishedAt            *time.Time                        `json:"finished_at"`
	CreatedBy             string                            `json:"created_by"`
	UpdatedBy             string                            `json:"updated_by"`
	CreatedAt             time.Time                         `json:"created_at"`
	UpdatedAt             time.Time                         `json:"updated_at"`
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
