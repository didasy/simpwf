package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// User is an audit actor. Authentication is a separate concern: this row
// records who acted, and for an OIDC caller it also caches the provider
// identity so the same human resolves to the same uuid across requests.
// Subject and Issuer are the provider identity and are unique together;
// both are empty for the configured system user.
type User struct {
	ID        string
	Subject   string
	Issuer    string
	Name      string
	Email     string
	Metadata  json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Role is a named bundle of resource-action permissions. Roles are seeded
// from configuration at startup and are read-only over the API.
type Role struct {
	Name        string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// RolePermission grants one resource-action to a role. The pair is the
// primary key; it is seeded from configuration and never written at runtime.
type RolePermission struct {
	Role      string
	Action    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NodeDefinition is an immutable, reusable node definition.
type NodeDefinition struct {
	ID                string
	Name              string
	Version           int
	PreviousVersionID *string
	LineageID         string
	Type              string
	Content           json.RawMessage
	CreatedBy         string
	UpdatedBy         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// WorkflowDefinition is an immutable workflow definition. New versions are
// created by referencing the previous version; lineage_id groups versions.
type WorkflowDefinition struct {
	ID                string
	Name              string
	Version           int
	PreviousVersionID *string
	LineageID         string
	Content           json.RawMessage
	CreatedBy         string
	UpdatedBy         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// WorkflowRequest is the durable record of a run-request.
type WorkflowRequest struct {
	ID                   string
	WorkflowDefinitionID string
	Context              json.RawMessage
	CreatedBy            string
	CreatedAt            time.Time
}

// CronSchedule fires a workflow definition on a crontab expression. While
// Enabled, the engine creates an instance of WorkflowDefinitionID with
// Context on every tick. Crontab is a standard 5-field cron expression
// (minute first) or an @descriptor; Timezone is the IANA zone ticks are
// evaluated in.
type CronSchedule struct {
	ID                   string
	WorkflowDefinitionID string
	Crontab              string
	Timezone             string
	Context              json.RawMessage
	Enabled              bool
	CreatedBy            string
	UpdatedBy            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// WorkflowInstance is a single execution of a workflow definition.
// Debug marks a step-through run: the instance starts paused and the
// engine re-pauses after every node transition until termination.
type WorkflowInstance struct {
	ID                   string
	WorkflowDefinitionID string
	ContextMode          string
	Debug                bool
	Status               WorkflowStatus
	WaitingReason        WaitingReason
	PauseRequested       bool
	TerminationPending   bool
	CurrentGroupID       string
	CurrentNodeID        string
	Frame                json.RawMessage
	Context              json.RawMessage
	Counters             json.RawMessage
	Revision             int64
	LeasedBy             string
	LeaseExpiry          time.Time
	Error                string
	StartedAt            *time.Time
	FinishedAt           *time.Time
	CreatedBy            string
	UpdatedBy            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// HistoryCursor identifies a history row using its stable database ordering.
// The id tie-breaker keeps cursor comparisons deterministic when rows share a
// timestamp.
type HistoryCursor struct {
	CreatedAt time.Time
	ID        string
}

// Before reports whether c sorts before other.
func (c HistoryCursor) Before(other HistoryCursor) bool {
	if c.CreatedAt.Before(other.CreatedAt) {
		return true
	}
	return c.CreatedAt.Equal(other.CreatedAt) && c.ID < other.ID
}

// AtOrBefore reports whether c sorts at or before other.
func (c HistoryCursor) AtOrBefore(other HistoryCursor) bool {
	return c.Before(other) || (c.CreatedAt.Equal(other.CreatedAt) && c.ID == other.ID)
}

// NodeContextHistory is one immutable lean-context anchor or diff.
type NodeContextHistory struct {
	ID                 string
	WorkflowInstanceID string
	OccurrenceID       string
	NodeID             string
	Attempt            int
	IsAnchor           bool
	Snapshot           json.RawMessage
	Diff               json.RawMessage
	Superseded         bool
	CreatedAt          time.Time
}

// Cursor returns history row's ordering cursor.
func (h NodeContextHistory) Cursor() HistoryCursor {
	return HistoryCursor{CreatedAt: h.CreatedAt, ID: h.ID}
}

// LeanOptions configures lean-context defaults and replay limits.
type LeanOptions struct {
	LeanContextDefault bool
	AnchorEvery        int
	ReplayMax          int
}

// NodeInstance is a single occurrence of a node within a workflow instance.
// Each attempt of a looped node shares the occurrence and increments Attempt.
// BranchID names the owning parallel branch, or "" for the parent scope.
type NodeInstance struct {
	ID                 string
	WorkflowInstanceID string
	BranchID           string
	NodeID             string // workflow graph node id the occurrence belongs to
	NodeDefinitionID   string
	Name               string
	Type               string
	Attempt            int
	Status             NodeStatus
	Input              json.RawMessage
	Output             json.RawMessage
	Error              string
	ContextBefore      json.RawMessage
	ContextAfter       json.RawMessage
	RecoveryPolicy     string
	RecoveryResult     string
	StartedAt          *time.Time
	FinishedAt         *time.Time
	StoppedAt          *time.Time
	Cancelled          bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// WorkflowInstanceEvent is an append-only audit event for an instance.
type WorkflowInstanceEvent struct {
	ID                 string
	WorkflowInstanceID string
	Type               string
	Data               json.RawMessage
	CreatedBy          string
	CreatedAt          time.Time
}

// InputDelivery records each input payload delivered to an instance for
// idempotency and audit.
type InputDelivery struct {
	ID                 string
	WorkflowInstanceID string
	NodeInstanceID     string
	IdempotencyKey     string
	Payload            json.RawMessage
	Accepted           bool
	Error              string
	CreatedAt          time.Time
}

// NodeInstanceID is the public node-instance identifier of the form
// <workflow instance id>:<node occurrence id>.
type NodeInstanceID struct {
	WorkflowInstanceID string
	OccurrenceID       string
}

// String renders the public identifier.
func (n NodeInstanceID) String() string {
	return fmt.Sprintf("%s:%s", n.WorkflowInstanceID, n.OccurrenceID)
}

// ParseNodeInstanceID parses "<instance>:<occurrence>" into its parts.
func ParseNodeInstanceID(s string) (NodeInstanceID, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return NodeInstanceID{}, fmt.Errorf("model: %q is not a valid node instance id", s)
	}
	return NodeInstanceID{WorkflowInstanceID: parts[0], OccurrenceID: parts[1]}, nil
}
