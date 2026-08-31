package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// User is an audit actor. No authentication is implemented yet; the
// configured system user is the actor for all operations.
type User struct {
	ID        string
	Name      string
	Email     string
	Metadata  json.RawMessage
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

// WorkflowInstance is a single execution of a workflow definition.
type WorkflowInstance struct {
	ID                   string
	WorkflowDefinitionID string
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

// NodeInstance is a single occurrence of a node within a workflow instance.
// Each attempt of a looped node shares the occurrence and increments Attempt.
type NodeInstance struct {
	ID                 string
	WorkflowInstanceID string
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
