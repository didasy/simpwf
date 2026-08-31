// Package repository holds GORM persistence models, mappers, and repository
// implementations. Persistence models are the source of truth for the schema;
// migrations are generated from them by cmd/atlas-loader, never AutoMigrate.
package repository

import (
	"time"

	"gorm.io/datatypes"
)

// UserModel persists model.User.
type UserModel struct {
	ID        string         `gorm:"column:id;type:uuid;primaryKey"`
	Name      string         `gorm:"column:name;not null"`
	Email     string         `gorm:"column:email;not null"`
	Metadata  datatypes.JSON `gorm:"column:metadata;type:jsonb;not null;default:'{}'"`
	CreatedAt time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the users table.
func (UserModel) TableName() string { return "users" }

// NodeDefinitionModel persists model.NodeDefinition.
type NodeDefinitionModel struct {
	ID                string         `gorm:"column:id;type:uuid;primaryKey"`
	Name              string         `gorm:"column:name;not null"`
	Version           int            `gorm:"column:version;not null"`
	PreviousVersionID *string        `gorm:"column:previous_version_id;type:uuid;uniqueIndex"`
	LineageID         string         `gorm:"column:lineage_id;type:uuid;not null;index"`
	Type              string         `gorm:"column:type;not null"`
	Content           datatypes.JSON `gorm:"column:content;type:jsonb;not null"`
	CreatedBy         string         `gorm:"column:created_by;type:uuid;not null"`
	UpdatedBy         string         `gorm:"column:updated_by;type:uuid;not null"`
	CreatedAt         time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt         time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the node_definitions table.
func (NodeDefinitionModel) TableName() string { return "node_definitions" }

// WorkflowDefinitionModel persists model.WorkflowDefinition.
type WorkflowDefinitionModel struct {
	ID                string         `gorm:"column:id;type:uuid;primaryKey"`
	Name              string         `gorm:"column:name;not null"`
	Version           int            `gorm:"column:version;not null"`
	PreviousVersionID *string        `gorm:"column:previous_version_id;type:uuid;uniqueIndex"`
	LineageID         string         `gorm:"column:lineage_id;type:uuid;not null;index"`
	Content           datatypes.JSON `gorm:"column:content;type:jsonb;not null"`
	CreatedBy         string         `gorm:"column:created_by;type:uuid;not null"`
	UpdatedBy         string         `gorm:"column:updated_by;type:uuid;not null"`
	CreatedAt         time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt         time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the workflow_definitions table.
func (WorkflowDefinitionModel) TableName() string { return "workflow_definitions" }

// WorkflowDefinitionNodeRefModel tracks which node definitions a workflow
// definition references, to block node-definition deletion while referenced.
type WorkflowDefinitionNodeRefModel struct {
	WorkflowDefinitionID string    `gorm:"column:workflow_definition_id;type:uuid;primaryKey"`
	NodeDefinitionID     string    `gorm:"column:node_definition_id;type:uuid;primaryKey;index"`
	CreatedAt            time.Time `gorm:"column:created_at;not null"`
}

// TableName is the workflow_definition_node_refs table.
func (WorkflowDefinitionNodeRefModel) TableName() string { return "workflow_definition_node_refs" }

// WorkflowRequestModel persists model.WorkflowRequest.
type WorkflowRequestModel struct {
	ID                   string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowDefinitionID string         `gorm:"column:workflow_definition_id;type:uuid;not null;index"`
	Context              datatypes.JSON `gorm:"column:context;type:jsonb;not null;default:'{}'"`
	CreatedBy            string         `gorm:"column:created_by;type:uuid;not null"`
	CreatedAt            time.Time      `gorm:"column:created_at;not null"`
}

// TableName is the workflow_requests table.
func (WorkflowRequestModel) TableName() string { return "workflow_requests" }

// WorkflowInstanceModel persists model.WorkflowInstance.
type WorkflowInstanceModel struct {
	ID                   string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowDefinitionID string         `gorm:"column:workflow_definition_id;type:uuid;not null;index"`
	Status               string         `gorm:"column:status;not null;index"`
	WaitingReason        string         `gorm:"column:waiting_reason;not null;default:''"`
	PauseRequested       bool           `gorm:"column:pause_requested;not null;default:false"`
	TerminationPending   bool           `gorm:"column:termination_pending;not null;default:false"`
	CurrentGroupID       *string        `gorm:"column:current_group_id;type:uuid"`
	CurrentNodeID        *string        `gorm:"column:current_node_id;type:uuid"`
	Frame                datatypes.JSON `gorm:"column:frame;type:jsonb;not null;default:'{}'"`
	Context              datatypes.JSON `gorm:"column:context;type:jsonb;not null;default:'{}'"`
	Counters             datatypes.JSON `gorm:"column:counters;type:jsonb;not null;default:'{}'"`
	Revision             int64          `gorm:"column:revision;not null;default:0"`
	LeasedBy             string         `gorm:"column:leased_by;not null;default:''"`
	LeaseExpiry          *time.Time     `gorm:"column:lease_expiry"`
	Error                string         `gorm:"column:error;not null;default:''"`
	StartedAt            *time.Time     `gorm:"column:started_at"`
	FinishedAt           *time.Time     `gorm:"column:finished_at"`
	CreatedBy            string         `gorm:"column:created_by;type:uuid;not null"`
	UpdatedBy            string         `gorm:"column:updated_by;type:uuid;not null"`
	CreatedAt            time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt            time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the workflow_instances table.
func (WorkflowInstanceModel) TableName() string { return "workflow_instances" }

// NodeInstanceModel persists model.NodeInstance.
type NodeInstanceModel struct {
	ID                 string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index"`
	NodeID             string         `gorm:"column:node_id;type:uuid;not null"`
	NodeDefinitionID   *string        `gorm:"column:node_definition_id;type:uuid"`
	Name               string         `gorm:"column:name;not null"`
	Type               string         `gorm:"column:type;not null"`
	Attempt            int            `gorm:"column:attempt;not null;default:0"`
	Status             string         `gorm:"column:status;not null;index"`
	Input              datatypes.JSON `gorm:"column:input;type:jsonb;not null;default:'null'"`
	Output             datatypes.JSON `gorm:"column:output;type:jsonb;not null;default:'null'"`
	Error              string         `gorm:"column:error;not null;default:''"`
	ContextBefore      datatypes.JSON `gorm:"column:context_before;type:jsonb;not null;default:'null'"`
	ContextAfter       datatypes.JSON `gorm:"column:context_after;type:jsonb;not null;default:'null'"`
	RecoveryPolicy     string         `gorm:"column:recovery_policy;not null;default:''"`
	RecoveryResult     string         `gorm:"column:recovery_result;not null;default:''"`
	StartedAt          *time.Time     `gorm:"column:started_at"`
	FinishedAt         *time.Time     `gorm:"column:finished_at"`
	StoppedAt          *time.Time     `gorm:"column:stopped_at"`
	Cancelled          bool           `gorm:"column:cancelled;not null;default:false"`
	CreatedAt          time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt          time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the node_instances table.
func (NodeInstanceModel) TableName() string { return "node_instances" }

// WorkflowInstanceEventModel persists model.WorkflowInstanceEvent.
type WorkflowInstanceEventModel struct {
	ID                 string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index"`
	Type               string         `gorm:"column:type;not null"`
	Data               datatypes.JSON `gorm:"column:data;type:jsonb;not null;default:'{}'"`
	CreatedBy          string         `gorm:"column:created_by;type:uuid;not null"`
	CreatedAt          time.Time      `gorm:"column:created_at;not null;index"`
}

// TableName is the workflow_instance_events table.
func (WorkflowInstanceEventModel) TableName() string { return "workflow_instance_events" }

// InputDeliveryModel persists model.InputDelivery.
type InputDeliveryModel struct {
	ID                 string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index"`
	NodeInstanceID     string         `gorm:"column:node_instance_id;type:uuid;not null;uniqueIndex:uq_input_deliveries_node_key"`
	IdempotencyKey     string         `gorm:"column:idempotency_key;not null;uniqueIndex:uq_input_deliveries_node_key"`
	Payload            datatypes.JSON `gorm:"column:payload;type:jsonb;not null"`
	Accepted           bool           `gorm:"column:accepted;not null;default:false"`
	Error              string         `gorm:"column:error;not null;default:''"`
	CreatedAt          time.Time      `gorm:"column:created_at;not null"`
}

// TableName is the input_deliveries table.
func (InputDeliveryModel) TableName() string { return "input_deliveries" }

// StatusUpdateOutboxModel persists one queued status-update notification.
// The outbox row commits atomically with the status transition that produced
// it; a separate dispatcher delivers events in strict per-instance order
// (revision, event_index) independently per transport. One logical event
// fans out to one row per configured transport; the unique index allows at
// most one undelivered row per (instance, transport, revision, event_index).
type StatusUpdateOutboxModel struct {
	ID                   string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowInstanceID   string         `gorm:"column:workflow_instance_id;type:uuid;not null;uniqueIndex:uq_status_update_outbox_evt,priority:1"`
	WorkflowDefinitionID string         `gorm:"column:workflow_definition_id;type:uuid;not null;index"`
	Revision             int64          `gorm:"column:revision;not null;uniqueIndex:uq_status_update_outbox_evt,priority:3"`
	EventIndex           int            `gorm:"column:event_index;not null;uniqueIndex:uq_status_update_outbox_evt,priority:4"`
	Transport            string         `gorm:"column:transport;not null;default:'http';uniqueIndex:uq_status_update_outbox_evt,priority:2"`
	Payload              datatypes.JSON `gorm:"column:payload;type:jsonb;not null;default:'{}'"`
	Attempts             int            `gorm:"column:attempts;not null;default:0"`
	NextAttemptAt        time.Time      `gorm:"column:next_attempt_at;not null"`
	ClaimedBy            string         `gorm:"column:claimed_by;not null;default:''"`
	ClaimExpiry          *time.Time     `gorm:"column:claim_expiry"`
	DeliveredAt          *time.Time     `gorm:"column:delivered_at"`
	DeadAt               *time.Time     `gorm:"column:dead_at"`
	LastError            string         `gorm:"column:last_error;not null;default:''"`
	CreatedAt            time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt            time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the status_update_outbox table.
func (StatusUpdateOutboxModel) TableName() string { return "status_update_outbox" }
