// Package repository holds GORM persistence models, mappers, and repository
// implementations. Persistence models are the source of truth for the schema;
// migrations are generated from them by cmd/atlas-loader, never AutoMigrate.
package repository

import (
	"time"

	"gorm.io/datatypes"
)

// UserModel persists model.User. Subject and Issuer hold the OIDC provider
// identity, so a just-in-time upsert can find the row again on the caller's
// next request. Both are empty for a user with no provider identity (the
// system user, and every locally created user), which is why the uniqueness
// index is partial: a plain composite unique would allow exactly one such
// row in the whole table and fail the second insert.
//
// Both tags must carry the same predicate. GORM derives one index from the
// pair, and a mismatch in uniqueIndex or where between the two columns emits
// a non-unique index, which would silently stop protecting the upsert.
type UserModel struct {
	ID        string         `gorm:"column:id;type:uuid;primaryKey"`
	Subject   string         `gorm:"column:subject;not null;default:'';uniqueIndex:uq_users_subject_issuer,where:subject <> '',priority:1"`
	Issuer    string         `gorm:"column:issuer;not null;default:'';uniqueIndex:uq_users_subject_issuer,where:subject <> '',priority:2"`
	Name      string         `gorm:"column:name;not null"`
	Email     string         `gorm:"column:email;not null"`
	Metadata  datatypes.JSON `gorm:"column:metadata;type:jsonb;not null;default:'{}'"`
	CreatedAt time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the users table.
func (UserModel) TableName() string { return "users" }

// RoleModel persists model.Role. Roles are seeded from configuration, so the
// catalog is owned by the deployment rather than by the API.
type RoleModel struct {
	Name        string    `gorm:"column:name;type:text;primaryKey"`
	Description string    `gorm:"column:description;not null;default:''"`
	CreatedAt   time.Time `gorm:"column:created_at;not null"`
	UpdatedAt   time.Time `gorm:"column:updated_at;not null"`
}

// TableName is the roles table.
func (RoleModel) TableName() string { return "roles" }

// RolePermissionModel persists model.RolePermission.
type RolePermissionModel struct {
	Role      string    `gorm:"column:role;type:text;primaryKey"`
	Action    string    `gorm:"column:action;type:text;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}

// TableName is the role_permissions table.
func (RolePermissionModel) TableName() string { return "role_permissions" }

// SecretModel persists a secret value. Secret values must only be exposed to
// the template execution path; API read paths return masked responses.
type SecretModel struct {
	Key       string    `gorm:"column:key;type:text;primaryKey"`
	Value     string    `gorm:"column:value;type:text;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}

// TableName is the secrets table.
func (SecretModel) TableName() string { return "secrets" }

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

// CronScheduleModel persists model.CronSchedule.
type CronScheduleModel struct {
	ID                   string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowDefinitionID string         `gorm:"column:workflow_definition_id;type:uuid;not null;index"`
	Crontab              string         `gorm:"column:crontab;not null"`
	Timezone             string         `gorm:"column:timezone;not null;default:'UTC'"`
	Context              datatypes.JSON `gorm:"column:context;type:jsonb;not null;default:'{}'"`
	Enabled              bool           `gorm:"column:enabled;not null"`
	CreatedBy            string         `gorm:"column:created_by;type:uuid;not null"`
	UpdatedBy            string         `gorm:"column:updated_by;type:uuid;not null"`
	CreatedAt            time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt            time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the cron_schedules table.
func (CronScheduleModel) TableName() string { return "cron_schedules" }

// ScheduleFireModel records one claimed schedule tick. The composite key
// (schedule_id, fire_at) is the fleet-wide mutual exclusion: every replica
// fires the same tick, but only the replica whose claim row wins the insert
// creates the instance. FireAt is the tick time truncated to whole seconds
// UTC; CreatedAt is when the claim was won.
//
// There is deliberately no foreign key to cron_schedules: deleting a
// schedule must not be blocked by its fire history, and orphaned rows age
// out through SweepFires like any other old claim. InstanceID is
// informational, set after the winner creates the instance.
type ScheduleFireModel struct {
	ScheduleID string    `gorm:"column:schedule_id;type:uuid;primaryKey"`
	FireAt     time.Time `gorm:"column:fire_at;primaryKey;index"`
	FiredBy    string    `gorm:"column:fired_by;not null"`
	InstanceID *string   `gorm:"column:instance_id;type:uuid"`
	CreatedAt  time.Time `gorm:"column:created_at;not null"`
	UpdatedAt  time.Time `gorm:"column:updated_at;not null"`
}

// TableName is the schedule_fires table.
func (ScheduleFireModel) TableName() string { return "schedule_fires" }

// WorkflowInstanceModel persists model.WorkflowInstance.
type WorkflowInstanceModel struct {
	ID                   string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowDefinitionID string         `gorm:"column:workflow_definition_id;type:uuid;not null;index"`
	ContextMode          string         `gorm:"column:context_mode;not null;default:'full'"`
	Debug                bool           `gorm:"column:debug;not null;default:false"`
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

// NodeContextHistoryModel stores one lean-context commit or anchor.
type NodeContextHistoryModel struct {
	ID                 string         `gorm:"column:id;type:uuid;primaryKey;index:idx_node_context_history_cursor,priority:3"`
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index;index:idx_node_context_history_cursor,priority:1"`
	OccurrenceID       string         `gorm:"column:occurrence_id;not null;default:'';index"`
	NodeID             string         `gorm:"column:node_id;not null"`
	Attempt            int            `gorm:"column:attempt;not null;default:0"`
	IsAnchor           bool           `gorm:"column:is_anchor;not null;default:false"`
	Snapshot           datatypes.JSON `gorm:"column:snapshot;type:jsonb;not null;default:'null'"`
	Diff               datatypes.JSON `gorm:"column:diff;type:jsonb;not null;default:'null'"`
	Superseded         bool           `gorm:"column:superseded;not null;default:false;index"`
	CreatedAt          time.Time      `gorm:"column:created_at;not null;index:idx_node_context_history_cursor,priority:2"`
}

// TableName is the node_context_history table.
func (NodeContextHistoryModel) TableName() string { return "node_context_history" }

// NodeInstanceModel persists model.NodeInstance.
type NodeInstanceModel struct {
	ID                 string         `gorm:"column:id;type:uuid;primaryKey"`
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index"`
	BranchID           *string        `gorm:"column:branch_id;type:uuid;index"`
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
	WorkflowInstanceID string         `gorm:"column:workflow_instance_id;type:uuid;not null;index;uniqueIndex:uq_input_deliveries_instance_key,priority:1"`
	NodeInstanceID     string         `gorm:"column:node_instance_id;type:uuid;not null;uniqueIndex:uq_input_deliveries_node_key"`
	IdempotencyKey     string         `gorm:"column:idempotency_key;not null;uniqueIndex:uq_input_deliveries_node_key;uniqueIndex:uq_input_deliveries_instance_key,priority:2"`
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

// ParallelExecutionModel persists model.ParallelExecution: one durable
// fork/join scope. CompletedCount is maintained by the barrier transaction,
// never recomputed from branches, so the last-branch check stays atomic.
type ParallelExecutionModel struct {
	ID             string    `gorm:"column:id;type:uuid;primaryKey"`
	InstanceID     string    `gorm:"column:instance_id;type:uuid;not null;index"`
	ParentBranchID *string   `gorm:"column:parent_branch_id;type:uuid;index"`
	Depth          int       `gorm:"column:depth;not null;default:1"`
	StartNodeID    string    `gorm:"column:start_node_id;type:uuid;not null"`
	EndNodeID      string    `gorm:"column:end_node_id;type:uuid;not null"`
	Status         string    `gorm:"column:status;not null;index"`
	BranchCount    int       `gorm:"column:branch_count;not null;default:0"`
	CompletedCount int       `gorm:"column:completed_count;not null;default:0"`
	CreatedAt      time.Time `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time `gorm:"column:updated_at;not null"`
}

// TableName is the parallel_executions table.
func (ParallelExecutionModel) TableName() string { return "parallel_executions" }

// ParallelBranchModel persists model.ParallelBranch: one branch cursor with
// its own context, status, and lease. Name is unique per execution so the
// join addresses stable branch identities.
type ParallelBranchModel struct {
	ID                  string         `gorm:"column:id;type:uuid;primaryKey"`
	ParallelExecutionID string         `gorm:"column:parallel_execution_id;type:uuid;not null;index;uniqueIndex:uq_parallel_branches_execution_name,priority:1"`
	InstanceID          string         `gorm:"column:instance_id;type:uuid;not null;index"`
	Name                string         `gorm:"column:name;not null;uniqueIndex:uq_parallel_branches_execution_name,priority:2"`
	BranchIndex         int            `gorm:"column:branch_index;not null;default:0"`
	StartNodeID         string         `gorm:"column:start_node_id;type:uuid;not null"`
	Frame               datatypes.JSON `gorm:"column:frame;type:jsonb;not null;default:'{}'"`
	Context             datatypes.JSON `gorm:"column:context;type:jsonb;not null;default:'{}'"`
	Counters            datatypes.JSON `gorm:"column:counters;type:jsonb;not null;default:'{}'"`
	Status              string         `gorm:"column:status;not null;index"`
	WaitingReason       string         `gorm:"column:waiting_reason;not null;default:''"`
	Revision            int64          `gorm:"column:revision;not null;default:0"`
	LeasedBy            string         `gorm:"column:leased_by;not null;default:''"`
	LeaseExpiry         *time.Time     `gorm:"column:lease_expiry;index"`
	Error               string         `gorm:"column:error;not null;default:''"`
	CreatedAt           time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt           time.Time      `gorm:"column:updated_at;not null"`
}

// TableName is the parallel_branches table.
func (ParallelBranchModel) TableName() string { return "parallel_branches" }
