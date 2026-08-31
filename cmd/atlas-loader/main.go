// Command atlas-loader is the Atlas Go Program Mode schema loader. It emits
// the desired PostgreSQL schema as SQL DDL to stdout: GORM generates the
// tables/indexes from the persistence models in internal/workflow/repository,
// and the loader appends the foreign keys that hold the schema together.
// It never runs AutoMigrate against a real database (GORM migrates an
// in-memory record driver). Production schema is owned by Atlas migrations.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"ariga.io/atlas-provider-gorm/gormschema"

	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

func main() {
	ddl, err := gormschema.New("postgres").Load(
		&repository.UserModel{},
		&repository.NodeDefinitionModel{},
		&repository.WorkflowDefinitionModel{},
		&repository.WorkflowDefinitionNodeRefModel{},
		&repository.WorkflowRequestModel{},
		&repository.WorkflowInstanceModel{},
		&repository.NodeInstanceModel{},
		&repository.WorkflowInstanceEventModel{},
		&repository.InputDeliveryModel{},
		&repository.StatusUpdateOutboxModel{},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := io.WriteString(os.Stdout, ddl); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := io.WriteString(os.Stdout, foreignKeyDDL()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// foreignKeyDDL returns the ALTER TABLE statements that add the reference
// integrity constraints required by the plan (foreign keys, RESTRICT on
// delete and update).
func foreignKeyDDL() string {
	var b strings.Builder
	add := func(table, symbol, columns, refTable, refColumns string) {
		fmt.Fprintf(&b,
			"ALTER TABLE %q ADD CONSTRAINT %q FOREIGN KEY (%s) REFERENCES %q (%s) ON UPDATE RESTRICT ON DELETE RESTRICT;\n",
			table, symbol, columns, refTable, refColumns)
	}
	// node_definitions
	add("node_definitions", "fk_node_definitions_created_by", "\"created_by\"", "users", "\"id\"")
	add("node_definitions", "fk_node_definitions_updated_by", "\"updated_by\"", "users", "\"id\"")
	add("node_definitions", "fk_node_definitions_previous_version", "\"previous_version_id\"", "node_definitions", "\"id\"")
	// workflow_definitions
	add("workflow_definitions", "fk_workflow_definitions_created_by", "\"created_by\"", "users", "\"id\"")
	add("workflow_definitions", "fk_workflow_definitions_updated_by", "\"updated_by\"", "users", "\"id\"")
	add("workflow_definitions", "fk_workflow_definitions_previous_version", "\"previous_version_id\"", "workflow_definitions", "\"id\"")
	// workflow_definition_node_refs
	add("workflow_definition_node_refs", "fk_wf_def_node_refs_workflow_definition", "\"workflow_definition_id\"", "workflow_definitions", "\"id\"")
	add("workflow_definition_node_refs", "fk_wf_def_node_refs_node_definition", "\"node_definition_id\"", "node_definitions", "\"id\"")
	// workflow_requests
	add("workflow_requests", "fk_workflow_requests_workflow_definition", "\"workflow_definition_id\"", "workflow_definitions", "\"id\"")
	add("workflow_requests", "fk_workflow_requests_created_by", "\"created_by\"", "users", "\"id\"")
	// workflow_instances
	add("workflow_instances", "fk_workflow_instances_workflow_definition", "\"workflow_definition_id\"", "workflow_definitions", "\"id\"")
	add("workflow_instances", "fk_workflow_instances_created_by", "\"created_by\"", "users", "\"id\"")
	add("workflow_instances", "fk_workflow_instances_updated_by", "\"updated_by\"", "users", "\"id\"")
	// node_instances
	add("node_instances", "fk_node_instances_workflow_instance", "\"workflow_instance_id\"", "workflow_instances", "\"id\"")
	add("node_instances", "fk_node_instances_node_definition", "\"node_definition_id\"", "node_definitions", "\"id\"")
	// workflow_instance_events
	add("workflow_instance_events", "fk_wf_instance_events_workflow_instance", "\"workflow_instance_id\"", "workflow_instances", "\"id\"")
	add("workflow_instance_events", "fk_wf_instance_events_created_by", "\"created_by\"", "users", "\"id\"")
	// input_deliveries
	add("input_deliveries", "fk_input_deliveries_workflow_instance", "\"workflow_instance_id\"", "workflow_instances", "\"id\"")
	add("input_deliveries", "fk_input_deliveries_node_instance", "\"node_instance_id\"", "node_instances", "\"id\"")
	// status_update_outbox
	add("status_update_outbox", "fk_status_update_outbox_workflow_instance", "\"workflow_instance_id\"", "workflow_instances", "\"id\"")
	add("status_update_outbox", "fk_status_update_outbox_workflow_definition", "\"workflow_definition_id\"", "workflow_definitions", "\"id\"")
	// Partial index: only undelivered, non-dead events are claimable.
	fmt.Fprintf(&b, "CREATE INDEX %q ON %q (%s) WHERE %s;\n",
		"idx_status_update_outbox_ready", "status_update_outbox",
		"\"next_attempt_at\"", "\"delivered_at\" IS NULL AND \"dead_at\" IS NULL")
	return b.String()
}
