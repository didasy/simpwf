-- Create "users" table
CREATE TABLE "public"."users" (
  "id" uuid NOT NULL,
  "name" text NOT NULL,
  "email" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT '{}',
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id")
);
-- Create "node_definitions" table
CREATE TABLE "public"."node_definitions" (
  "id" uuid NOT NULL,
  "name" text NOT NULL,
  "version" bigint NOT NULL,
  "previous_version_id" uuid NULL,
  "lineage_id" uuid NOT NULL,
  "type" text NOT NULL,
  "content" jsonb NOT NULL,
  "created_by" uuid NOT NULL,
  "updated_by" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_node_definitions_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_node_definitions_previous_version" FOREIGN KEY ("previous_version_id") REFERENCES "public"."node_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_node_definitions_updated_by" FOREIGN KEY ("updated_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_node_definitions_lineage_id" to table: "node_definitions"
CREATE INDEX "idx_node_definitions_lineage_id" ON "public"."node_definitions" ("lineage_id");
-- Create index "idx_node_definitions_previous_version_id" to table: "node_definitions"
CREATE UNIQUE INDEX "idx_node_definitions_previous_version_id" ON "public"."node_definitions" ("previous_version_id");
-- Create "workflow_definitions" table
CREATE TABLE "public"."workflow_definitions" (
  "id" uuid NOT NULL,
  "name" text NOT NULL,
  "version" bigint NOT NULL,
  "previous_version_id" uuid NULL,
  "lineage_id" uuid NOT NULL,
  "content" jsonb NOT NULL,
  "created_by" uuid NOT NULL,
  "updated_by" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_workflow_definitions_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_workflow_definitions_previous_version" FOREIGN KEY ("previous_version_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_workflow_definitions_updated_by" FOREIGN KEY ("updated_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_workflow_definitions_lineage_id" to table: "workflow_definitions"
CREATE INDEX "idx_workflow_definitions_lineage_id" ON "public"."workflow_definitions" ("lineage_id");
-- Create index "idx_workflow_definitions_previous_version_id" to table: "workflow_definitions"
CREATE UNIQUE INDEX "idx_workflow_definitions_previous_version_id" ON "public"."workflow_definitions" ("previous_version_id");
-- Create "workflow_instances" table
CREATE TABLE "public"."workflow_instances" (
  "id" uuid NOT NULL,
  "workflow_definition_id" uuid NOT NULL,
  "status" text NOT NULL,
  "waiting_reason" text NOT NULL DEFAULT '',
  "pause_requested" boolean NOT NULL DEFAULT false,
  "termination_pending" boolean NOT NULL DEFAULT false,
  "current_group_id" uuid NULL,
  "current_node_id" uuid NULL,
  "frame" jsonb NOT NULL DEFAULT '{}',
  "context" jsonb NOT NULL DEFAULT '{}',
  "counters" jsonb NOT NULL DEFAULT '{}',
  "revision" bigint NOT NULL DEFAULT 0,
  "leased_by" text NOT NULL DEFAULT '',
  "lease_expiry" timestamptz NULL,
  "error" text NOT NULL DEFAULT '',
  "started_at" timestamptz NULL,
  "finished_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_workflow_instances_workflow_definition" FOREIGN KEY ("workflow_definition_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_workflow_instances_status" to table: "workflow_instances"
CREATE INDEX "idx_workflow_instances_status" ON "public"."workflow_instances" ("status");
-- Create index "idx_workflow_instances_workflow_definition_id" to table: "workflow_instances"
CREATE INDEX "idx_workflow_instances_workflow_definition_id" ON "public"."workflow_instances" ("workflow_definition_id");
-- Create "node_instances" table
CREATE TABLE "public"."node_instances" (
  "id" uuid NOT NULL,
  "workflow_instance_id" uuid NOT NULL,
  "node_definition_id" uuid NOT NULL,
  "name" text NOT NULL,
  "type" text NOT NULL,
  "attempt" bigint NOT NULL DEFAULT 0,
  "status" text NOT NULL,
  "input" jsonb NOT NULL DEFAULT 'null',
  "output" jsonb NOT NULL DEFAULT 'null',
  "error" text NOT NULL DEFAULT '',
  "context_before" jsonb NOT NULL DEFAULT 'null',
  "context_after" jsonb NOT NULL DEFAULT 'null',
  "recovery_policy" text NOT NULL DEFAULT '',
  "recovery_result" text NOT NULL DEFAULT '',
  "started_at" timestamptz NULL,
  "finished_at" timestamptz NULL,
  "stopped_at" timestamptz NULL,
  "cancelled" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_node_instances_node_definition" FOREIGN KEY ("node_definition_id") REFERENCES "public"."node_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_node_instances_workflow_instance" FOREIGN KEY ("workflow_instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_node_instances_status" to table: "node_instances"
CREATE INDEX "idx_node_instances_status" ON "public"."node_instances" ("status");
-- Create index "idx_node_instances_workflow_instance_id" to table: "node_instances"
CREATE INDEX "idx_node_instances_workflow_instance_id" ON "public"."node_instances" ("workflow_instance_id");
-- Create "input_deliveries" table
CREATE TABLE "public"."input_deliveries" (
  "id" uuid NOT NULL,
  "workflow_instance_id" uuid NOT NULL,
  "node_instance_id" uuid NOT NULL,
  "idempotency_key" text NOT NULL,
  "payload" jsonb NOT NULL,
  "accepted" boolean NOT NULL DEFAULT false,
  "error" text NOT NULL DEFAULT '',
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_input_deliveries_node_instance" FOREIGN KEY ("node_instance_id") REFERENCES "public"."node_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_input_deliveries_workflow_instance" FOREIGN KEY ("workflow_instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_input_deliveries_workflow_instance_id" to table: "input_deliveries"
CREATE INDEX "idx_input_deliveries_workflow_instance_id" ON "public"."input_deliveries" ("workflow_instance_id");
-- Create index "uq_input_deliveries_node_key" to table: "input_deliveries"
CREATE UNIQUE INDEX "uq_input_deliveries_node_key" ON "public"."input_deliveries" ("node_instance_id", "idempotency_key");
-- Create "workflow_definition_node_refs" table
CREATE TABLE "public"."workflow_definition_node_refs" (
  "workflow_definition_id" uuid NOT NULL,
  "node_definition_id" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("workflow_definition_id", "node_definition_id"),
  CONSTRAINT "fk_wf_def_node_refs_node_definition" FOREIGN KEY ("node_definition_id") REFERENCES "public"."node_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_wf_def_node_refs_workflow_definition" FOREIGN KEY ("workflow_definition_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_workflow_definition_node_refs_node_definition_id" to table: "workflow_definition_node_refs"
CREATE INDEX "idx_workflow_definition_node_refs_node_definition_id" ON "public"."workflow_definition_node_refs" ("node_definition_id");
-- Create "workflow_instance_events" table
CREATE TABLE "public"."workflow_instance_events" (
  "id" uuid NOT NULL,
  "workflow_instance_id" uuid NOT NULL,
  "type" text NOT NULL,
  "data" jsonb NOT NULL DEFAULT '{}',
  "created_by" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_wf_instance_events_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_wf_instance_events_workflow_instance" FOREIGN KEY ("workflow_instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_workflow_instance_events_created_at" to table: "workflow_instance_events"
CREATE INDEX "idx_workflow_instance_events_created_at" ON "public"."workflow_instance_events" ("created_at");
-- Create index "idx_workflow_instance_events_workflow_instance_id" to table: "workflow_instance_events"
CREATE INDEX "idx_workflow_instance_events_workflow_instance_id" ON "public"."workflow_instance_events" ("workflow_instance_id");
-- Create "workflow_requests" table
CREATE TABLE "public"."workflow_requests" (
  "id" uuid NOT NULL,
  "workflow_definition_id" uuid NOT NULL,
  "context" jsonb NOT NULL DEFAULT '{}',
  "created_by" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_workflow_requests_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_workflow_requests_workflow_definition" FOREIGN KEY ("workflow_definition_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_workflow_requests_workflow_definition_id" to table: "workflow_requests"
CREATE INDEX "idx_workflow_requests_workflow_definition_id" ON "public"."workflow_requests" ("workflow_definition_id");
