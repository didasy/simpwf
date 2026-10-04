-- Create "parallel_branches" table
CREATE TABLE "public"."parallel_branches" (
  "id" uuid NOT NULL,
  "parallel_execution_id" uuid NOT NULL,
  "instance_id" uuid NOT NULL,
  "name" text NOT NULL,
  "branch_index" bigint NOT NULL DEFAULT 0,
  "start_node_id" uuid NOT NULL,
  "frame" jsonb NOT NULL DEFAULT '{}',
  "context" jsonb NOT NULL DEFAULT '{}',
  "counters" jsonb NOT NULL DEFAULT '{}',
  "status" text NOT NULL,
  "revision" bigint NOT NULL DEFAULT 0,
  "leased_by" text NOT NULL DEFAULT '',
  "lease_expiry" timestamptz NULL,
  "error" text NOT NULL DEFAULT '',
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_parallel_branches_instance_id" to table: "parallel_branches"
CREATE INDEX "idx_parallel_branches_instance_id" ON "public"."parallel_branches" ("instance_id");
-- Create index "idx_parallel_branches_lease_expiry" to table: "parallel_branches"
CREATE INDEX "idx_parallel_branches_lease_expiry" ON "public"."parallel_branches" ("lease_expiry");
-- Create index "idx_parallel_branches_parallel_execution_id" to table: "parallel_branches"
CREATE INDEX "idx_parallel_branches_parallel_execution_id" ON "public"."parallel_branches" ("parallel_execution_id");
-- Create index "idx_parallel_branches_status" to table: "parallel_branches"
CREATE INDEX "idx_parallel_branches_status" ON "public"."parallel_branches" ("status");
-- Create index "uq_parallel_branches_execution_name" to table: "parallel_branches"
CREATE UNIQUE INDEX "uq_parallel_branches_execution_name" ON "public"."parallel_branches" ("parallel_execution_id", "name");
-- Create "parallel_executions" table
CREATE TABLE "public"."parallel_executions" (
  "id" uuid NOT NULL,
  "instance_id" uuid NOT NULL,
  "parent_branch_id" uuid NULL,
  "depth" bigint NOT NULL DEFAULT 1,
  "start_node_id" uuid NOT NULL,
  "end_node_id" uuid NOT NULL,
  "status" text NOT NULL,
  "branch_count" bigint NOT NULL DEFAULT 0,
  "completed_count" bigint NOT NULL DEFAULT 0,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_parallel_executions_instance_id" to table: "parallel_executions"
CREATE INDEX "idx_parallel_executions_instance_id" ON "public"."parallel_executions" ("instance_id");
-- Create index "idx_parallel_executions_parent_branch_id" to table: "parallel_executions"
CREATE INDEX "idx_parallel_executions_parent_branch_id" ON "public"."parallel_executions" ("parent_branch_id");
-- Create index "idx_parallel_executions_status" to table: "parallel_executions"
CREATE INDEX "idx_parallel_executions_status" ON "public"."parallel_executions" ("status");
-- Modify "parallel_branches" table
ALTER TABLE "public"."parallel_branches" ADD CONSTRAINT "fk_parallel_branches_execution" FOREIGN KEY ("parallel_execution_id") REFERENCES "public"."parallel_executions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT, ADD CONSTRAINT "fk_parallel_branches_instance" FOREIGN KEY ("instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
-- Modify "parallel_executions" table
ALTER TABLE "public"."parallel_executions" ADD CONSTRAINT "fk_parallel_executions_instance" FOREIGN KEY ("instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT, ADD CONSTRAINT "fk_parallel_executions_parent_branch" FOREIGN KEY ("parent_branch_id") REFERENCES "public"."parallel_branches" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
