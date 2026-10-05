-- Create index "idx_cron_schedules_enabled" to table: "cron_schedules"
CREATE INDEX "idx_cron_schedules_enabled" ON "public"."cron_schedules" ("enabled");
-- Drop index "idx_workflow_instances_status" from table: "workflow_instances"
DROP INDEX "public"."idx_workflow_instances_status";
-- Create index "idx_workflow_instances_claim" to table: "workflow_instances"
CREATE INDEX "idx_workflow_instances_claim" ON "public"."workflow_instances" ("status", "waiting_reason", "lease_expiry", "updated_at");
-- Create index "idx_workflow_instances_created_by" to table: "workflow_instances"
CREATE INDEX "idx_workflow_instances_created_by" ON "public"."workflow_instances" ("created_by", "created_at");
-- Create index "idx_workflow_instances_termination_pending" to table: "workflow_instances"
CREATE INDEX "idx_workflow_instances_termination_pending" ON "public"."workflow_instances" ("termination_pending") WHERE (termination_pending = true);
