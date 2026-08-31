-- Drop index "uq_status_update_outbox_evt" from table: "status_update_outbox"
DROP INDEX "public"."uq_status_update_outbox_evt";
-- Create index "uq_status_update_outbox_evt" to table: "status_update_outbox"
CREATE UNIQUE INDEX "uq_status_update_outbox_evt" ON "public"."status_update_outbox" ("workflow_instance_id", "transport", "revision", "event_index");
