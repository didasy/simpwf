-- Create "status_update_outbox" table
CREATE TABLE "public"."status_update_outbox" (
  "id" uuid NOT NULL,
  "workflow_instance_id" uuid NOT NULL,
  "workflow_definition_id" uuid NOT NULL,
  "revision" bigint NOT NULL,
  "event_index" bigint NOT NULL,
  "transport" text NOT NULL DEFAULT 'http',
  "payload" jsonb NOT NULL DEFAULT '{}',
  "attempts" bigint NOT NULL DEFAULT 0,
  "next_attempt_at" timestamptz NOT NULL,
  "claimed_by" text NOT NULL DEFAULT '',
  "claim_expiry" timestamptz NULL,
  "delivered_at" timestamptz NULL,
  "dead_at" timestamptz NULL,
  "last_error" text NOT NULL DEFAULT '',
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_status_update_outbox_workflow_definition" FOREIGN KEY ("workflow_definition_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_status_update_outbox_workflow_instance" FOREIGN KEY ("workflow_instance_id") REFERENCES "public"."workflow_instances" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_status_update_outbox_ready" to table: "status_update_outbox"
CREATE INDEX "idx_status_update_outbox_ready" ON "public"."status_update_outbox" ("next_attempt_at") WHERE ((delivered_at IS NULL) AND (dead_at IS NULL));
-- Create index "idx_status_update_outbox_workflow_definition_id" to table: "status_update_outbox"
CREATE INDEX "idx_status_update_outbox_workflow_definition_id" ON "public"."status_update_outbox" ("workflow_definition_id");
-- Create index "uq_status_update_outbox_evt" to table: "status_update_outbox"
CREATE UNIQUE INDEX "uq_status_update_outbox_evt" ON "public"."status_update_outbox" ("workflow_instance_id", "revision", "event_index");
