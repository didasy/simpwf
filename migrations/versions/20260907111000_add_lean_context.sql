-- Add the snapshotted context mode and replayable lean-context history.
ALTER TABLE "public"."workflow_instances"
  ADD COLUMN "context_mode" text NOT NULL DEFAULT 'full';

CREATE TABLE "public"."node_context_history" (
  "id" uuid NOT NULL,
  "workflow_instance_id" uuid NOT NULL,
  "occurrence_id" text NOT NULL DEFAULT '',
  "node_id" text NOT NULL,
  "attempt" bigint NOT NULL DEFAULT 0,
  "is_anchor" boolean NOT NULL DEFAULT false,
  "snapshot" jsonb NOT NULL DEFAULT 'null',
  "diff" jsonb NOT NULL DEFAULT 'null',
  "superseded" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_node_context_history_workflow_instance"
    FOREIGN KEY ("workflow_instance_id") REFERENCES "public"."workflow_instances" ("id")
    ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX "idx_node_context_history_workflow_instance_id"
  ON "public"."node_context_history" ("workflow_instance_id");
CREATE INDEX "idx_node_context_history_occurrence_id"
  ON "public"."node_context_history" ("occurrence_id");
CREATE INDEX "idx_node_context_history_superseded"
  ON "public"."node_context_history" ("superseded");
CREATE INDEX "idx_node_context_history_cursor"
  ON "public"."node_context_history" ("workflow_instance_id", "created_at", "id");
