-- Create "cron_schedules" table
CREATE TABLE "public"."cron_schedules" (
  "id" uuid NOT NULL,
  "workflow_definition_id" uuid NOT NULL,
  "crontab" text NOT NULL,
  "timezone" text NOT NULL DEFAULT 'UTC',
  "context" jsonb NOT NULL DEFAULT '{}',
  "enabled" boolean NOT NULL,
  "created_by" uuid NOT NULL,
  "updated_by" uuid NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_cron_schedules_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_cron_schedules_updated_by" FOREIGN KEY ("updated_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_cron_schedules_workflow_definition" FOREIGN KEY ("workflow_definition_id") REFERENCES "public"."workflow_definitions" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT
);
-- Create index "idx_cron_schedules_workflow_definition_id" to table: "cron_schedules"
CREATE INDEX "idx_cron_schedules_workflow_definition_id" ON "public"."cron_schedules" ("workflow_definition_id");
