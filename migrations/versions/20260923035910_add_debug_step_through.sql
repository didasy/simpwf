-- Modify "workflow_instances" table
ALTER TABLE "public"."workflow_instances" ADD COLUMN "debug" boolean NOT NULL DEFAULT false;
