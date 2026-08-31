-- Modify "node_instances" table
ALTER TABLE "public"."node_instances" ALTER COLUMN "node_definition_id" DROP NOT NULL, ADD COLUMN "node_id" uuid NOT NULL;
