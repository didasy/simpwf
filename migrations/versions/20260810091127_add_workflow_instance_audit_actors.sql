-- Add audit actor columns to workflow_instances. The columns are added
-- nullable, backfilled from the owning workflow definition (every instance
-- references one via foreign key), then constrained NOT NULL with RESTRICT
-- foreign keys to users, matching the definition audit fields.
ALTER TABLE "public"."workflow_instances" ADD COLUMN "created_by" uuid;
ALTER TABLE "public"."workflow_instances" ADD COLUMN "updated_by" uuid;

-- Existing instances inherit the audit actors of their workflow definition.
UPDATE "public"."workflow_instances" AS "wi"
SET "created_by" = "wd"."created_by",
    "updated_by" = "wd"."updated_by"
FROM "public"."workflow_definitions" AS "wd"
WHERE "wi"."workflow_definition_id" = "wd"."id";

ALTER TABLE "public"."workflow_instances" ALTER COLUMN "created_by" SET NOT NULL;
ALTER TABLE "public"."workflow_instances" ALTER COLUMN "updated_by" SET NOT NULL;
ALTER TABLE "public"."workflow_instances" ADD CONSTRAINT "fk_workflow_instances_created_by" FOREIGN KEY ("created_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "public"."workflow_instances" ADD CONSTRAINT "fk_workflow_instances_updated_by" FOREIGN KEY ("updated_by") REFERENCES "public"."users" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
