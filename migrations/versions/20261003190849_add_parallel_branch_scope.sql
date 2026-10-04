-- Modify "parallel_branches" table
ALTER TABLE "public"."parallel_branches" ADD COLUMN "waiting_reason" text NOT NULL DEFAULT '';
-- Modify "node_instances" table
ALTER TABLE "public"."node_instances" ADD COLUMN "branch_id" uuid NULL, ADD CONSTRAINT "fk_node_instances_branch" FOREIGN KEY ("branch_id") REFERENCES "public"."parallel_branches" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT;
-- Create index "idx_node_instances_branch_id" to table: "node_instances"
CREATE INDEX "idx_node_instances_branch_id" ON "public"."node_instances" ("branch_id");
