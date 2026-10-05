-- Create "schedule_fires" table
CREATE TABLE "public"."schedule_fires" (
  "schedule_id" uuid NOT NULL,
  "fire_at" timestamptz NOT NULL,
  "fired_by" text NOT NULL,
  "instance_id" uuid NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("schedule_id", "fire_at")
);
-- Create index "idx_schedule_fires_fire_at" to table: "schedule_fires"
CREATE INDEX "idx_schedule_fires_fire_at" ON "public"."schedule_fires" ("fire_at");
