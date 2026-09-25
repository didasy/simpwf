-- Create "secrets" table
CREATE TABLE "secrets" ("key" text,"value" text NOT NULL,"created_at" timestamptz NOT NULL,"updated_at" timestamptz NOT NULL,PRIMARY KEY ("key"));
