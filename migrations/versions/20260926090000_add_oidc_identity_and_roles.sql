-- Identity, roles, and the role catalog.
--
-- users gains the OIDC provider identity used to resolve a just-in-time
-- identity row: the same (subject, issuer) pair always maps to the same
-- uuid, so every created_by/updated_by foreign key keeps pointing at one
-- row. The columns are added NOT NULL with an empty default, which is what
-- every existing row (including the system user) already means by "no
-- provider identity".
--
-- The uniqueness index is partial on purpose. A plain composite unique index
-- would treat every identity-less user as the key ('', ''), permitting
-- exactly one such row in the entire table; the second locally created user
-- would fail to insert. Restricting the index to rows that actually carry a
-- subject keeps the just-in-time upsert collision-proof without imposing a
-- limit on the users that have no provider identity.

-- Create "roles" table
CREATE TABLE "roles" (
  "name" text NOT NULL,
  "description" text NOT NULL DEFAULT '',
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("name")
);

-- Create "role_permissions" table
CREATE TABLE "role_permissions" (
  "role" text NOT NULL,
  "action" text NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("role", "action"),
  CONSTRAINT "fk_role_permissions_role" FOREIGN KEY ("role") REFERENCES "public"."roles" ("name") ON UPDATE RESTRICT ON DELETE RESTRICT
);

-- Alter "users" table
ALTER TABLE "users" ADD COLUMN "subject" text NOT NULL DEFAULT '';
ALTER TABLE "users" ADD COLUMN "issuer" text NOT NULL DEFAULT '';

-- Create index "uq_users_subject_issuer" to "users" table
CREATE UNIQUE INDEX "uq_users_subject_issuer" ON "users" ("subject","issuer") WHERE subject <> '';
