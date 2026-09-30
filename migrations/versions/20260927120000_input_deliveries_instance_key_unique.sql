-- One idempotency key means one delivery per workflow instance.
--
-- The existing uq_input_deliveries_node_key index makes the key unique per
-- (node_instance_id, idempotency_key), which left a caller free to reuse the
-- same key on two different nodes of the same instance and get two rows
-- back. Replay looks the key up by instance, so a key that resolves to more
-- than one row has no single answer; the query ordering was the only thing
-- keeping that deterministic.
--
-- The new index makes explicit the uniqueness the code already assumes: the
-- first delivery for a key wins, a corrected payload needs a fresh key, and
-- a replay of that key returns exactly the row the first writer produced.
-- The narrower node-scoped index is kept because a delivery is also looked
-- up per node occurrence, and it now follows from the wider one.

-- Create index "uq_input_deliveries_instance_key" to "input_deliveries" table
CREATE UNIQUE INDEX "uq_input_deliveries_instance_key" ON "input_deliveries" ("workflow_instance_id","idempotency_key");
