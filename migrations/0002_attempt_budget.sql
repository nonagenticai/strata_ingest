-- Attempt budget without ever decreasing `attempt` (it is the fence token of every step write):
-- a run given back on shutdown (`releases`, at most 3 refunded), a run abandoned while waiting
-- for a shared slot (`slot_waits`) and the runs before a manual retry (`attempt_base`) do not
-- use up `max_attempts`. Exhausted when
--   attempt - attempt_base >= max_attempts + least(releases, 3) + slot_waits.
ALTER TABLE ingest_steps
  ADD COLUMN releases int NOT NULL DEFAULT 0,
  ADD COLUMN slot_waits int NOT NULL DEFAULT 0,
  ADD COLUMN attempt_base int NOT NULL DEFAULT 0;
