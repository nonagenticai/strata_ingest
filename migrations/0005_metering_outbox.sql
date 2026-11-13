-- Outbox for billing cost events (`billing:costs`, src/metering.rs).
--
-- Before: the worker XADDed a step's cost event fire-and-forget AFTER `complete_and_enqueue`
-- committed, so a Redis outage, or a crash between the commit and the XADD, lost the event.
-- Now every paid provider call gets a row here the moment the call returns (`metering::record`,
-- its own short transaction), independent of the step's completion and its fence: the provider
-- was already paid, so the row exists even when the step's result is then fenced out or fails.
-- The drainer (`metering::run_drainer`) leases due rows, XADDs them outside any transaction and
-- marks them `sent` once Redis acknowledged (at-least-once; billing dedups on `event_key`).
--
-- The ledger is `ingest_steps` (provider, model, usage, cost_micros of the committed attempt)
-- plus this table for calls whose step did not commit. No foreign key to the step or job on
-- purpose: a billing record must outlive whatever happens to the job's rows.
CREATE TABLE metering_outbox (
  id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant          uuid        NOT NULL,
  -- The billing idempotency key, `<job_id>:<kind>/<key>:<attempt>` (one provider call per step
  -- attempt; unchanged by the switch to the outbox), so a re-send dedups in billing.
  event_key       text        NOT NULL UNIQUE,
  -- The flat string→string field map XADDed to the stream, frozen when the call returned.
  fields          jsonb       NOT NULL,
  -- pending: not yet acknowledged by Redis. sent: XADD returned an id.
  status          text        NOT NULL DEFAULT 'pending',
  attempts        integer     NOT NULL DEFAULT 0,
  last_error      text,
  -- Due time; also the claim lease (a claimed row is pushed past the lease until marked).
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  sent_at         timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT metering_outbox_status_ck CHECK (status IN ('pending', 'sent')),
  CONSTRAINT metering_outbox_sent_is_stamped_ck CHECK (status <> 'sent' OR sent_at IS NOT NULL)
);

-- The drainer's read: oldest due pending rows. Partial: sent rows are history, not work.
CREATE INDEX metering_outbox_due_idx ON metering_outbox (next_attempt_at) WHERE status = 'pending';
-- The retention prune of sent rows.
CREATE INDEX metering_outbox_sent_idx ON metering_outbox (sent_at) WHERE status = 'sent';
