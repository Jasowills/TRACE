-- Ground-truth record of every event the producer claims to have emitted.
-- Written by the producer itself, independent of whether the pipeline
-- ever actually processes it. This table IS the ground truth for validation.
CREATE TABLE IF NOT EXISTS producer_log (
  event_id      UUID PRIMARY KEY,
  emitted_at    TIMESTAMPTZ NOT NULL,
  fault_applied TEXT NOT NULL DEFAULT 'none',        -- 'none' | 'dropped_before_send' | 'duplicated' | 'delayed'
  fault_meta    JSONB
);

-- NOTE: duplicated events reuse the same event_id, so a second producer_log
-- row for the same event would violate the PRIMARY KEY above. The producer
-- therefore UPSERTs: the 'duplicated' row overwrites fault_applied/fault_meta
-- while preserving the ORIGINAL emitted_at. fault_meta.duplicate_of points
-- at the event_id itself (self-reference) to mark the duplication.
-- See src/producer.ts for the exact upsert.


-- Append-only. One row per (event_id, stage) transition observed by the pipeline.
-- The UNIQUE constraint is load-bearing: it makes the idempotency guard
-- atomic (INSERT ... ON CONFLICT DO NOTHING) so concurrent workers cannot
-- double-write a stage (see ADV-0002).
CREATE TABLE IF NOT EXISTS event_trace_log (
  id            BIGSERIAL PRIMARY KEY,
  event_id      UUID NOT NULL,
  stage         TEXT NOT NULL,   -- 'received' | 'enriched' | 'written'
  observed_at   TIMESTAMPTZ NOT NULL,
  worker_id     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trace_event_id ON event_trace_log (event_id);
CREATE INDEX IF NOT EXISTS idx_trace_stage ON event_trace_log (stage);


-- Final sink table, one row per successfully-written event.
CREATE TABLE IF NOT EXISTS sink_events (
  event_id      UUID PRIMARY KEY,
  payload       JSONB NOT NULL,
  written_at    TIMESTAMPTZ NOT NULL
);

-- Idempotency backstop (ADV-0002): exactly one row per (event_id, stage).
-- The plain DELETE collapses any duplicates first (no-op on clean DBs),
-- then the unique index makes INSERT ... ON CONFLICT (event_id, stage)
-- atomic. Both statements are safe to re-run on every migrate.
DELETE FROM event_trace_log a USING event_trace_log b
  WHERE a.id > b.id
    AND a.event_id = b.event_id
    AND a.stage = b.stage;
CREATE UNIQUE INDEX IF NOT EXISTS uq_trace_event_stage
  ON event_trace_log (event_id, stage);
