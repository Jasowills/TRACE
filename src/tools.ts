/**
 * tools.ts — shared implementations of the four MCP tools.
 * Queried against Postgres derived state only (never Redis, except health).
 */
import { Redis } from "ioredis";
import { getPool, STREAM_NAME, CONSUMER_GROUP } from "./db.js";

export interface TraceStage {
  stage: string;
  observed_at: string;
  worker_id: string;
}

export interface TraceEventResult {
  event_id: string;
  producer_record: {
    emitted_at: string;
    fault_applied: string;
    fault_meta: unknown;
  } | null;
  stages: TraceStage[];
  final_status: "written" | "dropped" | "in_flight" | "missing_unexpectedly";
}

export async function traceEvent(eventId: string): Promise<TraceEventResult> {
  const pool = getPool();
  const prod = await pool.query(
    `SELECT emitted_at, fault_applied, fault_meta FROM producer_log WHERE event_id = $1`,
    [eventId],
  );
  const stagesRes = await pool.query(
    `SELECT stage, observed_at, worker_id FROM event_trace_log
     WHERE event_id = $1 ORDER BY observed_at ASC, id ASC`,
    [eventId],
  );
  const stages: TraceStage[] = stagesRes.rows.map((r) => ({
    stage: r.stage as string,
    observed_at: (r.observed_at as Date).toISOString(),
    worker_id: r.worker_id as string,
  }));
  const producer_record =
    prod.rowCount === 0
      ? null
      : {
          emitted_at: (prod.rows[0].emitted_at as Date).toISOString(),
          fault_applied: prod.rows[0].fault_applied as string,
          fault_meta: prod.rows[0].fault_meta as unknown,
        };

  const hasWritten = stages.some((s) => s.stage === "written");
  const hasAnyStage = stages.length > 0;
  let final_status: TraceEventResult["final_status"];
  if (hasWritten) final_status = "written";
  else if (producer_record?.fault_applied === "dropped_before_send")
    final_status = "dropped";
  else if (hasAnyStage) final_status = "in_flight";
  else final_status = "missing_unexpectedly";

  return { event_id: eventId, producer_record, stages, final_status };
}

export interface DroppedEvent {
  event_id: string;
  expected: boolean;
  reason: string;
}

export async function findDroppedEvents(opts?: {
  since?: string;
  until?: string;
}): Promise<{ count: number; events: DroppedEvent[] }> {
  const pool = getPool();
  const conditions: string[] = [];
  const params: string[] = [];
  if (opts?.since) {
    params.push(opts.since);
    conditions.push(`p.emitted_at >= $${params.length}`);
  }
  if (opts?.until) {
    params.push(opts.until);
    conditions.push(`p.emitted_at <= $${params.length}`);
  }
  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  // An event is "dropped" if ground truth says it was emitted but there is
  // no 'written' trace entry and no sink row.
  const res = await pool.query(
    `SELECT p.event_id, p.fault_applied, p.fault_meta
     FROM producer_log p
     LEFT JOIN (
       SELECT DISTINCT event_id FROM event_trace_log WHERE stage = 'written'
     ) t ON t.event_id = p.event_id
     LEFT JOIN sink_events s ON s.event_id = p.event_id
     ${where}
     ${where ? "AND" : "WHERE"} t.event_id IS NULL AND s.event_id IS NULL
     ORDER BY p.emitted_at ASC`,
    params,
  );
  const events: DroppedEvent[] = res.rows.map((r) => {
    const expected = r.fault_applied === "dropped_before_send";
    const sendFailed =
      typeof r.fault_meta === "object" &&
      r.fault_meta !== null &&
      (r.fault_meta as Record<string, unknown>).send_failed === true;
    return {
      event_id: r.event_id as string,
      expected,
      reason: expected
        ? "deliberate fault: producer recorded dropped_before_send and never sent to stream"
        : sendFailed
          ? "producer failed to send this event (transport error recorded in fault_meta) — it never reached the stream"
          : "unexpected loss: producer sent the event but it never reached the sink",
    };
  });
  return { count: events.length, events };
}

export interface DuplicateEvent {
  event_id: string;
  occurrences: number;
  collapsed_correctly: boolean;
}

export async function findDuplicateEvents(opts?: {
  since?: string;
  until?: string;
}): Promise<{ count: number; events: DuplicateEvent[] }> {
  const pool = getPool();
  const conditions: string[] = ["p.fault_applied = 'duplicated'"];
  const params: string[] = [];
  if (opts?.since) {
    params.push(opts.since);
    conditions.push(`p.emitted_at >= $${params.length}`);
  }
  if (opts?.until) {
    params.push(opts.until);
    conditions.push(`p.emitted_at <= $${params.length}`);
  }
  const res = await pool.query(
    `SELECT p.event_id,
            (SELECT COUNT(*) FROM sink_events s WHERE s.event_id = p.event_id) AS sink_count,
            (SELECT COUNT(*) FROM event_trace_log t WHERE t.event_id = p.event_id AND t.stage = 'written') AS written_count
     FROM producer_log p
     WHERE ${conditions.join(" AND ")}
     ORDER BY p.emitted_at ASC`,
    params,
  );
  const events: DuplicateEvent[] = res.rows.map((r) => {
    const sinkCount = parseInt(r.sink_count as string, 10);
    const writtenCount = parseInt(r.written_count as string, 10);
    return {
      event_id: r.event_id as string,
      occurrences: 2, // duplicated fault = sent twice with same event_id
      collapsed_correctly: sinkCount === 1 && writtenCount === 1,
    };
  });
  return { count: events.length, events };
}

export interface PipelineHealth {
  stream_length: number;
  consumer_lag: number;
  events_per_second_5min: number;
  error_rate_5min: number;
}

export async function pipelineHealth(): Promise<PipelineHealth> {
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
  let stream_length = 0;
  let consumer_lag = 0;
  try {
    stream_length = await redis.xlen(STREAM_NAME);
    try {
      // XPENDING summary: total pending = lag.
      const pending = (await redis.xpending(
        STREAM_NAME,
        CONSUMER_GROUP,
      )) as unknown as [number, string | null, string | null, unknown[]] | null;
      if (Array.isArray(pending) && typeof pending[0] === "number") {
        consumer_lag = pending[0];
      }
    } catch {
      consumer_lag = 0; // group may not exist yet
    }
  } finally {
    redis.disconnect();
  }

  const pool = getPool();
  const epsRes = await pool.query(
    `SELECT COUNT(*) AS c FROM sink_events
     WHERE written_at >= NOW() - INTERVAL '5 minutes'`,
  );
  const epsCount = parseInt(epsRes.rows[0].c as string, 10);
  const events_per_second_5min = epsCount / 300;

  // Error rate: share of non-deliberately-dropped producer events from the
  // last 5 minutes with no 'written' trace entry.
  const errRes = await pool.query(
    `SELECT
       COUNT(*) AS total,
       COUNT(*) FILTER (
         WHERE p.fault_applied != 'dropped_before_send'
           AND t.event_id IS NULL
       ) AS missing
     FROM producer_log p
     LEFT JOIN (
       SELECT DISTINCT event_id FROM event_trace_log WHERE stage = 'written'
     ) t ON t.event_id = p.event_id
     WHERE p.emitted_at >= NOW() - INTERVAL '5 minutes'`,
  );
  const total = parseInt(errRes.rows[0].total as string, 10);
  const missing = parseInt(errRes.rows[0].missing as string, 10);
  const error_rate_5min = total === 0 ? 0 : missing / total;

  return { stream_length, consumer_lag, events_per_second_5min, error_rate_5min };
}
