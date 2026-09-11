/**
 * processor.ts — ingestion worker.
 * Consumes Redis Stream "raw-events" via consumer group, runs three
 * genuinely separate stages (received -> enriched -> written), writing one
 * append-only row to event_trace_log per stage and a final sink_events row.
 * Idempotent on duplicates: same event_id collapses to one sink row and
 * one 'written' trace entry.
 */
import { Redis } from "ioredis";
import { getPool, STREAM_NAME, CONSUMER_GROUP } from "./db.js";

const WORKER_ID = process.env.WORKER_ID ?? `worker-${process.pid}`;
const BLOCK_MS = 2000;
const BATCH = 50;

interface RawEvent {
  event_id: string;
  emitted_at: string;
  index: number;
  data: Record<string, unknown>;
}

async function ensureGroup(redis: Redis): Promise<void> {
  try {
    await redis.xgroup("CREATE", STREAM_NAME, CONSUMER_GROUP, "0", "MKSTREAM");
  } catch (err: unknown) {
    // BUSYGROUP means it already exists — fine.
    if (err instanceof Error && err.message.includes("BUSYGROUP")) return;
    throw err;
  }
}

async function logStage(
  eventId: string,
  stage: "received" | "enriched" | "written",
  observedAt: Date = new Date(),
): Promise<void> {
  const pool = getPool();
  // Idempotency guard: never write a duplicate (event_id, stage) row.
  // Duplicated deliveries collapse: second delivery's stages are skipped
  // once 'written' already exists.
  if (stage === "received" || stage === "enriched") {
    const existing = await pool.query(
      `SELECT 1 FROM event_trace_log WHERE event_id = $1 AND stage = $2 LIMIT 1`,
      [eventId, stage],
    );
    if ((existing.rowCount ?? 0) > 0) return;
  }
  if (stage === "written") {
    const existing = await pool.query(
      `SELECT 1 FROM event_trace_log WHERE event_id = $1 AND stage = 'written' LIMIT 1`,
      [eventId],
    );
    if ((existing.rowCount ?? 0) > 0) return;
  }
  await pool.query(
    `INSERT INTO event_trace_log (event_id, stage, observed_at, worker_id)
     VALUES ($1, $2, $3, $4)`,
    [eventId, stage, observedAt.toISOString(), WORKER_ID],
  );
}

function parseBody(fields: string[]): RawEvent | null {
  const map: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) map[fields[i]] = fields[i + 1];
  const body = map["body"] ?? map["event_id"];
  try {
    const parsed = JSON.parse(body) as RawEvent;
    if (typeof parsed.event_id !== "string") return null;
    return parsed;
  } catch {
    return null;
  }
}

/** Stage 1: mark receipt. */
export async function stageReceived(event: RawEvent): Promise<void> {
  await logStage(event.event_id, "received");
}

/** Stage 2: enrichment (adds processed_at + worker_id to a derived payload). */
export async function stageEnriched(event: RawEvent): Promise<Record<string, unknown>> {
  const enriched = {
    ...event.data,
    _enriched_at: new Date().toISOString(),
    _worker_id: WORKER_ID,
    _source_emitted_at: event.emitted_at,
  };
  await logStage(event.event_id, "enriched");
  return enriched;
}

/** Stage 3: write to sink (idempotent) + log 'written'. */
export async function stageWritten(
  event: RawEvent,
  enriched: Record<string, unknown>,
): Promise<void> {
  const pool = getPool();
  const now = new Date().toISOString();
  await pool.query(
    `INSERT INTO sink_events (event_id, payload, written_at)
     VALUES ($1, $2, $3)
     ON CONFLICT (event_id) DO NOTHING`,
    [event.event_id, JSON.stringify({ ...enriched, event_index: event.index }), now],
  );
  await logStage(event.event_id, "written", new Date(now));
}

/** Full pipeline for one event: received -> enriched -> written. */
export async function processEvent(event: RawEvent): Promise<void> {
  // Skip entirely if already fully written (duplicate delivery).
  const pool = getPool();
  const done = await pool.query(
    `SELECT 1 FROM event_trace_log WHERE event_id = $1 AND stage = 'written' LIMIT 1`,
    [event.event_id],
  );
  if ((done.rowCount ?? 0) > 0) return;
  await stageReceived(event);
  const enriched = await stageEnriched(event);
  await stageWritten(event, enriched);
}

export async function runWorker(opts?: {
  redis?: Redis;
  once?: boolean;
  consumerName?: string;
}): Promise<void> {
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const redis: Redis = opts?.redis ?? new Redis(redisUrl, { maxRetriesPerRequest: null });
  const consumer = opts?.consumerName ?? `${WORKER_ID}-${Date.now()}`;
  await ensureGroup(redis);

  // Drain-then-exit in --once mode: first pass reads pending ("0"),
  // subsequent passes read new (">") until a ">" pass returns nothing.
  // In daemon mode this loops forever with blocking reads.
  let first = true;
  let passes = 0;
  for (;;) {
    const id = first ? "0" : ">";
    first = false;
    passes++;
    const res = (await redis.xreadgroup(
      "GROUP",
      CONSUMER_GROUP,
      consumer,
      "COUNT",
      BATCH,
      "BLOCK",
      opts?.once ? 1000 : BLOCK_MS,
      "STREAMS",
      STREAM_NAME,
      id,
    )) as [string, [string, string[]][]][] | null;

    const gotEntries = res !== null && res.some(([, entries]) => entries.length > 0);
    if (!gotEntries) {
      // Pass 1 is the pending ("0") pass — an empty pending list just means
      // nothing was left unacked; always continue to the live (">") passes.
      // Only break once a live pass comes back empty (fully drained).
      if (opts?.once && passes >= 2) break;
      if (opts?.once) continue;
      continue;
    }
    for (const [, entries] of res) {
      for (const [msgId, fields] of entries) {
        const event = parseBody(fields);
        if (event) {
          try {
            await processEvent(event);
          } catch (err) {
            console.error(`failed processing ${msgId}`, err);
          }
        }
        await redis.xack(STREAM_NAME, CONSUMER_GROUP, msgId);
      }
    }
  }

  if (!opts?.redis) redis.disconnect();
}

async function main(): Promise<void> {
  const once = process.argv.includes("--once");
  console.log(`worker ${WORKER_ID} starting (group=${CONSUMER_GROUP}, once=${once})`);
  if (once) {
    await runWorker({ once: true });
    const { closePool } = await import("./db.js");
    await closePool();
    process.exit(0);
  }
  await runWorker();
}

if (process.argv[1]?.endsWith("processor.ts")) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
