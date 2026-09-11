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
  // Atomic idempotency guard: the UNIQUE(event_id, stage) constraint makes
  // this a single atomic upsert, so concurrent workers cannot double-write
  // a stage (ADV-0002). No SELECT-then-INSERT.
  await pool.query(
    `INSERT INTO event_trace_log (event_id, stage, observed_at, worker_id)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (event_id, stage) DO NOTHING`,
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

/** Human-readable reason a stream entry was skipped (ADV-0006: never silent). */
function malformedReason(fields: string[]): string {
  const map: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) map[fields[i]] = fields[i + 1];
  const body = map["body"] ?? map["event_id"];
  if (body === undefined) return "no body field";
  try {
    const parsed = JSON.parse(body) as { event_id?: unknown };
    if (typeof parsed.event_id !== "string")
      return `event_id missing or not a string: ${body.slice(0, 120)}`;
    return "unknown parse failure";
  } catch {
    return `body is not JSON: ${String(body).slice(0, 120)}`;
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

async function processBatch(
  redis: Redis,
  consumer: string,
  entries: [string, string[]][],
  stats: WorkerStats,
): Promise<void> {
  for (const [msgId, fields] of entries) {
    const event = parseBody(fields);
    if (!event) {
      // Poison pill: ACK to keep the drain moving, but NEVER silently.
      console.warn(`skipping malformed stream entry ${msgId}: ${malformedReason(fields)}`);
      stats.skippedMalformed++;
      await redis.xack(STREAM_NAME, CONSUMER_GROUP, msgId);
      continue;
    }
    try {
      await processEvent(event);
      // ACK only on success. A failed message stays pending and is
      // retried on the next pass / next worker run.
      await redis.xack(STREAM_NAME, CONSUMER_GROUP, msgId);
      stats.processed++;
    } catch (err) {
      console.error(`failed processing ${msgId} (left unacked for redelivery)`, err);
      stats.failed++;
    }
  }
}

export interface WorkerStats {
  processed: number;
  failed: number;
  skippedMalformed: number;
}

export async function runWorker(opts?: {
  redis?: Redis;
  once?: boolean;
  consumerName?: string;
  /** Consecutive no-progress passes before --once gives up (default 5). */
  maxStuckPasses?: number;
}): Promise<WorkerStats> {
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const redis: Redis = opts?.redis ?? new Redis(redisUrl, { maxRetriesPerRequest: null });
  const consumer = opts?.consumerName ?? `${WORKER_ID}-${Date.now()}`;
  await ensureGroup(redis);

  const stats: WorkerStats = { processed: 0, failed: 0, skippedMalformed: 0 };
  const maxStuckPasses = opts?.maxStuckPasses ?? 5;

  // Drain order per pass: reclaimed (dead consumers) -> pending (own
  // redeliveries) -> new arrivals. Failed messages are deliberately LEFT
  // UNACKED (ADV-0001) so a transient outage retries instead of silent loss.
  // Each source is read until it comes back empty, so backlog size never
  // matters; --once exits on the first fully-empty pass, and throws
  // (exit 1 via main) only when passes keep finding work but nothing gets
  // acknowledged — i.e. genuinely stuck, not merely backlogged.
  let stuckPasses = 0;
  let lastAcked = 0;
  const ackedTotal = () => stats.processed + stats.skippedMalformed;

  for (;;) {
    let found = 0;

    // Reclaim entries stranded by dead consumers: XAUTOCLAIM moves PEL
    // entries idle >10s to this consumer. Plain XREADGROUP "0" only returns
    // a consumer's OWN pending, so without this, messages unacked by a
    // crashed/killed worker would stall forever. Safe under live duplicates:
    // stage writes are idempotent (UNIQUE + ON CONFLICT DO NOTHING).
    found += await claimLoop(redis, consumer, stats);
    // Own pending redeliveries.
    found += await readUntilEmpty(redis, consumer, stats, "0");
    // New arrivals (blocks briefly in --once, longer in daemon).
    found += await readUntilEmpty(redis, consumer, stats, ">", opts?.once ? 500 : BLOCK_MS);

    if (opts?.once && found === 0) break;

    if (ackedTotal() > lastAcked) {
      lastAcked = ackedTotal();
      stuckPasses = 0;
    } else if (found > 0) {
      stuckPasses++;
      if (opts?.once && stuckPasses >= maxStuckPasses) {
        if (!opts?.redis) redis.disconnect();
        throw new Error(
          `drain stuck: ${stuckPasses} consecutive passes found work but acknowledged nothing ` +
            `(processed=${stats.processed} failed=${stats.failed}): ` +
            `likely a dependency outage — fix it and re-run`,
        );
      }
    }

    if (!opts?.once && stats.failed > 0) {
      // Avoid hot-spinning a poisoned/dependency-down drain in daemon mode.
      await new Promise((r) => setTimeout(r, 1000));
      stats.failed = 0;
    }
  }

  if (!opts?.redis) redis.disconnect();
  return stats;

  /** Read one source ID until an empty read; returns entries seen. */
  async function readUntilEmpty(
    r: Redis,
    c: string,
    st: WorkerStats,
    id: "0" | ">",
    blockMs = 100,
  ): Promise<number> {
    let seen = 0;
    for (let i = 0; i < 200; i++) {
      const res = (await r.xreadgroup(
        "GROUP",
        CONSUMER_GROUP,
        c,
        "COUNT",
        BATCH,
        "BLOCK",
        blockMs,
        "STREAMS",
        STREAM_NAME,
        id,
      )) as [string, [string, string[]][]][] | null;
      const entries = res?.flatMap(([, e]) => e) ?? [];
      if (entries.length === 0) break;
      seen += entries.length;
      const before = ackedTotal();
      await processBatch(r, c, entries, st);
      if (ackedTotal() === before) break; // no progress — stop re-reading same work
    }
    return seen;
  }

  /** XAUTOCLAIM loop; returns entries claimed. */
  async function claimLoop(r: Redis, c: string, st: WorkerStats): Promise<number> {
    let claimed = 0;
    let cursor = "0-0";
    for (;;) {
      let res: [string, [string, string[]][]];
      try {
        res = (await r.xautoclaim(
          STREAM_NAME,
          CONSUMER_GROUP,
          c,
          10_000,
          cursor,
          "COUNT",
          BATCH,
        )) as [string, [string, string[]][]];
      } catch {
        break; // group/stream gone — nothing to reclaim
      }
      const [nextCursor, entries] = res;
      if (entries.length > 0) {
        claimed += entries.length;
        const before = ackedTotal();
        await processBatch(r, c, entries, st);
        if (ackedTotal() === before) {
          // Claimed work that won't acknowledge (outage): stop, outer
          // stuck-detection decides whether to throw.
          if (nextCursor === "0-0") break;
          cursor = nextCursor;
          continue;
        }
      }
      if (nextCursor === "0-0") break;
      cursor = nextCursor;
    }
    return claimed;
  }
}

async function main(): Promise<void> {
  const once = process.argv.includes("--once");
  console.log(`worker ${WORKER_ID} starting (group=${CONSUMER_GROUP}, once=${once})`);
  if (once) {
    const stats = await runWorker({ once: true });
    console.log(`drain complete: ${JSON.stringify(stats)}`);
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
