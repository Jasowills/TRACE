/**
 * producer.ts — synthetic event producer with first-class fault injection.
 *
 * Usage:
 *   npx tsx src/producer.ts --events 1000 --drop 5 --duplicate 3 --delay 2 --delay-ms 4000
 *
 * Schema note: producer_log.event_id is PRIMARY KEY, so a duplicated event
 * cannot have two rows with the same event_id. The producer therefore
 * UPSERTs: first insert with fault_applied='none', then after sending twice,
 * update to fault_applied='duplicated' with fault_meta={duplicate_of:<id>}.
 * Ground-truth queries use `fault_applied='duplicated'` (not row counts).
 */
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { getPool, closePool, STREAM_NAME } from "./db.js";
import type { FaultApplied } from "./db.js";

export interface ProducerOptions {
  events: number;
  drop: number; // percent 0-100
  duplicate: number; // percent 0-100
  delay: number; // percent 0-100
  delayMs: number;
  seed?: number;
}

export function parseArgs(argv: string[]): ProducerOptions {
  const opts: ProducerOptions = {
    events: 100,
    drop: 0,
    duplicate: 0,
    delay: 0,
    delayMs: 4000,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--events" && next !== undefined) opts.events = parseInt(next, 10);
    else if (a === "--drop" && next !== undefined) opts.drop = parseFloat(next);
    else if (a === "--duplicate" && next !== undefined)
      opts.duplicate = parseFloat(next);
    else if (a === "--delay" && next !== undefined)
      opts.delay = parseFloat(next);
    else if (a === "--delay-ms" && next !== undefined)
      opts.delayMs = parseInt(next, 10);
    else if (a === "--seed" && next !== undefined)
      opts.seed = parseInt(next, 10);
    if (
      ["--events", "--drop", "--duplicate", "--delay", "--delay-ms", "--seed"].includes(a)
    )
      i++;
  }
  return opts;
}

/** Mulberry32 seeded RNG for deterministic fault assignment in tests. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Assign a fault to each of N events. Fault classes are mutually exclusive;
 * indices are shuffled so faults are spread uniformly.
 * Returns array of FaultApplied of length N.
 */
export function planFaults(
  n: number,
  dropPct: number,
  dupPct: number,
  delayPct: number,
  rand: () => number = Math.random,
): FaultApplied[] {
  const faults: FaultApplied[] = new Array(n).fill("none");
  const indices = Array.from({ length: n }, (_, i) => i);
  // Fisher-Yates shuffle
  for (let i = indices.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [indices[i], indices[j]] = [indices[j], indices[i]];
  }
  let cursor = 0;
  const take = (pct: number): number =>
    Math.min(n - cursor, Math.floor((n * pct) / 100));
  const nDrop = take(dropPct);
  for (let k = 0; k < nDrop; k++) faults[indices[cursor++]] = "dropped_before_send";
  const nDup = take(dupPct);
  for (let k = 0; k < nDup; k++) faults[indices[cursor++]] = "duplicated";
  const nDelay = take(delayPct);
  for (let k = 0; k < nDelay; k++) faults[indices[cursor++]] = "delayed";
  return faults;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface ProduceResult {
  emitted: number;
  sent: number;
  dropped: number;
  duplicated: number;
  delayed: number;
  /** Stream writes that failed after the ground-truth row was recorded. */
  sendFailed: number;
}

function sendErrorDetail(err: unknown): string {
  if (err instanceof AggregateError)
    return err.errors.map((e) => (e as Error)?.message ?? String(e)).join("; ");
  return err instanceof Error ? err.message : String(err);
}

export async function produce(opts: ProducerOptions): Promise<ProduceResult> {
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
  const pool = getPool();
  const rand = opts.seed !== undefined ? mulberry32(opts.seed) : Math.random;
  const faults = planFaults(opts.events, opts.drop, opts.duplicate, opts.delay, rand);

  const result: ProduceResult = {
    emitted: opts.events,
    sent: 0,
    dropped: 0,
    duplicated: 0,
    delayed: 0,
    sendFailed: 0,
  };

  // Ground truth FIRST (ADV-0004): the producer_log row is written before the
  // stream entry, so a crash can only leave "row without stream entry"
  // (detectable as unexpected loss) — never "stream entry without row"
  // (an invisible phantom). A failed send is recorded on the row and the
  // batch continues instead of aborting.
  const recordGroundTruth = (
    eventId: string,
    emittedAt: string,
    fault: FaultApplied,
    meta: Record<string, unknown>,
  ) =>
    pool.query(
      `INSERT INTO producer_log (event_id, emitted_at, fault_applied, fault_meta)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (event_id) DO UPDATE SET
         fault_applied = EXCLUDED.fault_applied,
         fault_meta = EXCLUDED.fault_meta`,
      [eventId, emittedAt, fault, JSON.stringify(meta)],
    );

  const markSendFailed = (eventId: string, err: unknown) =>
    pool.query(
      `UPDATE producer_log
       SET fault_meta = COALESCE(fault_meta, '{}'::jsonb) || $2
       WHERE event_id = $1`,
      [eventId, JSON.stringify({ send_failed: true, send_error: sendErrorDetail(err).slice(0, 300) })],
    );

  const sendOnce = async (eventId: string, payload: string): Promise<boolean> => {
    try {
      await redis.xadd(STREAM_NAME, "*", "event_id", eventId, "body", payload);
      result.sent++;
      return true;
    } catch (err) {
      await markSendFailed(eventId, err);
      result.sendFailed++;
      return false;
    }
  };

  const makePayload = (eventId: string, emittedAt: string, i: number) =>
    JSON.stringify({
      event_id: eventId,
      emitted_at: emittedAt,
      index: i,
      data: { seq: i, note: "synthetic" },
    });

  for (let i = 0; i < opts.events; i++) {
    const fault = faults[i];
    const eventId = randomUUID();
    const emittedAt = new Date().toISOString();

    if (fault === "dropped_before_send") {
      // Recorded in ground truth, NEVER sent to the stream.
      await recordGroundTruth(eventId, emittedAt, "dropped_before_send", {});
      result.dropped++;
      continue;
    }

    if (fault === "delayed") {
      // emitted_at is stamped BEFORE the hold so received - emitted_at >= delayMs.
      await recordGroundTruth(eventId, emittedAt, "delayed", { delay_ms: opts.delayMs });
      await sleep(opts.delayMs);
      if (await sendOnce(eventId, makePayload(eventId, emittedAt, i))) result.delayed++;
      continue;
    }

    if (fault === "duplicated") {
      // Upsert: single ground-truth row marked duplicated (PK prevents 2 rows).
      await recordGroundTruth(eventId, emittedAt, "duplicated", { duplicate_of: eventId });
      // Sent TWICE with the same event_id.
      const payload = makePayload(eventId, emittedAt, i);
      const first = await sendOnce(eventId, payload);
      const second = await sendOnce(eventId, payload);
      if (first && second) result.duplicated++;
      continue;
    }

    // Normal path.
    await recordGroundTruth(eventId, emittedAt, "none", {});
    await sendOnce(eventId, makePayload(eventId, emittedAt, i));
  }

  redis.disconnect();
  return result;
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`producing ${opts.events} events (drop=${opts.drop}% dup=${opts.duplicate}% delay=${opts.delay}% delayMs=${opts.delayMs})`);
  const res = await produce(opts);
  console.log(JSON.stringify(res));
  await closePool();
  if (res.sendFailed > 0) {
    console.error(`${res.sendFailed} events failed to send (marked send_failed in producer_log)`);
    process.exit(1);
  }
  process.exit(0);
}

// Run as CLI only when executed directly, not when imported by tests.
if (process.argv[1]?.endsWith("producer.ts")) {
  main().catch(async (err) => {
    console.error(`producer aborted: ${sendErrorDetail(err)}`);
    await closePool();
    process.exit(1);
  });
}
