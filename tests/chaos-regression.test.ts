/**
 * Regression tests for adversary findings (ADV-0002, ADV-0004).
 * These failed on the pre-fix code and must keep passing.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { getPool } from "../src/db.js";
import { processEvent } from "../src/processor.js";
import { produce } from "../src/producer.js";
import { runWorker } from "../src/processor.js";
import { migrate, resetAll } from "./helpers.js";

describe("adversary regressions", () => {
  beforeAll(async () => {
    await migrate();
  });

  it("ADV-0002: concurrent duplicate processing writes exactly one row per stage", async () => {
    await resetAll();
    const evs = Array.from({ length: 20 }, (_, i) => ({
      event_id: `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
      emitted_at: new Date().toISOString(),
      index: i,
      data: { seq: i },
    }));
    await Promise.all(evs.flatMap((ev) => [processEvent(ev), processEvent(ev)]));
    const dups = await getPool().query(
      `SELECT COUNT(*) AS c FROM (
         SELECT 1 FROM event_trace_log GROUP BY event_id, stage HAVING COUNT(*) > 1
       ) t`,
    );
    expect(parseInt(dups.rows[0].c as string, 10)).toBe(0);
    const stages = await getPool().query(
      `SELECT COUNT(*) AS c FROM event_trace_log`,
    );
    // 20 events x 3 stages, exactly one row each.
    expect(parseInt(stages.rows[0].c as string, 10)).toBe(60);
  });

  it("ADV-0004: every produced event has a ground-truth row (no phantoms possible)", async () => {
    await resetAll();
    const res = await produce({ events: 30, drop: 10, duplicate: 10, delay: 0, delayMs: 0, seed: 5 });
    expect(res.sendFailed).toBe(0);
    const count = await getPool().query(`SELECT COUNT(*) AS c FROM producer_log`);
    // Ground truth is written first: one row per emitted event, always.
    expect(parseInt(count.rows[0].c as string, 10)).toBe(30);
    await runWorker({ once: true });
    const orphans = await getPool().query(
      `SELECT COUNT(*) AS c FROM sink_events s
       LEFT JOIN event_trace_log t ON t.event_id = s.event_id AND t.stage = 'written'
       LEFT JOIN producer_log p ON p.event_id = s.event_id
       WHERE t.event_id IS NULL OR p.event_id IS NULL`,
    );
    expect(parseInt(orphans.rows[0].c as string, 10)).toBe(0);
  });
});
