/**
 * Delayed-fault ground truth (§5): a delayed event's 'received' timestamp
 * must be measurably later than its producer-stamped emitted_at.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { getPool } from "../src/db.js";
import { traceEvent } from "../src/tools.js";
import { migrate, resetAll, runPipeline } from "./helpers.js";

describe("delayed fault ground truth (§5)", () => {
  beforeAll(async () => {
    await migrate();
    await resetAll();
    await runPipeline({ events: 10, drop: 0, duplicate: 0, delay: 40, delayMs: 1500 });
  }, 120_000);

  it("delayed events arrive measurably later than emitted_at", async () => {
    const delayed = await getPool().query(
      `SELECT event_id, emitted_at FROM producer_log WHERE fault_applied = 'delayed'`,
    );
    expect(delayed.rowCount).toBe(4); // floor(10 * 40%)
    for (const row of delayed.rows) {
      const traced = await traceEvent(row.event_id as string);
      expect(traced.final_status).toBe("written");
      const received = traced.stages.find((s) => s.stage === "received");
      expect(received).toBeDefined();
      const lagMs =
        new Date(received!.observed_at).getTime() -
        new Date(row.emitted_at as Date).getTime();
      expect(lagMs).toBeGreaterThanOrEqual(1400); // 1500ms hold minus scheduling slack
    }
  });

  it("delayed lag exceeds any scheduling noise floor", async () => {
    // Every delayed event waited out the full hold: lag must clear delayMs
    // minus a small allowance for timer/DB-write granularity.
    const delayed = await getPool().query(
      `SELECT event_id, emitted_at, fault_meta FROM producer_log WHERE fault_applied = 'delayed'`,
    );
    for (const row of delayed.rows) {
      const meta = row.fault_meta as { delay_ms: number };
      expect(meta.delay_ms).toBe(1500);
      const traced = await traceEvent(row.event_id as string);
      const received = traced.stages.find((s) => s.stage === "received")!;
      const lagMs =
        new Date(received.observed_at).getTime() -
        new Date(row.emitted_at as Date).getTime();
      expect(lagMs).toBeGreaterThanOrEqual(meta.delay_ms - 100);
    }
  });
});
