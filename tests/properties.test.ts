/**
 * §6.2 Property-based tests (fast-check + live pipeline).
 * Each property runs the real producer + worker, then asserts the invariant.
 */
import { describe, it, expect, beforeAll } from "vitest";
import * as fc from "fast-check";
import { getPool } from "../src/db.js";
import { traceEvent, findDuplicateEvents } from "../src/tools.js";
import { produce, planFaults, mulberry32 } from "../src/producer.js";
import { runWorker } from "../src/processor.js";
import { migrate, resetAll } from "./helpers.js";

beforeAll(async () => {
  await migrate();
});

async function freshRun(seed: number, n: number, drop: number, dup: number) {
  await resetAll();
  await produce({
    events: n,
    drop,
    duplicate: dup,
    delay: 0,
    delayMs: 0,
    seed,
  });
  await runWorker({ once: true });
}

describe("pipeline properties (§6.2)", () => {
  it("No silent loss: every non-dropped producer event has a 'received' trace", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 10_000 }),
        fc.integer({ min: 5, max: 60 }),
        async (seed, n) => {
          await freshRun(seed, n, 10, 5);
          const missing = await getPool().query(
            `SELECT p.event_id FROM producer_log p
             LEFT JOIN event_trace_log t
               ON t.event_id = p.event_id AND t.stage = 'received'
             WHERE p.fault_applied != 'dropped_before_send'
               AND t.event_id IS NULL`,
          );
          expect(missing.rowCount).toBe(0);
        },
      ),
      { numRuns: 5 },
    );
  }, 180_000);

  it("No phantom success: every sink row has a 'written' trace + producer record", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 10_000 }),
        fc.integer({ min: 5, max: 60 }),
        async (seed, n) => {
          await freshRun(seed, n, 10, 5);
          const orphans = await getPool().query(
            `SELECT s.event_id FROM sink_events s
             LEFT JOIN event_trace_log t
               ON t.event_id = s.event_id AND t.stage = 'written'
             LEFT JOIN producer_log p ON p.event_id = s.event_id
             WHERE t.event_id IS NULL OR p.event_id IS NULL`,
          );
          expect(orphans.rowCount).toBe(0);
        },
      ),
      { numRuns: 5 },
    );
  }, 180_000);

  it("Duplicate collapse: fault_applied='duplicated' => exactly one sink row", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 10_000 }),
        // n >= 20 so floor(n*10%) >= 2 duplicated events always exist
        fc.integer({ min: 20, max: 60 }),
        async (seed, n) => {
          await freshRun(seed, n, 0, 10);
          const dupIds = await getPool().query(
            `SELECT event_id FROM producer_log WHERE fault_applied = 'duplicated'`,
          );
          expect(dupIds.rowCount).toBeGreaterThan(0);
          for (const row of dupIds.rows) {
            const c = await getPool().query(
              `SELECT COUNT(*) AS c FROM sink_events WHERE event_id = $1`,
              [row.event_id],
            );
            expect(parseInt(c.rows[0].c as string, 10)).toBe(1);
          }
          const { events } = await findDuplicateEvents();
          for (const e of events) expect(e.collapsed_correctly).toBe(true);
        },
      ),
      { numRuns: 5 },
    );
  }, 180_000);

  it("Trace ordering: stages chronological, no dup consecutive stages, no gaps", async () => {
    await freshRun(1234, 40, 10, 10);
    const ids = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied != 'dropped_before_send' LIMIT 20`,
    );
    const ORDER = ["received", "enriched", "written"];
    for (const row of ids.rows) {
      const traced = await traceEvent(row.event_id as string);
      const stages = traced.stages;
      expect(stages.length).toBeGreaterThan(0);
      // Chronologically ordered.
      const times = stages.map((s) => new Date(s.observed_at).getTime());
      expect([...times].sort((a, b) => a - b)).toEqual(times);
      // No duplicate consecutive stages.
      for (let i = 1; i < stages.length; i++) {
        expect(stages[i].stage).not.toBe(stages[i - 1].stage);
      }
      // No gaps: stage indices strictly increase along the canonical order.
      const idx = stages.map((s) => ORDER.indexOf(s.stage));
      for (const j of idx) expect(j).toBeGreaterThanOrEqual(0);
      for (let i = 1; i < idx.length; i++) {
        expect(idx[i]).toBeGreaterThan(idx[i - 1]);
      }
      // Written events show the full chain.
      if (traced.final_status === "written") {
        expect(stages.map((s) => s.stage)).toEqual(ORDER);
      }
    }
  }, 120_000);

  it("planFaults model check: counts always exact regardless of seed (pure fast-check)", () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 1_000_000 }),
        fc.integer({ min: 0, max: 100 }),
        fc.integer({ min: 0, max: 100 }),
        fc.integer({ min: 0, max: 100 }),
        fc.integer({ min: 0, max: 100 }),
        (seed, n, drop, dup, delay) => {
          const faults = planFaults(n, drop, dup, delay, mulberry32(seed));
          expect(faults).toHaveLength(n);
          // Fault classes are mutually exclusive; total assigned <= n.
          const assigned = faults.filter((f) => f !== "none").length;
          expect(assigned).toBeLessThanOrEqual(n);
          // Drop count is exact floor share unless capped by remaining slots.
          const nDrop = faults.filter((f) => f === "dropped_before_send").length;
          expect(nDrop).toBe(Math.min(n, Math.floor((n * drop) / 100)));
        },
      ),
      { numRuns: 100 },
    );
  });
});
