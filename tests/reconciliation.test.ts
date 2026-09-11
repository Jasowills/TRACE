/**
 * §6.1 Ground-truth reconciliation test (the core proof).
 * Produces with known flags, then compares MCP tool output against
 * producer_log-derived expectations with EXACT set matching.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { getPool } from "../src/db.js";
import { findDroppedEvents, findDuplicateEvents } from "../src/tools.js";
import { migrate, resetAll, runPipeline } from "./helpers.js";

describe("ground-truth reconciliation (§6.1)", () => {
  beforeAll(async () => {
    await migrate();
    await resetAll();
    // Headline demo parameters straight from the spec.
    await runPipeline({ events: 1000, drop: 5, duplicate: 3, delay: 0, delayMs: 0 });
  }, 180_000);

  it("find_dropped_events matches the injected drop set exactly", async () => {
    const expected = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied = 'dropped_before_send'`,
    );
    const expectedSet = new Set(expected.rows.map((r) => r.event_id as string));

    const { count, events } = await findDroppedEvents();
    const returnedExpected = events.filter((e) => e.expected);
    const returnedUnexpected = events.filter((e) => !e.expected);
    const returnedSet = new Set(returnedExpected.map((e) => e.event_id));

    // Zero false negatives: every injected drop is reported.
    for (const id of expectedSet) {
      expect(returnedSet.has(id)).toBe(true);
    }
    // Zero false positives: every reported expected-drop was really injected.
    for (const id of returnedSet) {
      expect(expectedSet.has(id)).toBe(true);
    }
    expect(returnedSet.size).toBe(expectedSet.size);
    expect(count).toBeGreaterThan(0);
    // No genuine loss: everything sent actually arrived.
    expect(returnedUnexpected).toEqual([]);
  });

  it("find_duplicate_events matches the injected duplicate set exactly", async () => {
    const expected = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied = 'duplicated'`,
    );
    const expectedSet = new Set(expected.rows.map((r) => r.event_id as string));

    const { events } = await findDuplicateEvents();
    const returnedSet = new Set(events.map((e) => e.event_id));

    for (const id of expectedSet) {
      expect(returnedSet.has(id)).toBe(true);
    }
    for (const id of returnedSet) {
      expect(expectedSet.has(id)).toBe(true);
    }
    expect(returnedSet.size).toBe(expectedSet.size);

    // Every duplicate collapsed to exactly one sink row.
    for (const e of events) {
      expect(e.occurrences).toBe(2);
      expect(e.collapsed_correctly).toBe(true);
    }

    // Independent cross-check at the SQL level: exactly one sink row each.
    for (const id of expectedSet) {
      const sink = await getPool().query(
        `SELECT COUNT(*) AS c FROM sink_events WHERE event_id = $1`,
        [id],
      );
      expect(parseInt(sink.rows[0].c as string, 10)).toBe(1);
    }
  });
});
