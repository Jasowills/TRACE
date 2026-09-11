/**
 * Security negative-path tests (OWASP A05, A10).
 * The attacker, not the user: hostile input must fail closed — no SQL
 * breakout, no data dump, no throw with internals, tables intact.
 */
import { describe, it, expect, beforeAll } from "vitest";
import { getPool } from "../src/db.js";
import { traceEvent, findDroppedEvents, findDuplicateEvents } from "../src/tools.js";
import { migrate, resetAll, runPipeline } from "./helpers.js";

const SQLI = [
  "' OR '1'='1",
  "'; DROP TABLE sink_events;--",
  "' UNION SELECT event_id FROM producer_log--",
  "\\'; DELETE FROM event_trace_log;--",
];

async function tableCounts(): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const t of ["producer_log", "event_trace_log", "sink_events"]) {
    const r = await getPool().query(`SELECT COUNT(*) AS c FROM ${t}`);
    out[t] = r.rows[0].c as string;
  }
  return out;
}

describe("security negative paths (A05/A10)", () => {
  beforeAll(async () => {
    await migrate();
    await resetAll();
    await runPipeline({ events: 20, drop: 10, duplicate: 10, delay: 0, delayMs: 0, seed: 77 });
  }, 120_000);

  it("A05: SQL injection via trace_event returns nothing and touches nothing", async () => {
    const before = await tableCounts();
    for (const payload of SQLI) {
      const res = await traceEvent(payload);
      // Fails closed: unknown id, no stages, no leak of other rows.
      expect(res.producer_record).toBeNull();
      expect(res.stages).toEqual([]);
      expect(res.final_status).toBe("missing_unexpectedly");
    }
    expect(await tableCounts()).toEqual(before);
  });

  it("A05: SQL injection via since/until filters fails closed with a clean error", async () => {
    const before = await tableCounts();
    for (const payload of SQLI) {
      // Invalid timestamps are rejected at the boundary with an
      // agent-readable error — never reach SQL, never dump rows.
      await expect(findDroppedEvents({ since: payload })).rejects.toThrow(/ISO timestamp/);
      await expect(findDuplicateEvents({ until: payload })).rejects.toThrow(/ISO timestamp/);
    }
    expect(await tableCounts()).toEqual(before);
  });

  it("A05: hostile unicode/quote event_ids are inert", async () => {
    const weird = ["\u0000", "''\"\"``", "a".repeat(500), "%00", "\nDROP"];
    for (const id of weird) {
      const res = await traceEvent(id);
      expect(res.event_id).toBe(id);
      expect(["missing_unexpectedly", "dropped", "in_flight", "written"]).toContain(
        res.final_status,
      );
    }
  });

  it("A10: error contract — unknown ids never throw or leak internals", async () => {
    const res = await traceEvent("00000000-0000-4000-8000-ffffffffffff");
    expect(res).toEqual({
      event_id: "00000000-0000-4000-8000-ffffffffffff",
      producer_record: null,
      stages: [],
      final_status: "missing_unexpectedly",
    });
    // Nothing in a normal tool result should carry stack traces or SQL.
    expect(JSON.stringify(res)).not.toMatch(/at async|node_modules|SELECT|pg-pool/i);
  });
});
