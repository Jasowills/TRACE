/**
 * §6.3 MCP protocol-level integration tests.
 * Spins up the ACTUAL MCP server process and talks to it with a REAL MCP
 * client over stdio — proving the transport/schema layer, not just the
 * functions underneath it. One test per tool (§7).
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { getPool } from "../src/db.js";
import { migrate, resetAll, runPipeline } from "./helpers.js";

let client: Client;
let transport: StdioClientTransport;
let knownEventId: string;

async function callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
  const res = await client.callTool({ name, arguments: args });
  const content = (res as { content: Array<{ type: string; text: string }> }).content;
  expect(content).toHaveLength(1);
  expect(content[0].type).toBe("text");
  return JSON.parse(content[0].text as string);
}

describe("MCP protocol integration (§6.3)", () => {
  beforeAll(async () => {
    await migrate();
    await resetAll();
    await runPipeline({ events: 50, drop: 10, duplicate: 10, delay: 0, delayMs: 0 });
    const row = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied = 'none' LIMIT 1`,
    );
    knownEventId = row.rows[0].event_id as string;

    transport = new StdioClientTransport({
      command: "npx",
      args: ["tsx", "src/mcp-server.ts"],
      env: { ...process.env } as Record<string, string>,
    });
    client = new Client({ name: "trace-test-client", version: "0.0.0" });
    await client.connect(transport);
  }, 120_000);

  afterAll(async () => {
    await client?.close().catch(() => undefined);
  });

  it("lists all four tools", async () => {
    const { tools } = await client.listTools();
    const names = tools.map((t) => t.name).sort();
    expect(names).toEqual([
      "find_dropped_events",
      "find_duplicate_events",
      "pipeline_health",
      "trace_event",
    ]);
    // Every tool must carry a usable description (spec §7 requirement).
    for (const t of tools) {
      expect(t.description && t.description.length).toBeGreaterThan(50);
    }
  });

  it("trace_event over the protocol", async () => {
    const res = (await callTool("trace_event", { event_id: knownEventId })) as {
      event_id: string;
      final_status: string;
      stages: Array<{ stage: string }>;
    };
    expect(res.event_id).toBe(knownEventId);
    expect(res.final_status).toBe("written");
    expect(res.stages.map((s) => s.stage)).toEqual(["received", "enriched", "written"]);
  });

  it("find_dropped_events over the protocol", async () => {
    const res = (await callTool("find_dropped_events", {})) as {
      count: number;
      events: Array<{ event_id: string; expected: boolean; reason: string }>;
    };
    const expected = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied = 'dropped_before_send'`,
    );
    expect(res.count).toBe(expected.rowCount);
    expect(new Set(res.events.map((e) => e.event_id))).toEqual(
      new Set(expected.rows.map((r) => r.event_id)),
    );
  });

  it("find_duplicate_events over the protocol", async () => {
    const res = (await callTool("find_duplicate_events", {})) as {
      count: number;
      events: Array<{ event_id: string; occurrences: number; collapsed_correctly: boolean }>;
    };
    const expected = await getPool().query(
      `SELECT event_id FROM producer_log WHERE fault_applied = 'duplicated'`,
    );
    expect(res.count).toBe(expected.rowCount);
    for (const e of res.events) {
      expect(e.occurrences).toBe(2);
      expect(e.collapsed_correctly).toBe(true);
    }
  });

  it("pipeline_health over the protocol", async () => {
    const res = (await callTool("pipeline_health", {})) as {
      stream_length: number;
      consumer_lag: number;
      events_per_second_5min: number;
      error_rate_5min: number;
    };
    expect(typeof res.stream_length).toBe("number");
    expect(typeof res.consumer_lag).toBe("number");
    expect(typeof res.events_per_second_5min).toBe("number");
    expect(typeof res.error_rate_5min).toBe("number");
    expect(res.error_rate_5min).toBe(0);
  });
});
