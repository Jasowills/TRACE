/**
 * mcp-server.ts — MCP server exposing the four TRACE tools.
 * Reads from Postgres derived state only (never the Redis stream directly,
 * except pipeline_health which reports broker-level lag/length).
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  traceEvent,
  findDroppedEvents,
  findDuplicateEvents,
  pipelineHealth,
} from "./tools.js";

function toText(obj: unknown): string {
  return JSON.stringify(obj, null, 2);
}

/**
 * Extract a non-empty message from anything thrown, including pg
 * AggregateError (whose .message is empty — ADV-0007). Agents get the
 * underlying reason instead of a blank error.
 */
function errorDetail(err: unknown): string {
  if (err instanceof AggregateError) {
    const parts = err.errors.map((e) =>
      e instanceof Error ? (e.message || e.name) : String(e),
    );
    return parts.length > 0 ? parts.join("; ") : "dependency error (no detail)";
  }
  if (err instanceof Error) return err.message || err.name;
  return String(err);
}

async function runTool<T>(name: string, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    throw new Error(`${name} failed: ${errorDetail(err)}`);
  }
}

export function createServer(): McpServer {
  const server = new McpServer({ name: "trace", version: "0.1.0" });

  server.tool(
    "trace_event",
    "Trace a single synthetic event through the toy ingestion pipeline. " +
      "Give it the event_id (UUID from producer_log). Returns the producer's " +
      "ground-truth record, the ordered stage history (received -> enriched -> written) " +
      "with timestamps and worker ids, and a final_status: 'written' means it reached " +
      "the sink; 'dropped' means the producer deliberately never sent it (--drop fault); " +
      "'in_flight' means it has partial stages but no written entry yet; " +
      "'missing_unexpectedly' means it was sent but has no trace at all (a real loss).",
    { event_id: z.string().describe("UUID of the event to trace") },
    async ({ event_id }) => ({
      content: [{ type: "text", text: toText(await runTool("trace_event", () => traceEvent(event_id))) }],
    }),
  );

  server.tool(
    "find_dropped_events",
    "Reconcile ground truth against the pipeline to find events that never reached " +
      "the sink. Compares producer_log (what the producer tried to send) with the " +
      "written trace entries and sink rows. Each result has expected=true when the " +
      "producer deliberately dropped it (--drop fault, fault_applied=dropped_before_send) " +
      "and expected=false when the event was sent but lost anyway (a genuine pipeline " +
      "failure). Optionally bound the search with ISO timestamps since/until on emitted_at.",
    {
      since: z.string().optional().describe("ISO timestamp lower bound on emitted_at"),
      until: z.string().optional().describe("ISO timestamp upper bound on emitted_at"),
    },
    async ({ since, until }) => ({
      content: [{ type: "text", text: toText(await runTool("find_dropped_events", () => findDroppedEvents({ since, until }))) }],
    }),
  );

  server.tool(
    "find_duplicate_events",
    "Find events the producer deliberately sent twice with the same event_id " +
      "(--duplicate fault, fault_applied=duplicated). occurrences is the number of " +
      "times it was sent (2). collapsed_correctly is true when the pipeline " +
      "collapsed the duplicate to exactly one sink row and one written trace entry. " +
      "Optionally bound with ISO timestamps since/until on emitted_at.",
    {
      since: z.string().optional().describe("ISO timestamp lower bound on emitted_at"),
      until: z.string().optional().describe("ISO timestamp upper bound on emitted_at"),
    },
    async ({ since, until }) => ({
      content: [{ type: "text", text: toText(await runTool("find_duplicate_events", () => findDuplicateEvents({ since, until }))) }],
    }),
  );

  server.tool(
    "pipeline_health",
    "Report current pipeline health: stream_length (total entries retained in " +
      "the Redis stream, INCLUDING already-consumed history — it only grows; " +
      "use consumer_lag for the actual backlog), consumer_lag (unacknowledged " +
      "messages in the ingest-worker group — the real 'is it keeping up' number), " +
      "events_per_second_5min (sink writes over the last 5 minutes), and " +
      "error_rate_5min (share of non-deliberately-dropped events emitted in the " +
      "last 5 minutes with no written trace entry). Takes no arguments. Use this " +
      "first when asking whether the pipeline is keeping up or losing events.",
    {},
    async () => ({
      content: [{ type: "text", text: toText(await runTool("pipeline_health", pipelineHealth)) }],
    }),
  );

  return server;
}

async function main(): Promise<void> {
  const server = createServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

if (process.argv[1]?.endsWith("mcp-server.ts")) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
