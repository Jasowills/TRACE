# TRACE — Telemetry Reconciliation And Correctness Engine

An MCP server that traces synthetic events through a toy ingestion pipeline.
Built to demonstrate the exact tool PostHog's own Q3 2026 objectives name as
unbuilt: *"Ship an ingestion MCP for tracing events through the pipeline."*

The pipeline mirrors PostHog's documented architecture
(Capture → Kafka → ingestion worker → ClickHouse) with lighter local
equivalents — **Redis Streams** for Kafka, **Postgres** for ClickHouse — so the
whole thing runs in Docker Compose with no cloud dependency.

```
producer.ts --fault-flags-->  Redis Stream "raw-events"
                                      |
                              processor.ts (consumer group "ingest-worker")
                                      |
                         [received] -> [enriched] -> [written]
                                      |
                              PostgreSQL "sink_events" table
                                      |
                         Every stage transition is logged to
                         PostgreSQL "event_trace_log" table
                         (append-only, never mutated)

mcp-server.ts reads from sink_events + event_trace_log to answer tool calls.
It never touches the Redis stream directly (except pipeline_health, which
reports broker-level length/lag) — it queries derived state only.
```

## The core idea

`producer_log` is the **ground truth** (what the producer tried to do,
including which faults it deliberately applied). `event_trace_log` +
`sink_events` is what the **pipeline observed and did**. Every correctness
claim in this repo is a comparison of those two independently-written records —
never inferred from one side alone.

## Prerequisites

Docker + Node 22.

## Demo walkthrough (the 2-minute version)

```bash
# 1. Start everything. This must genuinely work from a clean clone.
docker compose up -d
npm install
npx tsx src/migrate.ts

# 2. Emit 1000 synthetic events with known faults: 5% dropped, 3% duplicated.
npx tsx src/producer.ts --events 1000 --drop 5 --duplicate 3

# 3. Run the ingestion worker (drains the stream, then exits).
npx tsx src/processor.ts --once
```

Now connect an MCP client to the server. With Claude Desktop, add this to
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "trace": {
      "command": "npx",
      "args": ["tsx", "/absolute/path/to/TRACE/src/mcp-server.ts"],
      "env": {
        "DATABASE_URL": "postgres://trace:trace@localhost:5432/trace",
        "REDIS_URL": "redis://localhost:6379"
      }
    }
  }
}
```

(Or point any MCP Inspector-style client at `npx tsx src/mcp-server.ts`, which
uses the stdio transport.)

Then ask:

> **"What got dropped?"**

The agent calls `find_dropped_events()` and gets back the exact set of dropped
event IDs, each marked `expected: true` (deliberate `--drop` fault) or
`expected: false` (genuinely lost — a real pipeline bug). Ask
**"Were there duplicates, and did the pipeline handle them?"** →
`find_duplicate_events()` shows each twice-sent event collapsed to exactly one
sink row. Ask **"What happened to event X?"** → `trace_event(X)` returns the
producer record, the ordered `received → enriched → written` stage history,
and a `final_status` of `written | dropped | in_flight | missing_unexpectedly`.
Ask **"Is the pipeline healthy?"** → `pipeline_health()` reports stream
length, consumer lag, throughput, and error rate.

## Fault injection

```bash
npx tsx src/producer.ts --events 1000 --drop 5 --duplicate 3 --delay 2 --delay-ms 4000
```

| Flag | Effect |
|---|---|
| `--drop <pct>` | Recorded in `producer_log` as `dropped_before_send`, never sent. Should never appear downstream. |
| `--duplicate <pct>` | Sent to the stream **twice** with the same `event_id`, marked `duplicated`. Should collapse to one sink row. |
| `--delay <pct> --delay-ms <n>` | Held `n` ms before sending, marked `delayed`. `received` timestamp measurably later than `emitted_at`. |

## MCP tools

| Tool | What it answers |
|---|---|
| `trace_event(event_id)` | Full history of one event + `final_status` |
| `find_dropped_events(since?, until?)` | Events that never reached the sink; `expected` distinguishes deliberate faults from real loss |
| `find_duplicate_events(since?, until?)` | Twice-sent events + whether they collapsed correctly |
| `pipeline_health()` | Stream length, consumer lag, 5-min throughput, 5-min error rate |

## Validation

```bash
npm test   # Vitest: unit + property-based (fast-check) + MCP protocol integration
```

- **Ground-truth reconciliation** (`tests/reconciliation.test.ts`): with
  `--events 1000 --drop 5 --duplicate 3`, the tool-reported drop/duplicate
  **sets** are compared element-for-element against `producer_log`. Zero false
  positives, zero false negatives — or nothing else matters.
- **Property-based** (`tests/properties.test.ts`): no silent loss, no phantom
  success, duplicate collapse, trace ordering (+ a pure model check on fault
  planning across 100 seeds).
- **MCP protocol integration** (`tests/mcp-integration.test.ts`): spins up the
  real server process, connects a real client over stdio, calls all four tools.
- **Load sanity** (`tests/load.test.ts`): health numbers track an independent
  Redis `XLEN` measurement at increasing volumes.
- **Delay ground truth** (`tests/delay.test.ts`): delayed events' `received`
  timestamps lag `emitted_at` by the full hold.

## Project structure

```
docker-compose.yml          Redis + Postgres, one-command up
src/
  schema.sql                producer_log / event_trace_log / sink_events
  db.ts                     pg pool + shared constants
  migrate.ts                applies schema.sql
  producer.ts               synthetic producer CLI with --drop/--duplicate/--delay
  processor.ts              consumer-group worker: received → enriched → written
  tools.ts                  the four tool implementations (Postgres only)
  mcp-server.ts             stdio MCP server exposing the tools
tests/
  producer.unit.test.ts     arg parsing + fault planning model
  reconciliation.test.ts    §6.1 exact-set ground-truth proof (1000 events)
  properties.test.ts        §6.2 fast-check invariants
  mcp-integration.test.ts   §6.3 real server + real client over stdio
  load.test.ts              §6.4 health vs independent XLEN
  delay.test.ts             delayed-fault timestamp ground truth
```

## Known spec deviation

The spec's DDL declares `producer_log.event_id` as `PRIMARY KEY` **and** its
fault table describes a *second* `producer_log` row for duplicates — these two
can't both hold. The DDL wins: the producer upserts a single row per event
(`fault_applied='duplicated'`, `fault_meta.duplicate_of` = self). All
ground-truth queries key on `fault_applied`, never on row counts.

## Non-scope

No web UI, no multi-provider adapters, no real Kafka, no real ClickHouse, no
auth/multi-tenancy, no rules engine. Small, tightly-scoped proof of one
capability.
