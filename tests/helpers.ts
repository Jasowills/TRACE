import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Redis } from "ioredis";
import { getPool, STREAM_NAME } from "../src/db.js";
import { produce, type ProducerOptions } from "../src/producer.js";
import { runWorker } from "../src/processor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

export async function migrate(): Promise<void> {
  const sql = await readFile(join(__dirname, "..", "src", "schema.sql"), "utf8");
  await getPool().query(sql);
}

export async function resetDb(): Promise<void> {
  await getPool().query(
    "TRUNCATE producer_log, event_trace_log, sink_events;",
  );
}

export async function clearStream(): Promise<void> {
  const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", {
    maxRetriesPerRequest: 3,
  });
  await redis.del(STREAM_NAME);
  redis.disconnect();
}

export async function resetAll(): Promise<void> {
  await resetDb();
  await clearStream();
}

/** Produce N events with faults, then drain the stream with the worker. */
export async function runPipeline(opts: ProducerOptions): Promise<void> {
  await produce(opts);
  await runWorker({ once: true });
}
