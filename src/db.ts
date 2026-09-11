import { Pool } from "pg";

let pool: Pool | null = null;

export function getPool(): Pool {
  if (!pool) {
    const connectionString =
      process.env.DATABASE_URL ??
      "postgres://trace:trace@localhost:5432/trace";
    pool = new Pool({ connectionString });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

export const STREAM_NAME = process.env.STREAM_NAME ?? "raw-events";
export const CONSUMER_GROUP = process.env.CONSUMER_GROUP ?? "ingest-worker";

export type Stage = "received" | "enriched" | "written";
export type FaultApplied =
  | "none"
  | "dropped_before_send"
  | "duplicated"
  | "delayed";
