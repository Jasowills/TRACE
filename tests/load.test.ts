/**
 * §6.4 Load sanity check.
 * Produces at increasing rates and confirms pipeline_health() tracks an
 * independent measurement (separate Redis connection XLEN + wall clock).
 */
import { describe, it, expect, beforeAll } from "vitest";
import { Redis } from "ioredis";
import { pipelineHealth } from "../src/tools.js";
import { STREAM_NAME } from "../src/db.js";
import { migrate, resetAll, runPipeline } from "./helpers.js";

describe("load sanity (§6.4)", () => {
  beforeAll(async () => {
    await migrate();
    await resetAll();
  });

  it("health numbers track independent measurements at increasing volumes", async () => {
    const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
    const probe = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
    try {
      for (const n of [100, 500]) {
        const t0 = Date.now();
        await runPipeline({ events: n, drop: 0, duplicate: 0, delay: 0, delayMs: 0 });
        const wallMs = Date.now() - t0;

        const independentLen = await probe.xlen(STREAM_NAME);
        const health = await pipelineHealth();

        // stream_length matches XLEN exactly (same broker, independent client).
        expect(health.stream_length).toBe(independentLen);
        // Consumer drained everything it was given.
        expect(health.consumer_lag).toBe(0);
        // Wall-clock throughput is sane (>10 ev/s on local docker).
        const wallThroughput = (n / wallMs) * 1000;
        expect(wallThroughput).toBeGreaterThan(10);
        // No errors on a clean run.
        expect(health.error_rate_5min).toBe(0);
      }
    } finally {
      probe.disconnect();
    }
  }, 180_000);
});
