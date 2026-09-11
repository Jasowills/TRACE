import { describe, it, expect } from "vitest";
import { parseArgs, planFaults, mulberry32 } from "../src/producer.js";

describe("parseArgs", () => {
  it("parses the spec demo flags", () => {
    const opts = parseArgs([
      "--events", "1000",
      "--drop", "5",
      "--duplicate", "3",
    ]);
    expect(opts.events).toBe(1000);
    expect(opts.drop).toBe(5);
    expect(opts.duplicate).toBe(3);
  });

  it("parses delay flags with defaults", () => {
    const opts = parseArgs(["--events", "10", "--delay", "2", "--delay-ms", "4000"]);
    expect(opts.delay).toBe(2);
    expect(opts.delayMs).toBe(4000);
    const def = parseArgs([]);
    expect(def.events).toBe(100);
    expect(def.delayMs).toBe(4000);
  });
});

describe("planFaults", () => {
  it("assigns exact floor(pct) counts per fault class, mutually exclusive", () => {
    const faults = planFaults(1000, 5, 3, 2, mulberry32(42));
    expect(faults.filter((f) => f === "dropped_before_send")).toHaveLength(50);
    expect(faults.filter((f) => f === "duplicated")).toHaveLength(30);
    expect(faults.filter((f) => f === "delayed")).toHaveLength(20);
    expect(faults.filter((f) => f === "none")).toHaveLength(900);
  });

  it("is deterministic for the same seed", () => {
    const a = planFaults(100, 5, 3, 2, mulberry32(7));
    const b = planFaults(100, 5, 3, 2, mulberry32(7));
    expect(a).toEqual(b);
  });

  it("caps total faults at N when percentages overlap", () => {
    const faults = planFaults(10, 50, 50, 50, mulberry32(1));
    expect(faults).toHaveLength(10);
    // 5 drop + 5 dup fill all 10; delay gets nothing (no overlap allowed)
    expect(faults.filter((f) => f === "dropped_before_send")).toHaveLength(5);
    expect(faults.filter((f) => f === "duplicated")).toHaveLength(5);
  });

  it("assigns no faults at 0%", () => {
    expect(planFaults(100, 0, 0, 0).every((f) => f === "none")).toBe(true);
  });
});
