import { describe, it, expect } from "vitest";
import { DatasetProfiler, formatProfileMarkdown, formatProfileTerminal } from "../src/analytics/profiler.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row } from "../src/core/types.js";

describe("Data Profiler Test Suite", () => {
  async function* makeStream(rows: Row[], batchSize = 2): AsyncIterable<DataBatch> {
    for (let i = 0; i < rows.length; i += batchSize) {
      yield {
        rows: rows.slice(i, i + batchSize),
        offset: i,
      };
    }
  }

  const sampleData: Row[] = [
    { id: 1, name: "Alice", email: "alice@example.com", age: 25, revenue: 100.5, city: "Istanbul" },
    { id: 2, name: "Bob", email: "bob@example.com", age: 30, revenue: 200.0, city: "Ankara" },
    { id: 3, name: "Charlie", email: "invalid-email", age: null, revenue: 0, city: "Istanbul" },
    { id: 4, name: "David", email: "david@example.com", age: 40, revenue: null, city: "Izmir" },
    { id: 5, name: "Eve", email: "eve@example.com", age: 25, revenue: -50.0, city: "Istanbul" },
  ];

  it("should profile dataset columns and compute null and distinct metrics", async () => {
    const profiler = new DatasetProfiler();
    const pipeline = createPipeline(makeStream(sampleData));
    const result = await pipeline.reduce(profiler);

    expect(result.totalRows).toBe(5);
    expect(result.totalColumns).toBe(6);

    const cityCol = result.columns.find((c) => c.name === "city");
    expect(cityCol).toBeDefined();
    expect(cityCol!.inferredType).toBe("string");
    expect(cityCol!.nullCount).toBe(0);
    expect(cityCol!.topValues?.[0]?.value).toBe("Istanbul");
    expect(cityCol!.topValues?.[0]?.count).toBe(3);

    const ageCol = result.columns.find((c) => c.name === "age");
    expect(ageCol).toBeDefined();
    expect(ageCol!.inferredType).toBe("integer");
    expect(ageCol!.nullCount).toBe(1);
    expect(ageCol!.nullPercentage).toBe(20);
    expect(ageCol!.min).toBe(25);
    expect(ageCol!.max).toBe(40);
  });

  it("should detect semantic types like email", async () => {
    const profiler = new DatasetProfiler();
    const pipeline = createPipeline(makeStream(sampleData));
    const result = await pipeline.reduce(profiler);

    const emailCol = result.columns.find((c) => c.name === "email");
    expect(emailCol).toBeDefined();
    expect(emailCol!.semanticType).toBe("email");
  });

  it("should format terminal and markdown profile outputs cleanly", async () => {
    const profiler = new DatasetProfiler();
    const pipeline = createPipeline(makeStream(sampleData));
    const result = await pipeline.reduce(profiler);

    const terminalOutput = formatProfileTerminal(result);
    expect(terminalOutput).toContain("Rowpipe Dataset Profile");
    expect(terminalOutput).toContain("Istanbul");

    const markdownOutput = formatProfileMarkdown(result);
    expect(markdownOutput).toContain("# Dataset Profile Report");
    expect(markdownOutput).toContain("| **revenue** |");
  });
});
