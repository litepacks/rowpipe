import { describe, it, expect } from "vitest";
import { cleanRows } from "../src/transforms/clean.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row } from "../src/core/types.js";

describe("Data Cleaning Transform Test Suite", () => {
  async function* makeStream(rows: Row[], batchSize = 2): AsyncIterable<DataBatch> {
    for (let i = 0; i < rows.length; i += batchSize) {
      yield {
        rows: rows.slice(i, i + batchSize),
        offset: i,
      };
    }
  }

  it("should trim whitespace from string fields", async () => {
    const data: Row[] = [
      { name: "  Alice  ", city: " Istanbul " },
      { name: "Bob\t", city: "Ankara\n" },
    ];

    const stream = cleanRows({ trim: true })(makeStream(data));
    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results).toEqual([
      { name: "Alice", city: "Istanbul" },
      { name: "Bob", city: "Ankara" },
    ]);
  });

  it("should map custom null representations to actual null", async () => {
    const data: Row[] = [
      { id: "1", revenue: "N/A", status: "none" },
      { id: "2", revenue: "-", status: "NULL" },
      { id: "3", revenue: "100", status: "active" },
    ];

    const stream = cleanRows({
      nullValues: ["N/A", "none", "-", "NULL"],
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ id: "1", revenue: null, status: null });
    expect(results[1]).toEqual({ id: "2", revenue: null, status: null });
    expect(results[2]).toEqual({ id: "3", revenue: "100", status: "active" });
  });

  it("should fill null values with defaults", async () => {
    const data: Row[] = [
      { id: 1, revenue: null, country: null },
      { id: 2, revenue: 50, country: "TR" },
    ];

    const stream = cleanRows({
      fillNulls: { revenue: 0, country: "Unknown" },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ id: 1, revenue: 0, country: "Unknown" });
    expect(results[1]).toEqual({ id: 2, revenue: 50, country: "TR" });
  });

  it("should coerce string values to numeric and boolean types", async () => {
    const data: Row[] = [
      { count: "42", active: "true", rating: "4.8" },
      { count: "0", active: "false", rating: "3.5" },
    ];

    const stream = cleanRows({ coerce: true })(makeStream(data));
    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ count: 42, active: true, rating: 4.8 });
    expect(results[1]).toEqual({ count: 0, active: false, rating: 3.5 });
  });

  it("should transform string casing", async () => {
    const data: Row[] = [
      { city: "iSTanbUL", country: "tr" },
    ];

    const stream = cleanRows({
      case: { city: "title", country: "upper" },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ city: "Istanbul", country: "TR" });
  });
});
