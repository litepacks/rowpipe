import { describe, it, expect } from "vitest";
import { windowRows, parseWindowSpecs } from "../src/transforms/window.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row } from "../src/core/types.js";

describe("Streaming Window Functions Test Suite", () => {
  async function* makeStream(rows: Row[], batchSize = 2): AsyncIterable<DataBatch> {
    for (let i = 0; i < rows.length; i += batchSize) {
      yield {
        rows: rows.slice(i, i + batchSize),
        offset: i,
      };
    }
  }

  it("should calculate row_number()", async () => {
    const data: Row[] = [{ name: "A" }, { name: "B" }, { name: "C" }];
    const stream = windowRows({
      specs: { rn: "row_number()" },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results).toEqual([
      { name: "A", rn: 1 },
      { name: "B", rn: 2 },
      { name: "C", rn: 3 },
    ]);
  });

  it("should calculate lag() with offset and default value", async () => {
    const data: Row[] = [
      { id: 1, val: 10 },
      { id: 2, val: 20 },
      { id: 3, val: 30 },
      { id: 4, val: 40 },
    ];

    const stream = windowRows({
      specs: {
        prev: "lag(val, 1, 0)",
        prev2: "lag(val, 2, -1)",
      },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results).toEqual([
      { id: 1, val: 10, prev: 0, prev2: -1 },
      { id: 2, val: 20, prev: 10, prev2: -1 },
      { id: 3, val: 30, prev: 20, prev2: 10 },
      { id: 4, val: 40, prev: 30, prev2: 20 },
    ]);
  });

  it("should calculate lead() using bounded lookahead buffer", async () => {
    const data: Row[] = [
      { id: 1, val: 10 },
      { id: 2, val: 20 },
      { id: 3, val: 30 },
      { id: 4, val: 40 },
    ];

    const stream = windowRows({
      specs: {
        next: "lead(val, 1, 0)",
        next2: "lead(val, 2, -1)",
      },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results).toEqual([
      { id: 1, val: 10, next: 20, next2: 30 },
      { id: 2, val: 20, next: 30, next2: 40 },
      { id: 3, val: 30, next: 40, next2: -1 },
      { id: 4, val: 40, next: 0, next2: -1 },
    ]);
  });

  it("should calculate running_sum(), running_avg(), running_min(), running_max()", async () => {
    const data: Row[] = [
      { x: 10 },
      { x: 20 },
      { x: 5 },
      { x: 30 },
    ];

    const stream = windowRows({
      specs: {
        cum_sum: "running_sum(x)",
        cum_avg: "running_avg(x)",
        min_so_far: "running_min(x)",
        max_so_far: "running_max(x)",
      },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ x: 10, cum_sum: 10, cum_avg: 10, min_so_far: 10, max_so_far: 10 });
    expect(results[1]).toEqual({ x: 20, cum_sum: 30, cum_avg: 15, min_so_far: 10, max_so_far: 20 });
    expect(results[2]).toEqual({ x: 5, cum_sum: 35, cum_avg: 11.666666666666666, min_so_far: 5, max_so_far: 20 });
    expect(results[3]).toEqual({ x: 30, cum_sum: 65, cum_avg: 16.25, min_so_far: 5, max_so_far: 30 });
  });

  it("should calculate moving_avg() and moving_sum() over sliding ring buffer", async () => {
    const data: Row[] = [
      { v: 10 },
      { v: 20 },
      { v: 30 },
      { v: 40 },
      { v: 50 },
    ];

    const stream = windowRows({
      specs: {
        roll_sum3: "moving_sum(v, 3)",
        roll_avg3: "moving_avg(v, 3)",
      },
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ v: 10, roll_sum3: 10, roll_avg3: 10 });
    expect(results[1]).toEqual({ v: 20, roll_sum3: 30, roll_avg3: 15 });
    expect(results[2]).toEqual({ v: 30, roll_sum3: 60, roll_avg3: 20 });
    // Window of 3 slides: [20, 30, 40]
    expect(results[3]).toEqual({ v: 40, roll_sum3: 90, roll_avg3: 30 });
    // Window of 3 slides: [30, 40, 50]
    expect(results[4]).toEqual({ v: 50, roll_sum3: 120, roll_avg3: 40 });
  });

  it("should partition calculations by column", async () => {
    const data: Row[] = [
      { dept: "IT", salary: 100 },
      { dept: "IT", salary: 150 },
      { dept: "HR", salary: 80 },
      { dept: "HR", salary: 90 },
      { dept: "IT", salary: 200 },
    ];

    const stream = windowRows({
      specs: {
        rn: "row_number()",
        dept_sum: "running_sum(salary)",
      },
      by: "dept",
    })(makeStream(data));

    const pipeline = createPipeline(stream);
    const results = await pipeline.toArray();

    expect(results[0]).toEqual({ dept: "IT", salary: 100, rn: 1, dept_sum: 100 });
    expect(results[1]).toEqual({ dept: "IT", salary: 150, rn: 2, dept_sum: 250 });
    expect(results[2]).toEqual({ dept: "HR", salary: 80, rn: 1, dept_sum: 80 });
    expect(results[3]).toEqual({ dept: "HR", salary: 90, rn: 2, dept_sum: 170 });
    expect(results[4]).toEqual({ dept: "IT", salary: 200, rn: 3, dept_sum: 450 });
  });
});
