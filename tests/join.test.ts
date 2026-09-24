import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { joinStreams, joinRows } from "../src/transforms/join/join.js";
import { SpillableJoinIndex } from "../src/transforms/join/index-storage.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row } from "../src/core/types.js";

describe("Stream Join Engine Test Suite", () => {
  async function* makeStream(rows: Row[], batchSize = 2): AsyncIterable<DataBatch> {
    for (let i = 0; i < rows.length; i += batchSize) {
      yield {
        rows: rows.slice(i, i + batchSize),
        offset: i,
      };
    }
  }

  const users: Row[] = [
    { id: 1, name: "Alice", dept_id: "D10" },
    { id: 2, name: "Bob", dept_id: "D20" },
    { id: 3, name: "Charlie", dept_id: "D30" },
    { id: 4, name: "David", dept_id: "D99" }, // No matching dept
  ];

  const departments: Row[] = [
    { dept_id: "D10", dept_name: "Engineering", floor: 3 },
    { dept_id: "D20", dept_name: "Marketing", floor: 2 },
    { dept_id: "D40", dept_name: "Finance", floor: 1 }, // No matching user
  ];

  it("should perform INNER join", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "inner",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(2);
    expect(results[0]).toEqual({
      id: 1,
      name: "Alice",
      dept_id: "D10",
      dept_name: "Engineering",
      floor: 3,
    });
    expect(results[1]).toEqual({
      id: 2,
      name: "Bob",
      dept_id: "D20",
      dept_name: "Marketing",
      floor: 2,
    });
  });

  it("should perform LEFT join", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "left",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(4);
    // Matched
    expect(results[0]!.dept_name).toBe("Engineering");
    // Unmatched left row
    expect(results[2]!.name).toBe("Charlie");
    expect(results[2]!.dept_name).toBeNull();
    expect(results[3]!.name).toBe("David");
    expect(results[3]!.dept_name).toBeNull();
  });

  it("should perform RIGHT join", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "right",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(3);
    const d40 = results.find((r) => r.dept_id === "D40");
    expect(d40).toBeDefined();
    expect(d40!.dept_name).toBe("Finance");
    expect(d40!.name).toBeNull();
  });

  it("should perform FULL outer join", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "full",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    // 2 matched + 2 unmatched left + 1 unmatched right = 5 total
    expect(results.length).toBe(5);
  });

  it("should perform SEMI join (left semi join / exists)", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "semi",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(2);
    expect(results.map((r) => r.name)).toEqual(["Alice", "Bob"]);
  });

  it("should perform ANTI join (left anti join / not exists)", async () => {
    const joined = joinStreams(makeStream(users), makeStream(departments), {
      on: "dept_id",
      type: "anti",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(2);
    expect(results.map((r) => r.name)).toEqual(["Charlie", "David"]);
  });

  it("should handle column collisions with custom suffix or prefix", async () => {
    const leftData: Row[] = [{ id: 1, name: "Left Alice", status: "A" }];
    const rightData: Row[] = [{ id: 1, name: "Right Alice", status: "B" }];

    const joined = joinStreams(makeStream(leftData), makeStream(rightData), {
      on: "id",
      suffixRight: "_r",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(1);
    expect(results[0]).toEqual({
      id: 1,
      name: "Left Alice",
      status: "A",
      name_r: "Right Alice",
      status_r: "B",
    });
  });

  it("should support asymmetric keys (e.g. user_id=id)", async () => {
    const orders: Row[] = [{ order_id: 101, buyer_id: 1, total: 50 }];
    const customers: Row[] = [{ id: 1, customer_name: "Alice" }];

    const joined = joinStreams(makeStream(orders), makeStream(customers), {
      on: "buyer_id=id",
    });

    const pipeline = createPipeline(joined);
    const results = await pipeline.toArray();

    expect(results.length).toBe(1);
    expect(results[0]).toEqual({
      order_id: 101,
      buyer_id: 1,
      total: 50,
      customer_name: "Alice",
    });
  });

  it("should spill to disk when right table exceeds memory limit", async () => {
    const index = new SpillableJoinIndex({ memoryLimit: "1kb" });

    // Insert 500 rows to force disk spill
    for (let i = 0; i < 500; i++) {
      await index.set(`k_${i}`, { id: i, payload: `test_payload_${i}_` + "X".repeat(50) });
    }

    expect(index.count()).toBe(500);

    const hit = await index.get("k_250");
    expect(hit).toBeDefined();
    expect(hit![0]!.id).toBe(250);

    await index.close();
  });
});
