import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createPipeline } from "../src/core/pipeline.js";
import { limitRows } from "../src/transforms/limit.js";
import { offsetRows } from "../src/transforms/offset.js";
import { tailRows } from "../src/transforms/tail.js";
import { topRows } from "../src/transforms/top.js";
import { sortRows, parseSortSpecs } from "../src/transforms/sort/index.js";
import { uniqueRows } from "../src/transforms/unique.js";
import { groupRows } from "../src/transforms/group.js";
import { countStream } from "../src/transforms/count.js";
import { optimizePipeline, formatExecutionPlan } from "../src/planner/index.js";
import type { DataBatch, DataStream, Row } from "../src/core/types.js";

const TEST_DIR = join(process.cwd(), "scratch_test_ops");

async function* createMockStream(rows: Row[], batchSize = 3): DataStream {
  for (let i = 0; i < rows.length; i += batchSize) {
    yield {
      rows: rows.slice(i, i + batchSize),
      offset: i,
    };
  }
}

describe("Rowpipe 2.0 Operations & Planner Test Suite", () => {
  beforeEach(() => {
    if (!existsSync(TEST_DIR)) mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  // 1. Limit & Offset
  describe("Limit & Offset Transforms", () => {
    const sampleRows: Row[] = Array.from({ length: 20 }, (_, i) => ({
      id: i + 1,
      name: `User_${i + 1}`,
    }));

    it("should limit stream to exact count with early cancellation", async () => {
      let batchesGenerated = 0;
      async function* trackedStream(): DataStream {
        for (let i = 0; i < 20; i += 3) {
          batchesGenerated++;
          yield { rows: sampleRows.slice(i, i + 3), offset: i };
        }
      }

      const pipeline = createPipeline(trackedStream()).pipe(limitRows(5));
      const results = await pipeline.toArray();

      expect(results).toHaveLength(5);
      expect(results.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
      // Should not have generated all 7 batches
      expect(batchesGenerated).toBeLessThanOrEqual(2);
    });

    it("should skip offset rows correctly", async () => {
      const pipeline = createPipeline(createMockStream(sampleRows)).pipe(offsetRows(15));
      const results = await pipeline.toArray();

      expect(results).toHaveLength(5);
      expect(results.map((r) => r.id)).toEqual([16, 17, 18, 19, 20]);
    });

    it("should combine offset and limit correctly", async () => {
      const pipeline = createPipeline(createMockStream(sampleRows))
        .pipe(offsetRows(5))
        .pipe(limitRows(3));

      const results = await pipeline.toArray();
      expect(results).toHaveLength(3);
      expect(results.map((r) => r.id)).toEqual([6, 7, 8]);
    });
  });

  // 2. Tail Transform
  describe("Tail Transform (Bounded Ring Buffer)", () => {
    const sampleRows: Row[] = Array.from({ length: 50 }, (_, i) => ({
      id: i + 1,
      name: `User_${i + 1}`,
    }));

    it("should emit exact last N rows using ring buffer", async () => {
      const pipeline = createPipeline(createMockStream(sampleRows)).pipe(tailRows(5));
      const results = await pipeline.toArray();

      expect(results).toHaveLength(5);
      expect(results.map((r) => r.id)).toEqual([46, 47, 48, 49, 50]);
    });

    it("should handle tail count greater than total stream rows", async () => {
      const small = sampleRows.slice(0, 3);
      const pipeline = createPipeline(createMockStream(small)).pipe(tailRows(10));
      const results = await pipeline.toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => r.id)).toEqual([1, 2, 3]);
    });
  });

  // 3. Top-K Heap
  describe("Top-K Heap Transform", () => {
    const dataset: Row[] = [
      { name: "Alice", score: 85, revenue: 1200 },
      { name: "Bob", score: 92, revenue: 800 },
      { name: "Charlie", score: 78, revenue: 2500 },
      { name: "David", score: 95, revenue: 1500 },
      { name: "Eve", score: 88, revenue: 3100 },
    ];

    it("should extract top 3 largest by revenue", async () => {
      const pipeline = createPipeline(createMockStream(dataset)).pipe(
        topRows({ by: "revenue", count: 3 })
      );
      const results = await pipeline.toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => r.name)).toEqual(["Eve", "Charlie", "David"]);
      expect(results.map((r) => r.revenue)).toEqual([3100, 2500, 1500]);
    });

    it("should extract bottom 2 smallest by score", async () => {
      const pipeline = createPipeline(createMockStream(dataset)).pipe(
        topRows({ by: "score", count: 2, smallest: true })
      );
      const results = await pipeline.toArray();

      expect(results).toHaveLength(2);
      expect(results.map((r) => r.name)).toEqual(["Charlie", "Alice"]);
    });
  });

  // 4. Sort & External Merge Sort
  describe("Sort & External Merge Sort", () => {
    const products: Row[] = [
      { id: 1, name: "item10", price: 100, country: "TR", created: new Date("2026-01-01") },
      { id: 2, name: "item2", price: 20, country: "US", created: new Date("2026-03-01") },
      { id: 3, name: "item1", price: 50, country: "TR", created: new Date("2026-02-01") },
      { id: 4, name: "item20", price: 20, country: "DE", created: new Date("2026-01-15") },
      { id: 5, name: "item3", price: 100, country: "US", created: new Date("2026-02-20") },
    ];

    it("should sort numerically (not lexicographically)", async () => {
      const numbers: Row[] = [{ val: "10" }, { val: "2" }, { val: "1" }, { val: "20" }];
      const pipeline = createPipeline(createMockStream(numbers)).pipe(sortRows({ by: "val" }));
      const results = await pipeline.toArray();

      expect(results.map((r) => r.val)).toEqual(["1", "2", "10", "20"]);
    });

    it("should sort multi-column (country ASC, price DESC)", async () => {
      const pipeline = createPipeline(createMockStream(products)).pipe(
        sortRows({ by: "country,price:desc" })
      );
      const results = await pipeline.toArray();

      expect(results.map((r) => `${r.country}:${r.price}`)).toEqual([
        "DE:20",
        "TR:100",
        "TR:50",
        "US:100",
        "US:20",
      ]);
    });

    it("should support natural sorting on filenames/identifiers", async () => {
      const files: Row[] = [
        { file: "file10.txt" },
        { file: "file1.txt" },
        { file: "file2.txt" },
      ];
      const pipeline = createPipeline(createMockStream(files)).pipe(
        sortRows({ by: "file", natural: true })
      );
      const results = await pipeline.toArray();

      expect(results.map((r) => r.file)).toEqual(["file1.txt", "file2.txt", "file10.txt"]);
    });

    it("should handle nulls first and nulls last", async () => {
      const rowsWithNulls: Row[] = [
        { id: 1, score: 50 },
        { id: 2, score: null },
        { id: 3, score: 90 },
      ];

      const lastPipeline = createPipeline(createMockStream(rowsWithNulls)).pipe(
        sortRows({ by: "score", nulls: "last" })
      );
      const lastRes = await lastPipeline.toArray();
      expect(lastRes.map((r) => r.score)).toEqual([50, 90, null]);

      const firstPipeline = createPipeline(createMockStream(rowsWithNulls)).pipe(
        sortRows({ by: "score", nulls: "first" })
      );
      const firstRes = await firstPipeline.toArray();
      expect(firstRes.map((r) => r.score)).toEqual([null, 50, 90]);
    });

    it("should activate disk spilling when memory threshold is low", async () => {
      // Generate 2,000 rows
      const largeRows: Row[] = Array.from({ length: 2000 }, (_, i) => ({
        id: 2000 - i,
        val: (i * 37) % 500,
        text: `Data_Row_${i}`,
      }));

      // Set extremely low memory threshold (1KB) to force multiple disk spills
      const pipeline = createPipeline(createMockStream(largeRows, 100)).pipe(
        sortRows({
          by: "val,id",
          memoryLimit: 1024, // 1 KB
          tempDir: TEST_DIR,
        })
      );

      const sorted = await pipeline.toArray(5000);
      expect(sorted).toHaveLength(2000);

      // Verify strict sorted order
      for (let i = 1; i < sorted.length; i++) {
        const prev = sorted[i - 1]!;
        const curr = sorted[i]!;
        expect(Number(prev.val)).toBeLessThanOrEqual(Number(curr.val));
        if (prev.val === curr.val) {
          expect(Number(prev.id)).toBeLessThanOrEqual(Number(curr.id));
        }
      }
    });
  });

  // 5. Unique & Deduplication
  describe("Unique Transform", () => {
    const dataset: Row[] = [
      { email: "alice@test.com", version: 1, role: "User" },
      { email: "bob@test.com", version: 1, role: "Admin" },
      { email: "alice@test.com", version: 2, role: "Manager" },
      { email: "charlie@test.com", version: 1, role: "User" },
      { email: "bob@test.com", version: 2, role: "SuperAdmin" },
    ];

    it("should deduplicate by key column keeping first occurrence", async () => {
      const pipeline = createPipeline(createMockStream(dataset)).pipe(
        uniqueRows({ by: "email", keep: "first" })
      );
      const results = await pipeline.toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => `${r.email}:v${r.version}`)).toEqual([
        "alice@test.com:v1",
        "bob@test.com:v1",
        "charlie@test.com:v1",
      ]);
    });

    it("should deduplicate keeping last occurrence", async () => {
      const pipeline = createPipeline(createMockStream(dataset)).pipe(
        uniqueRows({ by: "email", keep: "last" })
      );
      const results = await pipeline.toArray();

      expect(results).toHaveLength(3);
      expect(results.map((r) => `${r.email}:v${r.version}`)).toEqual([
        "alice@test.com:v2",
        "bob@test.com:v2",
        "charlie@test.com:v1",
      ]);
    });
  });

  // 6. Group & Aggregations
  describe("Group & Aggregations Transform", () => {
    const sales: Row[] = [
      { country: "TR", revenue: 100, cost: 40 },
      { country: "US", revenue: 200, cost: 90 },
      { country: "TR", revenue: 300, cost: 100 },
      { country: "US", revenue: 150, cost: 50 },
      { country: "DE", revenue: 400, cost: 120 },
    ];

    it("should group by country and compute count, sum, avg, min, max", async () => {
      const pipeline = createPipeline(createMockStream(sales)).pipe(
        groupRows({
          by: "country",
          count: true,
          sum: "revenue",
          avg: "revenue",
          max: "revenue",
        })
      );

      const results = await pipeline.toArray();
      expect(results).toHaveLength(3);

      const tr = results.find((r) => r.country === "TR")!;
      expect(tr.count).toBe(2);
      expect(tr.revenue_sum).toBe(400);
      expect(tr.revenue_avg).toBe(200);
      expect(tr.revenue_max).toBe(300);

      const de = results.find((r) => r.country === "DE")!;
      expect(de.count).toBe(1);
      expect(de.revenue_sum).toBe(400);
    });

    it("should support compact aggregation strings e.g. --agg 'count(),sum(revenue)'", async () => {
      const pipeline = createPipeline(createMockStream(sales)).pipe(
        groupRows({
          by: "country",
          agg: "count(),sum(revenue),max(cost)",
        })
      );

      const results = await pipeline.toArray();
      const us = results.find((r) => r.country === "US")!;
      expect(us.count).toBe(2);
      expect(us.revenue_sum).toBe(350);
      expect(us.cost_max).toBe(90);
    });
  });

  // 7. Count Stream Analysis
  describe("Count Stream Analyzer", () => {
    const users: Row[] = [
      { id: 1, country: "TR", email: "a@test.com" },
      { id: 2, country: "US", email: "b@test.com" },
      { id: 3, country: "TR", email: "c@test.com" },
      { id: 4, country: "TR", email: "a@test.com" },
    ];

    it("should count total rows", async () => {
      const res = await countStream(createMockStream(users));
      expect(res.totalRows).toBe(4);
    });

    it("should count distinct column values", async () => {
      const res = await countStream(createMockStream(users), { distinct: "email" });
      expect(res.distinctCount).toBe(3);
    });

    it("should group-count by column", async () => {
      const res = await countStream(createMockStream(users), { by: "country" });
      expect(res.groups).toBeDefined();
      expect(res.groups![0]).toEqual({ key: { country: "TR" }, count: 3 });
      expect(res.groups![1]).toEqual({ key: { country: "US" }, count: 1 });
    });
  });

  // 8. Pipeline Planner & Optimizer
  describe("Pipeline Planner & Optimizer", () => {
    it("should optimize Sort + Limit into TopK heap", () => {
      const plan = optimizePipeline([
        { type: "filter", expression: "age >= 18" },
        { type: "sort", specs: [{ column: "score", direction: "desc" }] },
        { type: "limit", count: 10 },
      ]);

      expect(plan.operations).toHaveLength(2);
      expect(plan.operations[0]).toEqual({ type: "filter", expression: "age >= 18" });
      expect(plan.operations[1]?.type).toBe("top");
      expect(plan.operations[1]).toMatchObject({
        type: "top",
        count: 10,
        order: "desc",
      });
      expect(plan.optimizationsApplied[0]).toContain("sort(score desc) + limit(10) -> top-k (10)");
      expect(plan.memoryClassification).toBe("bounded-state");
    });

    it("should compute effective read limit on offset + limit", () => {
      const plan = optimizePipeline([
        { type: "offset", count: 100 },
        { type: "limit", count: 50 },
      ]);

      expect(plan.effectiveReadLimit).toBe(150);
      expect(plan.optimizationsApplied[0]).toContain("offset(100) + limit -> upstream stream capped at 150 rows");
    });

    it("should format ASCII execution plan correctly", () => {
      const plan = optimizePipeline([
        { type: "filter", expression: "score > 80" },
        { type: "sort", specs: [{ column: "revenue", direction: "desc" }] },
        { type: "limit", count: 5 },
      ]);

      const ascii = formatExecutionPlan(plan, "CSVReader (sales.csv)", "JSONLWriter (stdout)");
      expect(ascii).toContain("Rowpipe Execution Plan");
      expect(ascii).toContain("TopK (revenue DESC, 5) [Bounded Heap]");
      expect(ascii).toContain("Bounded State (O(K)");
    });

    it("should produce identical results between sort+limit and optimized top-k", async () => {
      const dataset: Row[] = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        score: (i * 17) % 53,
      }));

      // Unoptimized sort + limit
      const sortPipeline = createPipeline(createMockStream(dataset))
        .pipe(sortRows({ by: "score:desc" }))
        .pipe(limitRows(10));
      const sortResults = await sortPipeline.toArray();

      // Optimized top-k
      const topPipeline = createPipeline(createMockStream(dataset)).pipe(
        topRows({ by: "score", count: 10 })
      );
      const topResults = await topPipeline.toArray();

      expect(topResults.map((r) => r.score)).toEqual(sortResults.map((r) => r.score));
    });
  });
});
