import { createReadStream, createWriteStream, existsSync, mkdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPipeline } from "../src/core/pipeline.js";
import { createReader } from "../src/readers/index.js";
import { createWriter } from "../src/writers/index.js";
import { limitRows } from "../src/transforms/limit.js";
import { tailRows } from "../src/transforms/tail.js";
import { topRows } from "../src/transforms/top.js";
import { sortRows } from "../src/transforms/sort/index.js";
import { groupRows } from "../src/transforms/group.js";
import { uniqueRows } from "../src/transforms/unique.js";
import { countStream } from "../src/transforms/count.js";
import { formatBytes, formatNumber } from "../src/utils/formatting.js";
import { Writable } from "node:stream";

class DevNullWritable extends Writable {
  _write(chunk: any, encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    callback();
  }
}

async function generateCsv(filePath: string, rowCount: number): Promise<number> {
  const ws = createWriteStream(filePath, "utf-8");
  ws.write("id,country,category,revenue,quantity,created_at\n");

  const countries = ["US", "TR", "DE", "GB", "FR", "JP", "CA", "AU", "NL", "BR"];
  const categories = ["electronics", "clothing", "home", "books", "sports", "automotive", "garden", "toys"];

  for (let i = 1; i <= rowCount; i++) {
    const country = countries[i % countries.length];
    const category = categories[i % categories.length];
    const revenue = ((i * 37) % 10000) + 0.99;
    const qty = (i % 50) + 1;
    ws.write(`${i},${country},${category},${revenue.toFixed(2)},${qty},2026-09-16T10:00:00Z\n`);
  }

  await new Promise<void>((resolve) => ws.end(() => resolve()));
  return statSync(filePath).size;
}

interface BenchResult {
  scenario: string;
  totalRows: number;
  processedRows: number;
  elapsedMs: number;
  rowsPerSec: number;
  mbPerSec: number;
  peakRssBytes: number;
  notes?: string;
}

async function runScenario(
  name: string,
  totalRows: number,
  fileSize: number,
  fn: () => Promise<number>,
  notes?: string
): Promise<BenchResult> {
  let peakRss = process.memoryUsage().rss;
  const interval = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > peakRss) peakRss = rss;
  }, 20);

  const start = Date.now();
  const processed = await fn();
  const elapsedMs = Math.max(1, Date.now() - start);
  clearInterval(interval);

  const rowsPerSec = Math.round((processed / elapsedMs) * 1000);
  const effectiveBytes = (fileSize * (processed / totalRows));
  const mbPerSec = (effectiveBytes / (1024 * 1024)) / (elapsedMs / 1000);

  return {
    scenario: name,
    totalRows,
    processedRows: processed,
    elapsedMs,
    rowsPerSec,
    mbPerSec,
    peakRssBytes: peakRss,
    notes,
  };
}

async function main() {
  const benchDir = join(tmpdir(), `rowpipe-ops-bench-${Date.now()}`);
  mkdirSync(benchDir, { recursive: true });

  const datasetRows = 1_000_000;
  const dataFile = join(benchDir, "bench_data.csv");

  console.log("\n=========================================================================");
  console.log("            Rowpipe 2.0 Tabular Operations Benchmark Suite               ");
  console.log("=========================================================================\n");
  console.log(`Generating test dataset: ${formatNumber(datasetRows)} rows...`);

  const fileSize = await generateCsv(dataFile, datasetRows);
  console.log(`Dataset generated: ${formatBytes(fileSize)} (${dataFile})\n`);

  const results: BenchResult[] = [];

  // 1. Limit (100 rows out of 1M - instant cancellation)
  results.push(
    await runScenario("Limit 100 (Early Stream Cancellation)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 1000 });
      const pipeline = createPipeline(reader).pipe(limitRows(100));
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return count;
    }, "Cancels upstream reading after first batch")
  );

  // 2. Tail (last 100 rows out of 1M - bounded ring buffer)
  results.push(
    await runScenario("Tail 100 (Bounded RingBuffer)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      const pipeline = createPipeline(reader).pipe(tailRows(100));
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return datasetRows;
    }, "O(N) ring buffer space")
  );

  // 3. Top-K (Top 100 highest revenue - bounded min-heap)
  results.push(
    await runScenario("Top-100 by Revenue (Bounded Min-Heap)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      const pipeline = createPipeline(reader).pipe(topRows({ by: "revenue", count: 100 }));
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return datasetRows;
    }, "O(K) heap space, single-pass streaming")
  );

  // 4. Count & Distinct
  results.push(
    await runScenario("Count + Distinct (Streaming HLL)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      await countStream(reader.read(), { distinct: "country", approx: true });
      return datasetRows;
    }, "O(1) memory cardinality estimation")
  );

  // 5. Group-by & Multi-Aggregations
  results.push(
    await runScenario("Group-by Country & Category (Sum/Avg/Min/Max)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      const pipeline = createPipeline(reader).pipe(
        groupRows({
          by: ["country", "category"],
          count: true,
          sum: "revenue",
          avg: "revenue",
          max: "quantity",
        })
      );
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return datasetRows;
    }, "Single-pass hash accumulator")
  );

  // 6. Unique Deduplication
  results.push(
    await runScenario("Unique Deduplication (by country,category)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      const pipeline = createPipeline(reader).pipe(
        uniqueRows({
          by: ["country", "category"],
          memoryLimit: "16mb",
          tempDir: benchDir,
        })
      );
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return datasetRows;
    }, "SpillableKeyStore with partition hash spill")
  );

  // 7. Full External Sort with Disk Spilling (16MB memory threshold on 1M rows)
  results.push(
    await runScenario("External Merge Sort (16MB memory limit, disk spill)", datasetRows, fileSize, async () => {
      const reader = createReader(createReadStream(dataFile), { format: "csv", batchSize: 5000 });
      const pipeline = createPipeline(reader).pipe(
        sortRows({
          by: "revenue:desc",
          memoryLimit: "16mb",
          tempDir: benchDir,
        })
      );
      let count = 0;
      for await (const batch of pipeline.batches()) {
        count += batch.rows.length;
      }
      return datasetRows;
    }, "Sorted runs spilled to disk, k-way min-heap merge")
  );

  // Print results table
  console.log("---------------------------------------------------------------------------------------------------------");
  console.log(
    "Scenario".padEnd(42) +
    "Rows/sec".padStart(14) +
    "Throughput".padStart(14) +
    "Elapsed".padStart(12) +
    "Peak RSS".padStart(12)
  );
  console.log("---------------------------------------------------------------------------------------------------------");

  for (const r of results) {
    console.log(
      r.scenario.padEnd(42) +
      formatNumber(r.rowsPerSec).padStart(14) +
      `${r.mbPerSec.toFixed(1)} MB/s`.padStart(14) +
      `${(r.elapsedMs / 1000).toFixed(2)}s`.padStart(12) +
      formatBytes(r.peakRssBytes).padStart(12)
    );
  }
  console.log("---------------------------------------------------------------------------------------------------------\n");

  // Cleanup
  if (existsSync(benchDir)) {
    rmSync(benchDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
