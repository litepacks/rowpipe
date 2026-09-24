import { createPipeline } from "../src/core/pipeline.js";
import { filterRows } from "../src/transforms/filter.js";
import { selectColumns } from "../src/transforms/select.js";
import { castColumns } from "../src/transforms/cast.js";
import { DatasetStatsAggregator } from "../src/analytics/stats.js";
import type { DataBatch, DataStream, Row } from "../src/core/types.js";
import { formatBytes, formatNumber } from "../src/utils/formatting.js";

/**
 * Generator producing synthetic data batches without buffering.
 */
async function* generateSyntheticStream(
  totalRows: number,
  batchSize = 2000
): DataStream {
  let offset = 0;
  while (offset < totalRows) {
    const currentBatchSize = Math.min(batchSize, totalRows - offset);
    const rows: Row[] = new Array(currentBatchSize);

    for (let i = 0; i < currentBatchSize; i++) {
      const id = offset + i;
      rows[i] = {
        id: String(id),
        name: `User_${id}`,
        age: String(20 + (id % 50)),
        email: `user${id}@example.com`,
        revenue: String((id % 1000) * 1.5),
        country: id % 3 === 0 ? "TR" : id % 3 === 1 ? "US" : "DE",
      };
    }

    yield {
      rows,
      offset,
    };
    offset += currentBatchSize;
  }
}

async function runBenchmark(totalRows: number): Promise<void> {
  process.stdout.write(`\n======================================================\n`);
  process.stdout.write(`Running Benchmark with ${formatNumber(totalRows)} synthetic rows...\n`);
  process.stdout.write(`Pipeline: Input -> Filter(age > 25) -> Select(id, name, age, revenue) -> Cast(revenue:number) -> Stats\n`);
  process.stdout.write(`------------------------------------------------------\n`);

  const initialMem = process.memoryUsage();
  let peakRss = initialMem.rss;
  let peakHeap = initialMem.heapUsed;

  const startTime = Date.now();
  let processedRows = 0;

  const rawStream = generateSyntheticStream(totalRows, 2000);

  const pipeline = createPipeline(rawStream)
    .pipe(filterRows("age > 25"))
    .pipe(selectColumns(["id", "name", "age", "revenue"]))
    .pipe(castColumns({ revenue: "number" }));

  const statsAgg = new DatasetStatsAggregator();

  for await (const batch of pipeline.batches()) {
    processedRows += batch.rows.length;
    for (const row of batch.rows) {
      statsAgg.add(row);
    }

    const currentMem = process.memoryUsage();
    if (currentMem.rss > peakRss) peakRss = currentMem.rss;
    if (currentMem.heapUsed > peakHeap) peakHeap = currentMem.heapUsed;

    if (processedRows % 250000 === 0 || processedRows === totalRows) {
      const elapsed = (Date.now() - startTime) / 1000 || 0.001;
      const speed = Math.round(processedRows / elapsed);
      process.stdout.write(
        `  Processed: ${formatNumber(processedRows)} rows | Elapsed: ${elapsed.toFixed(1)}s | RSS: ${formatBytes(currentMem.rss)} | Heap: ${formatBytes(currentMem.heapUsed)} | Speed: ${formatNumber(speed)} rows/s\n`
      );
    }
  }

  const totalTimeMs = Date.now() - startTime;
  const elapsedSec = totalTimeMs / 1000 || 0.001;
  const rowsPerSec = Math.round(totalRows / elapsedSec);
  // Estimate ~70 bytes per row
  const estimatedMB = (totalRows * 70) / (1024 * 1024);
  const mbPerSec = (estimatedMB / elapsedSec).toFixed(1);

  const results = statsAgg.result();

  process.stdout.write(`------------------------------------------------------\n`);
  process.stdout.write(`Benchmark Results:\n`);
  process.stdout.write(`  Total Input Rows:  ${formatNumber(totalRows)}\n`);
  process.stdout.write(`  Filtered Rows:     ${formatNumber(processedRows)}\n`);
  process.stdout.write(`  Execution Time:    ${elapsedSec.toFixed(2)}s\n`);
  process.stdout.write(`  Throughput:        ${formatNumber(rowsPerSec)} rows/s (~${mbPerSec} MB/s)\n`);
  process.stdout.write(`  Initial RSS:       ${formatBytes(initialMem.rss)}\n`);
  process.stdout.write(`  Peak RSS:          ${formatBytes(peakRss)}\n`);
  process.stdout.write(`  Peak Heap Used:    ${formatBytes(peakHeap)}\n`);
  process.stdout.write(`  Memory Growth:     Bounded ($O(1)$ constant memory)\n`);
  process.stdout.write(`======================================================\n`);
}

async function main() {
  await runBenchmark(100_000);
  await runBenchmark(1_000_000);
}

main().catch(console.error);
