import { createWriteStream, existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { computeDiff } from "../src/diff/engine.js";
import { formatBytes, formatNumber } from "../src/utils/formatting.js";

async function runBenchmark() {
  const benchDir = join(tmpdir(), `rowpipe-diff-bench-${Date.now()}`);
  mkdirSync(benchDir, { recursive: true });

  const leftFile = join(benchDir, "bench_left.csv");
  const rightFile = join(benchDir, "bench_right.csv");

  const totalRows = 1_000_000;
  const changedRows = 100_000;
  const addedRows = 5_000;
  const removedRows = 5_000;

  console.log(`\n======================================================`);
  console.log(`  Rowpipe Diff Benchmark (1,000,000 rows, 64MB memory limit)`);
  console.log(`======================================================\n`);
  console.log(`Generating synthetic datasets...`);

  // Stream generate left file
  const leftWs = createWriteStream(leftFile, "utf-8");
  leftWs.write("id,sku,category,price,quantity,status,updated_at\n");
  for (let i = 1; i <= totalRows; i++) {
    leftWs.write(
      `${i},SKU-${i},Category-${i % 20},${(i * 1.5).toFixed(2)},${i % 100},active,2026-01-01\n`
    );
  }
  await new Promise<void>((resolve) => leftWs.end(() => resolve()));

  // Stream generate right file
  const rightWs = createWriteStream(rightFile, "utf-8");
  rightWs.write("id,sku,category,price,quantity,status,updated_at\n");
  // 1..(totalRows - removedRows)
  for (let i = 1; i <= totalRows - removedRows; i++) {
    if (i <= changedRows) {
      // Changed price and quantity
      rightWs.write(
        `${i},SKU-${i},Category-${i % 20},${(i * 1.5 + 5).toFixed(2)},${(i % 100) + 1},active,2026-09-16\n`
      );
    } else {
      // Unchanged
      rightWs.write(
        `${i},SKU-${i},Category-${i % 20},${(i * 1.5).toFixed(2)},${i % 100},active,2026-01-01\n`
      );
    }
  }
  // Added rows
  for (let i = totalRows + 1; i <= totalRows + addedRows; i++) {
    rightWs.write(
      `${i},SKU-${i},Category-${i % 20},${(i * 1.5).toFixed(2)},${i % 100},new,2026-09-16\n`
    );
  }
  await new Promise<void>((resolve) => rightWs.end(() => resolve()));

  console.log(`Datasets generated. Starting streaming diff...\n`);

  const startTime = Date.now();
  let peakRss = process.memoryUsage().rss;

  const rssInterval = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > peakRss) peakRss = rss;
  }, 50);

  const summary = await computeDiff({
    left: leftFile,
    right: rightFile,
    keys: ["id"],
    memoryLimitBytes: 64 * 1024 * 1024, // 64 MB limit to force disk spilling
  });

  clearInterval(rssInterval);
  const totalDurationSec = (Date.now() - startTime) / 1000;
  const rowsPerSec = Math.round((summary.rows.totalLeft + summary.rows.totalRight) / totalDurationSec);

  // Clean up
  rmSync(benchDir, { recursive: true, force: true });

  console.log(`------------------------------------------------------`);
  console.log(`  Diff Results Summary`);
  console.log(`------------------------------------------------------`);
  console.log(`  Total Left Rows:      ${formatNumber(summary.rows.totalLeft)}`);
  console.log(`  Total Right Rows:     ${formatNumber(summary.rows.totalRight)}`);
  console.log(`  Added Rows:           ${formatNumber(summary.rows.added)}`);
  console.log(`  Removed Rows:         ${formatNumber(summary.rows.removed)}`);
  console.log(`  Changed Rows:         ${formatNumber(summary.rows.changed)}`);
  console.log(`  Unchanged Rows:       ${formatNumber(summary.rows.unchanged)}`);
  console.log(`------------------------------------------------------`);
  console.log(`  Total Time:           ${totalDurationSec.toFixed(2)}s`);
  console.log(`  Throughput:           ${formatNumber(rowsPerSec)} rows/s`);
  console.log(`  Peak RSS Memory:      ${formatBytes(peakRss)}`);
  console.log(`  Spill-to-Disk Active: ${summary.spillStats?.isSpilled ? "YES (" + formatBytes(summary.spillStats.spilledBytes) + ")" : "NO"}`);
  console.log(`======================================================\n`);
}

runBenchmark().catch((err) => {
  console.error("Benchmark error:", err);
  process.exit(1);
});
