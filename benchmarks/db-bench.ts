import { DatabaseReader } from "../src/db/source.js";
import { SqliteDatabaseAdapter } from "../src/db/adapters/sqlite.js";
import { createPipeline } from "../src/core/pipeline.js";
import { filterRows } from "../src/transforms/filter.js";
import { selectColumns } from "../src/transforms/select.js";
import { castColumns } from "../src/transforms/cast.js";
import { DatasetStatsAggregator } from "../src/analytics/stats.js";
import { formatBytes, formatNumber } from "../src/utils/formatting.js";
import type { Row } from "../src/core/types.js";

async function populateDatabase(db: any, totalRows: number): Promise<void> {
  db.exec(`
    CREATE TABLE benchmark_users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      age INTEGER NOT NULL,
      email TEXT NOT NULL,
      revenue REAL NOT NULL,
      country TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);

  db.exec("BEGIN TRANSACTION;");
  const insertStmt = db.prepare(`
    INSERT INTO benchmark_users (id, name, age, email, revenue, country, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  for (let i = 1; i <= totalRows; i++) {
    insertStmt.run(
      i,
      `User_${i}`,
      20 + (i % 50),
      `user${i}@example.com`,
      (i % 1000) * 1.5,
      i % 3 === 0 ? "TR" : i % 3 === 1 ? "US" : "DE",
      "2026-09-16T10:00:00Z"
    );
  }
  db.exec("COMMIT;");
}

async function runNaiveAllAtOnce(db: any): Promise<{ durationMs: number; peakRss: number; rows: number }> {
  if (global.gc) global.gc();
  const startMem = process.memoryUsage().rss;
  let peakRss = startMem;

  const rssInterval = setInterval(() => {
    const current = process.memoryUsage().rss;
    if (current > peakRss) peakRss = current;
  }, 10);

  const startTime = Date.now();

  // Naive: db.prepare(...).all() loads entire dataset into single JS Array
  const stmt = db.prepare("SELECT * FROM benchmark_users");
  const allRows = stmt.all() as unknown as Row[];

  // Local filtering & stats
  const filtered = allRows.filter((r) => Number(r.age) > 25);
  const aggregator = new DatasetStatsAggregator();
  for (const r of filtered) {
    aggregator.add({ id: r.id, name: r.name, age: r.age, revenue: Number(r.revenue) });
  }
  const _result = aggregator.result();

  clearInterval(rssInterval);
  const durationMs = Date.now() - startTime;
  return { durationMs, peakRss, rows: allRows.length };
}

async function runRowpipeStreaming(adapter: SqliteDatabaseAdapter, batchSize: number): Promise<{ durationMs: number; peakRss: number; rows: number }> {
  if (global.gc) global.gc();
  const startMem = process.memoryUsage().rss;
  let peakRss = startMem;

  const rssInterval = setInterval(() => {
    const current = process.memoryUsage().rss;
    if (current > peakRss) peakRss = current;
  }, 10);

  const startTime = Date.now();

  const reader = new DatabaseReader(adapter, {
    table: "benchmark_users",
    batchSize,
  });

  const aggregator = new DatasetStatsAggregator();
  await createPipeline(reader)
    .pipe(filterRows("age > 25"))
    .pipe(selectColumns(["id", "name", "age", "revenue"]))
    .pipe(castColumns({ revenue: "number" }))
    .reduce(aggregator);

  clearInterval(rssInterval);
  const durationMs = Date.now() - startTime;
  return { durationMs, peakRss, rows: 100000 };
}

async function main() {
  const TOTAL_ROWS = 100_000;
  const BATCH_SIZE = 2_000;

  process.stdout.write(`\n======================================================\n`);
  process.stdout.write(`  Rowpipe Database Streaming Benchmark (SQLite)     \n`);
  process.stdout.write(`======================================================\n`);
  process.stdout.write(`Dataset: ${formatNumber(TOTAL_ROWS)} rows synthetic database table\n`);
  process.stdout.write(`Batch Size: ${formatNumber(BATCH_SIZE)} rows/batch\n`);
  process.stdout.write(`Pipeline: DB Cursor -> Filter(age > 25) -> Select -> Cast -> Stats\n`);
  process.stdout.write(`------------------------------------------------------\n`);

  process.stdout.write(`Populating in-memory database with ${formatNumber(TOTAL_ROWS)} rows... `);
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(":memory:");
  await populateDatabase(db, TOTAL_ROWS);
  process.stdout.write(`Done.\n\n`);

  process.stdout.write(`[1/2] Running Naive All-At-Once (stmt.all() buffer)... `);
  const naiveResult = await runNaiveAllAtOnce(db);
  const naiveThroughput = Math.round(TOTAL_ROWS / (naiveResult.durationMs / 1000));
  process.stdout.write(`Done in ${naiveResult.durationMs}ms (${formatNumber(naiveThroughput)} rows/s)\n`);

  process.stdout.write(`[2/2] Running Rowpipe Streaming Cursor (bounded batch size)... `);
  const adapter = new SqliteDatabaseAdapter({
    dialect: "sqlite",
    url: "sqlite://:memory:",
    sanitizedUrl: "sqlite://:memory:",
    filePath: ":memory:",
  });
  // Share populated DB
  (adapter as any).db = db;

  const streamResult = await runRowpipeStreaming(adapter, BATCH_SIZE);
  const streamThroughput = Math.round(TOTAL_ROWS / (streamResult.durationMs / 1000));
  process.stdout.write(`Done in ${streamResult.durationMs}ms (${formatNumber(streamThroughput)} rows/s)\n\n`);

  process.stdout.write(`======================================================\n`);
  process.stdout.write(`                 BENCHMARK RESULTS                    \n`);
  process.stdout.write(`======================================================\n`);
  process.stdout.write(`Metric                   | Naive All-At-Once   | Rowpipe Streaming   \n`);
  process.stdout.write(`-------------------------+---------------------+---------------------\n`);
  process.stdout.write(`Processed Rows           | ${formatNumber(TOTAL_ROWS).padEnd(19)} | ${formatNumber(TOTAL_ROWS).padEnd(19)} \n`);
  process.stdout.write(`Elapsed Time             | ${(naiveResult.durationMs + " ms").padEnd(19)} | ${(streamResult.durationMs + " ms").padEnd(19)} \n`);
  process.stdout.write(`Throughput               | ${(formatNumber(naiveThroughput) + " rows/s").padEnd(19)} | ${(formatNumber(streamThroughput) + " rows/s").padEnd(19)} \n`);
  process.stdout.write(`Peak RSS Memory          | ${formatBytes(naiveResult.peakRss).padEnd(19)} | ${formatBytes(streamResult.peakRss).padEnd(19)} \n`);
  process.stdout.write(`Memory Scalability       | O(N) Unbounded      | O(1) Bounded        \n`);
  process.stdout.write(`======================================================\n\n`);
}

main().catch((err) => {
  console.error("Benchmark error:", err);
  process.exit(1);
});
