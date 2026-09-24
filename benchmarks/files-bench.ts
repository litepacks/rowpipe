import { promises as fsPromises } from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { FileSystemReader, createPipeline, filterRows, selectColumns } from "../src/index.js";

async function main() {
  const tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "rowpipe-files-bench-"));
  const totalDirectories = 100;
  const filesPerDir = 500;
  const totalFiles = totalDirectories * filesPerDir; // 50,000 files

  console.log("============================================================");
  console.log("           Rowpipe Filesystem Streaming Benchmark");
  console.log("============================================================");
  console.log(`Generating ${totalFiles.toLocaleString()} synthetic files...`);

  const genStart = Date.now();
  for (let d = 0; d < totalDirectories; d++) {
    const dir = path.join(tmpDir, `dir_${d}`);
    await fsPromises.mkdir(dir, { recursive: true });
    for (let f = 0; f < filesPerDir; f++) {
      const ext = f % 5 === 0 ? "csv" : f % 3 === 0 ? "jsonl" : "txt";
      await fsPromises.writeFile(
        path.join(dir, `item_${f}.${ext}`),
        `id,value\n${f},synthetic_payload_${d}_${f}\n`
      );
    }
  }
  const genTime = ((Date.now() - genStart) / 1000).toFixed(2);
  console.log(`Generated in ${genTime}s.`);

  // Benchmark 1: Pure Streaming Traversal + Metadata Extraction
  console.log("\n1. Running Streaming Traversal + Metadata Extraction...");
  const memBefore = process.memoryUsage().rss;
  const startScan = Date.now();

  const reader = new FileSystemReader({
    root: tmpDir,
    recursive: true,
    batchSize: 1000,
  });

  let filesScanned = 0;
  let totalBytes = 0;
  let peakRss = memBefore;

  for await (const batch of reader.read()) {
    filesScanned += batch.rows.length;
    for (const r of batch.rows) {
      totalBytes += Number(r["size"] || 0);
    }
    const currentRss = process.memoryUsage().rss;
    if (currentRss > peakRss) peakRss = currentRss;
  }

  const scanElapsedSec = (Date.now() - startScan) / 1000;
  const scanRate = Math.round(filesScanned / scanElapsedSec);
  const peakMemMb = (peakRss / 1024 / 1024).toFixed(1);

  console.log("------------------------------------------------------------");
  console.log(`Files Scanned:       ${filesScanned.toLocaleString()}`);
  console.log(`Total File Size:     ${(totalBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Scan Duration:       ${scanElapsedSec.toFixed(2)}s`);
  console.log(`Throughput:          ${scanRate.toLocaleString()} files/s`);
  console.log(`Peak RSS Memory:     ${peakMemMb} MB`);

  // Benchmark 2: Streaming Pipeline with Filtering & Selection
  console.log("\n2. Running Pipeline (Files -> Filter size > 0 -> Select)...");
  const startPipe = Date.now();
  const pipeReader = new FileSystemReader({ root: tmpDir, batchSize: 1000 });
  const pipeline = createPipeline(pipeReader)
    .pipe(filterRows("size > 0"))
    .pipe(selectColumns(["relative_path", "extension", "size"]));

  let pipeCount = 0;
  for await (const batch of pipeline.batches()) {
    pipeCount += batch.rows.length;
  }
  const pipeElapsedSec = (Date.now() - startPipe) / 1000;
  const pipeRate = Math.round(pipeCount / pipeElapsedSec);

  console.log("------------------------------------------------------------");
  console.log(`Filtered Stream:     ${pipeCount.toLocaleString()} rows`);
  console.log(`Pipeline Time:       ${pipeElapsedSec.toFixed(2)}s`);
  console.log(`Pipeline Rate:       ${pipeRate.toLocaleString()} files/s`);

  // Benchmark 3: Fast Content Fingerprinting
  console.log("\n3. Running Fast Streaming Fingerprinting (--hash fast)...");
  const startFastHash = Date.now();
  const hashReader = new FileSystemReader({
    root: tmpDir,
    hash: "fast",
    batchSize: 1000,
    concurrency: 16,
  });

  let hashedCount = 0;
  for await (const batch of hashReader.read()) {
    hashedCount += batch.rows.length;
  }
  const hashElapsedSec = (Date.now() - startFastHash) / 1000;
  const hashRate = Math.round(hashedCount / hashElapsedSec);

  console.log("------------------------------------------------------------");
  console.log(`Hashed Files:        ${hashedCount.toLocaleString()} files`);
  console.log(`Hash Time:           ${hashElapsedSec.toFixed(2)}s`);
  console.log(`Hash Throughput:     ${hashRate.toLocaleString()} files/s`);
  console.log("============================================================");

  // Cleanup
  console.log("\nCleaning up temporary benchmark files...");
  await fsPromises.rm(tmpDir, { recursive: true, force: true });
  console.log("Done.");
}

main().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
