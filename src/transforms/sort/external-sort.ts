import { createReadStream, createWriteStream, promises as fsPromises } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as readline from "node:readline";
import { parseMemoryLimit } from "../../diff/storage/spillable-index.js";
import type { DataBatch, DataStream, Row, TransformFunction } from "../../core/types.js";
import { Heap } from "../../utils/heap.js";
import {
  createRowComparator,
  parseSortSpecs,
  SequencedRow,
  SortOptions,
} from "./comparator.js";

interface HeapRunEntry {
  row: Row;
  seq: number;
  runIndex: number;
}

/**
 * External Merge Sort Engine.
 * Sorts arbitrary-scale datasets with bounded RAM and K-Way Heap merge.
 */
export function externalSort(options: SortOptions): TransformFunction {
  const specs = parseSortSpecs(options.by, {
    nulls: options.nulls,
    ignoreCase: options.ignoreCase,
    natural: options.natural,
  });

  const rowComparator = createRowComparator(specs);
  const memoryLimitBytes = parseMemoryLimit(options.memoryLimit, 256 * 1024 * 1024);
  const effectiveBatchSize = options.batchSize || 1000;
  const baseTempDir = options.tempDir || os.tmpdir();

  return (stream: DataStream): DataStream => {
    return (async function* () {
      let currentRun: SequencedRow[] = [];
      let currentRunBytes = 0;
      let totalSeq = 0;
      const runFiles: string[] = [];
      const sessionDir = path.join(
        baseTempDir,
        `rowpipe-sort-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
      );

      const cleanup = async () => {
        try {
          await fsPromises.rm(sessionDir, { recursive: true, force: true });
        } catch {
          // Ignore temp cleanup error
        }
      };

      try {
        for await (const batch of stream) {
          for (let i = 0; i < batch.rows.length; i++) {
            const row = batch.rows[i]!;
            currentRun.push({ row, seq: totalSeq++ });
            // Estimate row memory (~64 bytes overhead + keys/values)
            currentRunBytes += 64 + Object.keys(row).length * 24;

            if (currentRunBytes >= memoryLimitBytes) {
              // Spill current run
              await fsPromises.mkdir(sessionDir, { recursive: true });
              currentRun.sort(rowComparator);

              const runFilePath = path.join(sessionDir, `run_${runFiles.length}.jsonl`);
              const writeStream = createWriteStream(runFilePath, { encoding: "utf-8" });

              for (let r = 0; r < currentRun.length; r++) {
                const item = currentRun[r]!;
                writeStream.write(JSON.stringify({ row: item.row, seq: item.seq }) + "\n");
              }

              await new Promise<void>((resolve, reject) => {
                writeStream.end((err?: Error | null) => {
                  if (err) reject(err);
                  else resolve();
                });
              });

              runFiles.push(runFilePath);
              currentRun = [];
              currentRunBytes = 0;
            }
          }
        }

        // Case 1: All data fit in memory without spilling
        if (runFiles.length === 0) {
          currentRun.sort(rowComparator);
          let offset = 0;
          for (let i = 0; i < currentRun.length; i += effectiveBatchSize) {
            const slice = currentRun.slice(i, i + effectiveBatchSize).map((item) => item.row);
            yield {
              rows: slice,
              offset,
            };
            offset += slice.length;
          }
          return;
        }

        // Case 2: Spill remainder run if any rows are in memory
        if (currentRun.length > 0) {
          await fsPromises.mkdir(sessionDir, { recursive: true });
          currentRun.sort(rowComparator);

          const runFilePath = path.join(sessionDir, `run_${runFiles.length}.jsonl`);
          const writeStream = createWriteStream(runFilePath, { encoding: "utf-8" });

          for (let r = 0; r < currentRun.length; r++) {
            const item = currentRun[r]!;
            writeStream.write(JSON.stringify({ row: item.row, seq: item.seq }) + "\n");
          }

          await new Promise<void>((resolve, reject) => {
            writeStream.end((err?: Error | null) => {
              if (err) reject(err);
              else resolve();
            });
          });

          runFiles.push(runFilePath);
          currentRun = [];
          currentRunBytes = 0;
        }

        // Case 3: K-Way Merge using Min-Heap
        const k = runFiles.length;
        const iterators: AsyncIterator<string>[] = new Array(k);

        for (let i = 0; i < k; i++) {
          const rl = readline.createInterface({
            input: createReadStream(runFiles[i]!, { encoding: "utf-8" }),
            crlfDelay: Infinity,
          });
          iterators[i] = rl[Symbol.asyncIterator]();
        }

        const heapComparator = (a: HeapRunEntry, b: HeapRunEntry) => {
          return rowComparator({ row: a.row, seq: a.seq }, { row: b.row, seq: b.seq });
        };

        const heap = new Heap<HeapRunEntry>(heapComparator);

        // Prime the heap with 1 item from each run
        for (let i = 0; i < k; i++) {
          const next = await iterators[i]!.next();
          if (!next.done && next.value.trim().length > 0) {
            const parsed = JSON.parse(next.value) as { row: Row; seq: number };
            heap.push({ row: parsed.row, seq: parsed.seq, runIndex: i });
          }
        }

        let outputRows: Row[] = [];
        let globalOffset = 0;

        while (!heap.isEmpty()) {
          const smallest = heap.pop()!;
          outputRows.push(smallest.row);

          if (outputRows.length >= effectiveBatchSize) {
            yield {
              rows: outputRows,
              offset: globalOffset,
            };
            globalOffset += outputRows.length;
            outputRows = [];
          }

          // Advance iterator for smallest.runIndex
          const next = await iterators[smallest.runIndex]!.next();
          if (!next.done && next.value.trim().length > 0) {
            const parsed = JSON.parse(next.value) as { row: Row; seq: number };
            heap.push({ row: parsed.row, seq: parsed.seq, runIndex: smallest.runIndex });
          }
        }

        if (outputRows.length > 0) {
          yield {
            rows: outputRows,
            offset: globalOffset,
          };
        }
      } finally {
        await cleanup();
      }
    })();
  };
}
