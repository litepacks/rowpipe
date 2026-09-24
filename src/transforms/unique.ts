import { encodeCompositeKey } from "../diff/key.js";
import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { SpillableKeyStore } from "../utils/keystore.js";

export interface UniqueOptions {
  by?: string | string[];
  keep?: "first" | "last";
  memoryLimit?: number | string;
  tempDir?: string;
  batchSize?: number;
}

function getRowUniqueKey(row: Row, byCols?: string[]): string {
  if (byCols && byCols.length > 0) {
    return encodeCompositeKey(row, byCols).encoded;
  }
  return JSON.stringify(row);
}

/**
 * Deduplicates rows by key columns (or whole row) with spillable disk backup for large cardinalities.
 */
export function uniqueRows(options: UniqueOptions = {}): TransformFunction {
  const keep = options.keep || "first";
  const byCols = options.by
    ? (Array.isArray(options.by) ? options.by : options.by.split(",").map((s) => s.trim()).filter(Boolean))
    : undefined;
  const effectiveBatchSize = options.batchSize || 1000;

  return (stream: DataStream): DataStream => {
    return (async function* () {
      const keyStore = new SpillableKeyStore({
        memoryLimit: options.memoryLimit,
        tempDir: options.tempDir,
      });

      try {
        if (keep === "first") {
          // Streaming mode: Emit immediately on first encounter
          let currentBatchRows: Row[] = [];
          let globalOffset = 0;

          for await (const batch of stream) {
            for (let i = 0; i < batch.rows.length; i++) {
              const row = batch.rows[i]!;
              const key = getRowUniqueKey(row, byCols);

              const isNew = await keyStore.add(key);
              if (isNew) {
                currentBatchRows.push(row);
                if (currentBatchRows.length >= effectiveBatchSize) {
                  yield {
                    rows: currentBatchRows,
                    offset: globalOffset,
                  };
                  globalOffset += currentBatchRows.length;
                  currentBatchRows = [];
                }
              }
            }
          }

          if (currentBatchRows.length > 0) {
            yield {
              rows: currentBatchRows,
              offset: globalOffset,
            };
          }
        } else {
          // keep: "last" mode: Retain latest row per key
          const latestRows = new Map<string, Row>();
          let globalOffset = 0;

          for await (const batch of stream) {
            for (let i = 0; i < batch.rows.length; i++) {
              const row = batch.rows[i]!;
              const key = getRowUniqueKey(row, byCols);
              latestRows.set(key, row);
            }
          }

          let currentBatchRows: Row[] = [];
          for (const row of latestRows.values()) {
            currentBatchRows.push(row);
            if (currentBatchRows.length >= effectiveBatchSize) {
              yield {
                rows: currentBatchRows,
                offset: globalOffset,
              };
              globalOffset += currentBatchRows.length;
              currentBatchRows = [];
            }
          }

          if (currentBatchRows.length > 0) {
            yield {
              rows: currentBatchRows,
              offset: globalOffset,
            };
          }
        }
      } finally {
        await keyStore.close();
      }
    })();
  };
}
