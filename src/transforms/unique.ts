import { encodeCompositeKeyString, encodeSingleKeyValue } from "../diff/key.js";
import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { SpillableKeyStore } from "../utils/keystore.js";

export interface UniqueOptions {
  by?: string | string[];
  keep?: "first" | "last";
  memoryLimit?: number | string;
  tempDir?: string;
  batchSize?: number;
}

/**
 * Deduplicates rows by key columns (or whole row) with spillable disk backup for large cardinalities.
 */
export function uniqueRows(options: UniqueOptions = {}): TransformFunction {
  const keep = options.keep || "first";
  const byCols = options.by
    ? (Array.isArray(options.by) ? options.by : options.by.split(",").map((s) => s.trim()).filter(Boolean))
    : undefined;
  const numByCols = byCols ? byCols.length : 0;
  const col0 = numByCols > 0 ? byCols![0]! : undefined;
  const col1 = numByCols > 1 ? byCols![1]! : undefined;
  const effectiveBatchSize = options.batchSize || 1000;

  function getKey(row: Row): string {
    if (numByCols === 0) {
      return JSON.stringify(row);
    }
    if (numByCols === 1) {
      return encodeSingleKeyValue(row[col0!]);
    }
    if (numByCols === 2) {
      return (
        encodeSingleKeyValue(row[col0!]) +
        "|" +
        encodeSingleKeyValue(row[col1!])
      );
    }
    return encodeCompositeKeyString(row, byCols!);
  }

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
            const rows = batch.rows;
            const len = rows.length;
            for (let i = 0; i < len; i++) {
              const row = rows[i]!;
              const key = getKey(row);

              let isNew = keyStore.addSync(key);
              if (isNew === null) {
                isNew = await keyStore.add(key);
              }

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
            const rows = batch.rows;
            const len = rows.length;
            for (let i = 0; i < len; i++) {
              const row = rows[i]!;
              const key = getKey(row);
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
