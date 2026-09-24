import { encodeCompositeKey } from "../../diff/key.js";
import { InvalidArgumentError } from "../../core/errors.js";
import type { DataBatch, DataStream, Row, TabularReader, TransformFunction } from "../../core/types.js";
import { SpillableJoinIndex } from "./index-storage.js";
import type { JoinKeyMapping, JoinOptions, JoinType } from "./types.js";

/**
 * Parses user join key options into structured left/right column mappings.
 */
export function parseJoinKeys(options: JoinOptions): JoinKeyMapping {
  if (options.leftKey && options.rightKey) {
    const leftCols = Array.isArray(options.leftKey) ? options.leftKey : options.leftKey.split(",").map((s) => s.trim());
    const rightCols = Array.isArray(options.rightKey) ? options.rightKey : options.rightKey.split(",").map((s) => s.trim());
    if (leftCols.length !== rightCols.length) {
      throw new InvalidArgumentError(
        `Mismatched join keys: left has ${leftCols.length} columns (${leftCols.join(",")}), right has ${rightCols.length} columns (${rightCols.join(",")})`
      );
    }
    return { left: leftCols, right: rightCols };
  }

  if (options.on) {
    const specs = Array.isArray(options.on) ? options.on : options.on.split(",").map((s) => s.trim());
    const leftCols: string[] = [];
    const rightCols: string[] = [];

    for (const spec of specs) {
      if (spec.includes("=")) {
        const parts = spec.split("=");
        leftCols.push(parts[0]!.trim());
        rightCols.push(parts[1]!.trim());
      } else {
        leftCols.push(spec.trim());
        rightCols.push(spec.trim());
      }
    }
    return { left: leftCols, right: rightCols };
  }

  // Default fallback to 'id'
  return { left: ["id"], right: ["id"] };
}

/**
 * Generates an encoded lookup key for a row given key columns.
 */
function getRowKey(row: Row, keyCols: string[]): string {
  return encodeCompositeKey(row, keyCols).encoded;
}

/**
 * Merges a left row and right row, handling colliding column names.
 */
function mergeRows(
  leftRow: Row | null,
  rightRow: Row | null,
  joinKeys: JoinKeyMapping,
  options: JoinOptions,
  sampleLeftCols?: string[],
  sampleRightCols?: string[]
): Row {
  const result: Row = {};
  const prefixRight = options.prefixRight || "";
  const suffixRight = options.suffixRight !== undefined ? options.suffixRight : (prefixRight ? "" : "_right");
  const prefixLeft = options.prefixLeft || "";
  const suffixLeft = options.suffixLeft || "";

  if (leftRow) {
    for (const [k, v] of Object.entries(leftRow)) {
      const isKey = joinKeys.left.includes(k);
      const outKey = isKey ? k : `${prefixLeft}${k}${suffixLeft}`;
      result[outKey] = v;
    }
  } else if (sampleLeftCols) {
    for (const col of sampleLeftCols) {
      const isKey = joinKeys.left.includes(col);
      const outKey = isKey ? col : `${prefixLeft}${col}${suffixLeft}`;
      result[outKey] = null;
    }
  }

  if (rightRow) {
    for (const [k, v] of Object.entries(rightRow)) {
      const rightKeyIdx = joinKeys.right.indexOf(k);
      if (rightKeyIdx >= 0) {
        // This is a join key column
        const leftKeyName = joinKeys.left[rightKeyIdx]!;
        if (!leftRow) {
          result[leftKeyName] = v;
        }
        continue;
      }

      // Check if collides with left row non-key column
      let targetKey = k;
      if (leftRow && k in leftRow) {
        targetKey = `${prefixRight}${k}${suffixRight}`;
      } else if (prefixRight || (suffixRight && suffixRight !== "_right")) {
        targetKey = `${prefixRight}${k}${suffixRight}`;
      }
      result[targetKey] = v;
    }
  } else if (sampleRightCols) {
    for (const col of sampleRightCols) {
      const rightKeyIdx = joinKeys.right.indexOf(col);
      if (rightKeyIdx >= 0) continue;
      let targetKey = col;
      if (leftRow && col in leftRow) {
        targetKey = `${prefixRight}${col}${suffixRight}`;
      } else if (prefixRight || (suffixRight && suffixRight !== "_right")) {
        targetKey = `${prefixRight}${col}${suffixRight}`;
      }
      result[targetKey] = null;
    }
  }

  return result;
}

/**
 * Joins two tabular streams with bounded memory and spill-to-disk index support.
 */
export async function* joinStreams(
  leftSource: DataStream | TabularReader,
  rightSource: DataStream | TabularReader,
  options: JoinOptions = {}
): DataStream {
  const joinType: JoinType = options.type || "inner";
  const keys = parseJoinKeys(options);
  const effectiveBatchSize = Math.max(1, options.batchSize || 1000);

  const rightIndex = new SpillableJoinIndex({
    memoryLimit: options.memoryLimit,
    tempDir: options.tempDir,
  });

  const rightStream: DataStream = "read" in rightSource ? rightSource.read({ batchSize: effectiveBatchSize }) : rightSource;
  const leftStream: DataStream = "read" in leftSource ? leftSource.read({ batchSize: effectiveBatchSize }) : leftSource;

  let rightSampleCols: string[] = [];
  let leftSampleCols: string[] = [];

  try {
    // 1. Build Phase: Ingest right dataset into SpillableJoinIndex
    for await (const batch of rightStream) {
      for (const row of batch.rows) {
        if (rightSampleCols.length === 0) {
          rightSampleCols = Object.keys(row);
        }
        const key = getRowKey(row, keys.right);
        await rightIndex.set(key, row);
      }
    }

    // 2. Probe Phase: Stream left dataset and match against rightIndex
    let currentBatch: Row[] = [];
    let globalOffset = 0;

    for await (const batch of leftStream) {
      for (const leftRow of batch.rows) {
        if (leftSampleCols.length === 0) {
          leftSampleCols = Object.keys(leftRow);
        }
        const key = getRowKey(leftRow, keys.left);

        if (joinType === "semi") {
          const exists = await rightIndex.has(key);
          if (exists) {
            currentBatch.push(leftRow);
          }
        } else if (joinType === "anti") {
          const exists = await rightIndex.has(key);
          if (!exists) {
            currentBatch.push(leftRow);
          }
        } else {
          const matchedRightRows = await rightIndex.get(key);
          if (matchedRightRows && matchedRightRows.length > 0) {
            rightIndex.markMatched(key);
            for (const rightRow of matchedRightRows) {
              const merged = mergeRows(leftRow, rightRow, keys, options, leftSampleCols, rightSampleCols);
              currentBatch.push(merged);
            }
          } else if (joinType === "left" || joinType === "full") {
            const merged = mergeRows(leftRow, null, keys, options, leftSampleCols, rightSampleCols);
            currentBatch.push(merged);
          }
        }

        if (currentBatch.length >= effectiveBatchSize) {
          yield {
            rows: currentBatch,
            offset: globalOffset,
          };
          globalOffset += currentBatch.length;
          currentBatch = [];
        }
      }
    }

    // 3. Post-Probe Phase: For right and full outer joins, emit unmatched right rows
    if (joinType === "right" || joinType === "full") {
      for await (const unmatchedRightRow of rightIndex.getUnmatched()) {
        const merged = mergeRows(null, unmatchedRightRow, keys, options, leftSampleCols, rightSampleCols);
        currentBatch.push(merged);

        if (currentBatch.length >= effectiveBatchSize) {
          yield {
            rows: currentBatch,
            offset: globalOffset,
          };
          globalOffset += currentBatch.length;
          currentBatch = [];
        }
      }
    }

    if (currentBatch.length > 0) {
      yield {
        rows: currentBatch,
        offset: globalOffset,
      };
      globalOffset += currentBatch.length;
    }
  } finally {
    await rightIndex.close();
    if ("close" in rightSource && typeof rightSource.close === "function") {
      await rightSource.close();
    }
    if ("close" in leftSource && typeof leftSource.close === "function") {
      await leftSource.close();
    }
  }
}

/**
 * Returns a TransformFunction for pipeline chaining (e.g. pipeline.pipe(joinRows(rightReader, options))).
 */
export function joinRows(
  rightSource: DataStream | TabularReader,
  options: JoinOptions = {}
): TransformFunction {
  return (leftStream: DataStream): DataStream => {
    return joinStreams(leftStream, rightSource, options);
  };
}
