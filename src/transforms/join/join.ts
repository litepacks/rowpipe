import { encodeCompositeKeyString, encodeSingleKeyValue } from "../../diff/key.js";
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
 * Generates an optimized key extractor for a row given key columns.
 */
function createRowKeyExtractor(keyCols: string[]): (row: Row) => string {
  const len = keyCols.length;
  if (len === 1) {
    const col0 = keyCols[0]!;
    return (row: Row) => encodeSingleKeyValue(row[col0]);
  }
  if (len === 2) {
    const col0 = keyCols[0]!;
    const col1 = keyCols[1]!;
    return (row: Row) =>
      encodeSingleKeyValue(row[col0]) + "|" + encodeSingleKeyValue(row[col1]);
  }
  return (row: Row) => encodeCompositeKeyString(row, keyCols);
}

/**
 * Compiles a zero-allocation row merger for joining left and right rows.
 */
function createRowMerger(
  joinKeys: JoinKeyMapping,
  options: JoinOptions
): (
  leftRow: Row | null,
  rightRow: Row | null,
  sampleLeftCols?: string[],
  sampleRightCols?: string[]
) => Row {
  const leftKeySet = new Set(joinKeys.left);
  const rightKeyMap = new Map<string, string>();
  for (let i = 0; i < joinKeys.right.length; i++) {
    rightKeyMap.set(joinKeys.right[i]!, joinKeys.left[i]!);
  }

  const prefixRight = options.prefixRight || "";
  const suffixRight =
    options.suffixRight !== undefined
      ? options.suffixRight
      : prefixRight
        ? ""
        : "_right";
  const prefixLeft = options.prefixLeft || "";
  const suffixLeft = options.suffixLeft || "";

  const hasLeftDecoration = prefixLeft.length > 0 || suffixLeft.length > 0;
  const hasRightDecoration =
    prefixRight.length > 0 || (suffixRight.length > 0 && suffixRight !== "_right");

  return function merge(
    leftRow: Row | null,
    rightRow: Row | null,
    sampleLeftCols?: string[],
    sampleRightCols?: string[]
  ): Row {
    const result: Row = {};

    if (leftRow) {
      for (const k in leftRow) {
        const isKey = leftKeySet.has(k);
        const outKey = isKey || !hasLeftDecoration ? k : `${prefixLeft}${k}${suffixLeft}`;
        result[outKey] = leftRow[k];
      }
    } else if (sampleLeftCols) {
      for (let i = 0; i < sampleLeftCols.length; i++) {
        const col = sampleLeftCols[i]!;
        const isKey = leftKeySet.has(col);
        const outKey = isKey || !hasLeftDecoration ? col : `${prefixLeft}${col}${suffixLeft}`;
        result[outKey] = null;
      }
    }

    if (rightRow) {
      for (const k in rightRow) {
        const leftKeyName = rightKeyMap.get(k);
        if (leftKeyName !== undefined) {
          if (!leftRow) {
            result[leftKeyName] = rightRow[k];
          }
          continue;
        }

        let targetKey = k;
        if ((leftRow && k in leftRow) || hasRightDecoration) {
          targetKey = `${prefixRight}${k}${suffixRight}`;
        }
        result[targetKey] = rightRow[k];
      }
    } else if (sampleRightCols) {
      for (let i = 0; i < sampleRightCols.length; i++) {
        const col = sampleRightCols[i]!;
        if (rightKeyMap.has(col)) continue;
        let targetKey = col;
        if ((leftRow && col in leftRow) || hasRightDecoration) {
          targetKey = `${prefixRight}${col}${suffixRight}`;
        }
        result[targetKey] = null;
      }
    }

    return result;
  };
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

  const getRightKey = createRowKeyExtractor(keys.right);
  const getLeftKey = createRowKeyExtractor(keys.left);
  const mergeRows = createRowMerger(keys, options);

  let rightSampleCols: string[] = [];
  let leftSampleCols: string[] = [];

  try {
    // 1. Build Phase: Ingest right dataset into SpillableJoinIndex
    for await (const batch of rightStream) {
      const rows = batch.rows;
      const len = rows.length;
      for (let i = 0; i < len; i++) {
        const row = rows[i]!;
        if (rightSampleCols.length === 0) {
          rightSampleCols = Object.keys(row);
        }
        const key = getRightKey(row);
        if (rightIndex.setSync && rightIndex.setSync(key, row)) {
          continue;
        }
        await rightIndex.set(key, row);
      }
    }

    // 2. Probe Phase: Stream left dataset and match against rightIndex
    let currentBatch: Row[] = [];
    let globalOffset = 0;

    for await (const batch of leftStream) {
      const rows = batch.rows;
      const len = rows.length;
      for (let i = 0; i < len; i++) {
        const leftRow = rows[i]!;
        if (leftSampleCols.length === 0) {
          leftSampleCols = Object.keys(leftRow);
        }
        const key = getLeftKey(leftRow);

        if (joinType === "semi") {
          let exists = rightIndex.hasSync ? rightIndex.hasSync(key) : undefined;
          if (exists === undefined) {
            exists = await rightIndex.has(key);
          }
          if (exists) {
            currentBatch.push(leftRow);
          }
        } else if (joinType === "anti") {
          let exists = rightIndex.hasSync ? rightIndex.hasSync(key) : undefined;
          if (exists === undefined) {
            exists = await rightIndex.has(key);
          }
          if (!exists) {
            currentBatch.push(leftRow);
          }
        } else {
          let matchedRightRows = rightIndex.getSync ? rightIndex.getSync(key) : undefined;
          if (matchedRightRows === undefined) {
            matchedRightRows = await rightIndex.get(key);
          }
          if (matchedRightRows && matchedRightRows.length > 0) {
            rightIndex.markMatched(key);
            for (let m = 0; m < matchedRightRows.length; m++) {
              const merged = mergeRows(leftRow, matchedRightRows[m]!, leftSampleCols, rightSampleCols);
              currentBatch.push(merged);
            }
          } else if (joinType === "left" || joinType === "full") {
            const merged = mergeRows(leftRow, null, leftSampleCols, rightSampleCols);
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
        const merged = mergeRows(null, unmatchedRightRow, leftSampleCols, rightSampleCols);
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
