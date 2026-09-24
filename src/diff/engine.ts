import { DuplicateKeyError, InvalidArgumentError } from "../core/errors.js";
import type { ReaderOptions, Row, TabularReader } from "../core/types.js";
import { createReader } from "../readers/index.js";
import { areValuesEqual, compareRows } from "./comparator.js";
import { computeRowFingerprint } from "./hash.js";
import { encodeCompositeKey } from "./key.js";
import { computeSchemaDiff } from "./schema.js";
import { SpillableDiffIndex } from "./storage/spillable-index.js";
import type {
  DiffEngineOptions,
  DiffEvent,
  DiffProgressInfo,
  DiffSummary,
  IndexedRow,
} from "./types.js";

function createReaderFromSource(
  source: TabularReader | string,
  options?: ReaderOptions
): { reader: TabularReader; sourceName: string } {
  if (typeof source === "string") {
    return {
      reader: createReader(source, options),
      sourceName: source,
    };
  }
  return {
    reader: source,
    sourceName: options?.filePath || "stream",
  };
}

/**
 * Streams fine-grained diff events (added, removed, changed, unchanged) between two datasets.
 */
export async function* diffRows(options: DiffEngineOptions): AsyncIterable<DiffEvent> {
  const { keys, ignore = [], columns, duplicateKey = "error" } = options;

  if (!keys || keys.length === 0) {
    throw new InvalidArgumentError("At least one key column must be specified with --key");
  }

  // Validate that key columns are not in ignore list
  for (const k of keys) {
    if (ignore.includes(k)) {
      throw new InvalidArgumentError(`Cannot ignore key column "${k}" in diff.`);
    }
  }

  const leftInput = options.left ?? options.leftPath;
  const rightInput = options.right ?? options.rightPath;

  if (!leftInput) {
    throw new InvalidArgumentError("Left source is required for diff.");
  }
  if (!rightInput) {
    throw new InvalidArgumentError("Right source is required for diff.");
  }

  const leftOpts: ReaderOptions = {
    ...options.leftOptions,
    format: options.fromLeft ?? options.leftOptions?.format,
  };
  const rightOpts: ReaderOptions = {
    ...options.rightOptions,
    format: options.fromRight ?? options.rightOptions?.format,
  };

  const { reader: leftReader, sourceName: leftSource } = createReaderFromSource(
    leftInput,
    leftOpts
  );
  const { reader: rightReader, sourceName: rightSource } = createReaderFromSource(
    rightInput,
    rightOpts
  );

  const index = new SpillableDiffIndex(options.memoryLimitBytes);

  const startTime = Date.now();

  let leftRowsCount = 0;
  let rightRowsCount = 0;
  let addedCount = 0;
  let removedCount = 0;
  let changedCount = 0;
  let unchangedCount = 0;

  // Track discovered columns
  const allDiscoveredColumns = new Set<string>();
  const seenLeftKeys = new Map<string, number>();

  const reportProgress = (phase: DiffProgressInfo["phase"]) => {
    if (options.onProgress) {
      const spillStats = index.getSpillStats?.();
      options.onProgress({
        phase,
        leftRowsProcessed: leftRowsCount,
        rightRowsProcessed: rightRowsCount,
        addedCount,
        removedCount,
        changedCount,
        unchangedCount,
        elapsedMs: Date.now() - startTime,
        isSpilled: spillStats?.isSpilled,
      });
    }
  };

  const explicitCompareCols = columns && columns.length > 0
    ? columns.filter((col) => !keys.includes(col) && !ignore.includes(col))
    : undefined;

  try {
    // ---------------------------------------------------------
    // Phase 1: Stream and Index Left Dataset
    // ---------------------------------------------------------
    let cachedLeftKeysList: string[] = [];
    let cachedLeftCompareCols: string[] = [];

    for await (const batch of leftReader.read(options.leftOptions)) {
      for (let i = 0; i < batch.rows.length; i++) {
        leftRowsCount++;
        const row = batch.rows[i]!;

        const { encoded, rawKey } = encodeCompositeKey(row, keys, { coerce: options.coerce });

        const firstSeenRow = seenLeftKeys.get(encoded);
        if (firstSeenRow !== undefined) {
          if (duplicateKey === "error") {
            const keyStr = Object.entries(rawKey)
              .map(([k, v]) => `${k}=${v}`)
              .join(", ");
            throw new DuplicateKeyError({
              dataset: "left",
              key: keyStr,
              rows: [firstSeenRow, leftRowsCount],
              filePath: leftSource,
            });
          }
          if (duplicateKey === "first") {
            continue;
          }
        } else {
          seenLeftKeys.set(encoded, leftRowsCount);
        }

        // Determine compared columns for this row with memoized cache
        let rowCompareCols: string[];
        if (explicitCompareCols) {
          rowCompareCols = explicitCompareCols;
          if (leftRowsCount === 1) {
            for (const col of Object.keys(row)) {
              allDiscoveredColumns.add(col);
            }
          }
        } else {
          const rowKeys = Object.keys(row);
          let match = rowKeys.length === cachedLeftKeysList.length;
          if (match) {
            for (let k = 0; k < rowKeys.length; k++) {
              if (rowKeys[k] !== cachedLeftKeysList[k]) {
                match = false;
                break;
              }
            }
          }

          if (match) {
            rowCompareCols = cachedLeftCompareCols;
          } else {
            for (let c = 0; c < rowKeys.length; c++) {
              allDiscoveredColumns.add(rowKeys[c]!);
            }
            cachedLeftKeysList = rowKeys;
            cachedLeftCompareCols = rowKeys.filter((col) => !keys.includes(col) && !ignore.includes(col));
            rowCompareCols = cachedLeftCompareCols;
          }
        }

        const hash = computeRowFingerprint(row, rowCompareCols);

        await index.set(encoded, {
          rowNumber: leftRowsCount,
          row,
          hash,
        });
      }
      reportProgress("indexing_left");
    }

    // ---------------------------------------------------------
    // Phase 2: Stream and Compare Right Dataset
    // ---------------------------------------------------------
    const seenRightKeys = new Map<string, number>();
    const comparisonOptions = {
      coerce: options.coerce,
      epsilon: options.epsilon,
      ignoreCase: options.ignoreCase,
      trim: options.trim,
    };

    for await (const batch of rightReader.read(options.rightOptions)) {
      for (let i = 0; i < batch.rows.length; i++) {
        rightRowsCount++;
        const rightRow = batch.rows[i]!;

        const { encoded, rawKey } = encodeCompositeKey(rightRow, keys, { coerce: options.coerce });

        const firstSeenRightRow = seenRightKeys.get(encoded);
        if (firstSeenRightRow !== undefined) {
          if (duplicateKey === "error") {
            const keyStr = Object.entries(rawKey)
              .map(([k, v]) => `${k}=${v}`)
              .join(", ");
            throw new DuplicateKeyError({
              dataset: "right",
              key: keyStr,
              rows: [firstSeenRightRow, rightRowsCount],
              filePath: rightSource,
            });
          }
          if (duplicateKey === "first") {
            continue;
          }
        } else {
          seenRightKeys.set(encoded, rightRowsCount);
        }

        const indexed = await index.get(encoded);

        if (!indexed) {
          // Key not in left dataset -> ADDED
          addedCount++;
          if (rightRowsCount <= 10) {
            for (const col of Object.keys(rightRow)) {
              allDiscoveredColumns.add(col);
            }
          }
          yield {
            type: "added",
            key: rawKey,
            encodedKey: encoded,
            row: rightRow,
            rowNumber: rightRowsCount,
          };
        } else {
          // Key exists in left dataset -> Check for Changes
          const leftRow = indexed.row;
          let compareCols: string[];

          if (explicitCompareCols) {
            compareCols = explicitCompareCols;
          } else {
            // Check if right row keys match cached list
            const rightKeys = Object.keys(rightRow);
            let match = rightKeys.length === cachedLeftKeysList.length;
            if (match) {
              for (let k = 0; k < rightKeys.length; k++) {
                if (rightKeys[k] !== cachedLeftKeysList[k]) {
                  match = false;
                  break;
                }
              }
            }

            if (match) {
              compareCols = cachedLeftCompareCols;
            } else {
              const combinedKeys = Array.from(
                new Set([...Object.keys(leftRow), ...rightKeys])
              );
              compareCols = getEffectiveCompareColumns(
                combinedKeys,
                keys,
                columns,
                ignore
              );
            }
          }

          const changes = compareRows(leftRow, rightRow, compareCols, comparisonOptions);

          if (changes.length > 0) {
            changedCount++;
            yield {
              type: "changed",
              key: rawKey,
              encodedKey: encoded,
              changes,
              leftRow,
              rightRow,
              leftRowNumber: indexed.rowNumber,
              rightRowNumber: rightRowsCount,
            };
          } else {
            unchangedCount++;
            yield {
              type: "unchanged",
              key: rawKey,
              encodedKey: encoded,
              row: rightRow,
              leftRowNumber: indexed.rowNumber,
              rightRowNumber: rightRowsCount,
            };
          }

          // Delete matched entry from index
          await index.delete(encoded);
        }
      }
      reportProgress("comparing_right");
    }

    // ---------------------------------------------------------
    // Phase 3: Remaining Left Entries -> REMOVED
    // ---------------------------------------------------------
    for await (const [encodedKey, indexed] of index.entries()) {
      removedCount++;
      const rawKey: Record<string, unknown> = {};
      for (const k of keys) {
        rawKey[k] = indexed.row[k];
      }

      yield {
        type: "removed",
        key: rawKey,
        encodedKey,
        row: indexed.row,
        rowNumber: indexed.rowNumber,
      };
    }

    reportProgress("finishing");
  } finally {
    await index.close();
    if (leftReader?.close) await leftReader.close();
    if (rightReader?.close) await rightReader.close();
  }
}

function getEffectiveCompareColumns(
  rowKeys: string[],
  keyColumns: string[],
  explicitColumns?: string[],
  ignoreColumns: string[] = []
): string[] {
  let targetCols = explicitColumns && explicitColumns.length > 0 ? explicitColumns : rowKeys;
  return targetCols.filter((col) => !keyColumns.includes(col) && !ignoreColumns.includes(col));
}

/**
 * Computes a complete DiffSummary by consuming the diff stream.
 */
export async function computeDiff(options: DiffEngineOptions): Promise<DiffSummary> {
  const startTime = Date.now();
  const columnChangesCount: Record<string, { changed: number }> = {};

  let added = 0;
  let removed = 0;
  let changed = 0;
  let unchanged = 0;
  let totalLeft = 0;
  let totalRight = 0;

  let recordedSpillStats: { isSpilled: boolean; spilledBytes: number } | undefined = undefined;
  const originalOnProgress = options.onProgress;

  const wrappedOptions: DiffEngineOptions = {
    ...options,
    onProgress: (info) => {
      if (info.isSpilled) {
        recordedSpillStats = { isSpilled: true, spilledBytes: 0 };
      }
      if (originalOnProgress) originalOnProgress(info);
    },
  };

  for await (const event of diffRows(wrappedOptions)) {
    switch (event.type) {
      case "added":
        added++;
        totalRight++;
        break;
      case "removed":
        removed++;
        totalLeft++;
        break;
      case "changed":
        changed++;
        totalLeft++;
        totalRight++;
        for (const ch of event.changes) {
          if (!columnChangesCount[ch.column]) {
            columnChangesCount[ch.column] = { changed: 0 };
          }
          columnChangesCount[ch.column]!.changed++;
        }
        break;
      case "unchanged":
        unchanged++;
        totalLeft++;
        totalRight++;
        break;
    }
  }

  // Schema diff if requested
  let schemaDiffResult: any = undefined;
  const leftInput = options.left ?? options.leftPath;
  const rightInput = options.right ?? options.rightPath;

  if (options.includeSchema !== false && leftInput && rightInput) {
    try {
      const leftOpts: ReaderOptions = {
        ...options.leftOptions,
        format: options.fromLeft ?? options.leftOptions?.format,
      };
      const rightOpts: ReaderOptions = {
        ...options.rightOptions,
        format: options.fromRight ?? options.rightOptions?.format,
      };
      const { reader: lReader } = createReaderFromSource(leftInput, leftOpts);
      const { reader: rReader } = createReaderFromSource(rightInput, rightOpts);
      schemaDiffResult = await computeSchemaDiff(lReader, rReader);
    } catch {
      // Ignore schema diff failure
    }
  }

  const leftSourceName =
    typeof leftInput === "string" ? leftInput : options.leftOptions?.filePath || "left";
  const rightSourceName =
    typeof rightInput === "string" ? rightInput : options.rightOptions?.filePath || "right";


  return {
    leftSource: leftSourceName,
    rightSource: rightSourceName,
    keyColumns: options.keys,
    rows: {
      added,
      removed,
      changed,
      unchanged,
      totalLeft,
      totalRight,
    },
    columns: columnChangesCount,
    schema: schemaDiffResult,
    executionTimeMs: Date.now() - startTime,
    spillStats: recordedSpillStats,
  };
}
