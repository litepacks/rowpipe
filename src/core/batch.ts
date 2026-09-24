import type { DataBatch, DataStream, Row } from "./types.js";

/**
 * Packs an AsyncIterable of individual rows into DataBatch chunks with a given batchSize.
 */
export async function* rowsToBatches(
  rows: AsyncIterable<Row> | Iterable<Row>,
  batchSize = 1000
): AsyncGenerator<DataBatch> {
  const effectiveBatchSize = Math.max(1, batchSize);
  let currentRows: Row[] = [];
  let totalOffset = 0;

  for await (const row of rows) {
    currentRows.push(row);
    if (currentRows.length >= effectiveBatchSize) {
      yield {
        rows: currentRows,
        offset: totalOffset,
      };
      totalOffset += currentRows.length;
      currentRows = [];
    }
  }

  if (currentRows.length > 0) {
    yield {
      rows: currentRows,
      offset: totalOffset,
    };
  }
}

/**
 * Unpacks DataBatches into individual rows.
 */
export async function* batchesToRows(stream: DataStream): AsyncGenerator<Row> {
  for await (const batch of stream) {
    for (const row of batch.rows) {
      yield row;
    }
  }
}

/**
 * Applies a mapping function to each row in batches, maintaining batch structure
 * and filtering out null/undefined results.
 */
export async function* batchMap(
  stream: DataStream,
  fn: (row: Row, globalIndex: number) => Row | null | undefined,
  targetBatchSize = 1000
): AsyncGenerator<DataBatch> {
  let pendingRows: Row[] = [];
  let currentOffset = 0;
  let globalIndex = 0;

  for await (const batch of stream) {
    for (const row of batch.rows) {
      const transformed = fn(row, globalIndex++);
      if (transformed !== null && transformed !== undefined) {
        pendingRows.push(transformed);
        if (pendingRows.length >= targetBatchSize) {
          yield {
            rows: pendingRows,
            offset: currentOffset,
          };
          currentOffset += pendingRows.length;
          pendingRows = [];
        }
      }
    }
  }

  if (pendingRows.length > 0) {
    yield {
      rows: pendingRows,
      offset: currentOffset,
    };
  }
}
