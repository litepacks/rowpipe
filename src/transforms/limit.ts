import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

/**
 * Limits the number of rows emitted to `maxCount` and cancels the upstream stream immediately.
 * Zero memory overhead ($O(1)$) and true early cancellation.
 */
export function limitRows(maxCount: number): TransformFunction {
  const limit = Math.max(0, maxCount);

  return (stream: DataStream): DataStream => {
    return (async function* () {
      if (limit === 0) return;

      let emitted = 0;

      for await (const batch of stream) {
        if (batch.rows.length === 0) continue;

        const remaining = limit - emitted;
        if (remaining <= 0) {
          break;
        }

        if (batch.rows.length <= remaining) {
          yield batch;
          emitted += batch.rows.length;
        } else {
          // Slice partial batch to meet exact limit
          const sliceRows: Row[] = batch.rows.slice(0, remaining);
          yield {
            rows: sliceRows,
            offset: batch.offset,
          };
          emitted += sliceRows.length;
          break;
        }

        if (emitted >= limit) {
          break;
        }
      }
    })();
  };
}
