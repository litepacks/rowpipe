import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

/**
 * Skips the first `skipCount` rows in the stream without buffering.
 * Zero memory overhead ($O(1)$).
 */
export function offsetRows(skipCount: number): TransformFunction {
  const offsetToSkip = Math.max(0, skipCount);

  return (stream: DataStream): DataStream => {
    return (async function* () {
      if (offsetToSkip === 0) {
        yield* stream;
        return;
      }

      let skipped = 0;

      for await (const batch of stream) {
        if (batch.rows.length === 0) continue;

        if (skipped < offsetToSkip) {
          const remainingToSkip = offsetToSkip - skipped;

          if (batch.rows.length <= remainingToSkip) {
            skipped += batch.rows.length;
            continue;
          }

          // Partial batch skip
          const remainingRows = batch.rows.slice(remainingToSkip);
          skipped += remainingToSkip;

          yield {
            rows: remainingRows,
            offset: batch.offset + remainingToSkip,
          };
        } else {
          yield batch;
        }
      }
    })();
  };
}
