import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { RingBuffer } from "../utils/ring-buffer.js";

/**
 * Retains and emits only the last `count` rows of a stream using a bounded O(N) Ring Buffer.
 */
export function tailRows(count = 10): TransformFunction {
  const tailCount = Math.max(1, count);

  return (stream: DataStream): DataStream => {
    return (async function* () {
      const ringBuffer = new RingBuffer<Row>(tailCount);
      let totalRowsSeen = 0;

      for await (const batch of stream) {
        for (let i = 0; i < batch.rows.length; i++) {
          ringBuffer.push(batch.rows[i]!);
          totalRowsSeen++;
        }
      }

      if (ringBuffer.size > 0) {
        const rows = ringBuffer.toArray();
        yield {
          rows,
          offset: Math.max(0, totalRowsSeen - rows.length),
        };
      }
    })();
  };
}
