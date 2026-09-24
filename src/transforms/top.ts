import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { Heap } from "../utils/heap.js";
import {
  createRawRowComparator,
  parseSortSpecs,
  SortKeySpec,
} from "./sort/comparator.js";

export interface TopOptions {
  by: string | string[] | SortKeySpec[];
  count?: number;
  order?: "asc" | "desc";
  smallest?: boolean;
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
}

/**
 * Retains only the Top-K (or Bottom-K) rows using a bounded O(K) Binary Heap.
 * Time complexity: O(N * log K), Memory complexity: O(K).
 */
export function topRows(options: TopOptions): TransformFunction {
  const k = Math.max(1, options.count ?? 10);
  const isSmallest = options.smallest || options.order === "asc";
  const defaultDirection: "asc" | "desc" = isSmallest ? "asc" : "desc";

  const rawSpecs = parseSortSpecs(options.by, {
    defaultDirection,
    nulls: options.nulls,
    ignoreCase: options.ignoreCase,
    natural: options.natural,
  });

  // Desired output comparator: desiredComparator(a, b) < 0 means 'a' comes before 'b'
  const desiredComparator = createRawRowComparator(rawSpecs);

  return (stream: DataStream): DataStream => {
    return (async function* () {
      // Min-Heap where root is the LEAST preferred element among current top K
      // If desiredComparator(a, b) > 0 ('a' is worse than 'b'), then heapComparator(a, b) < 0 (floats to root)
      const heapComparator = (a: Row, b: Row) => -desiredComparator(a, b);
      const heap = new Heap<Row>(heapComparator);

      for await (const batch of stream) {
        for (let i = 0; i < batch.rows.length; i++) {
          const row = batch.rows[i]!;

          if (heap.size < k) {
            heap.push(row);
          } else {
            const worst = heap.peek()!;
            // If incoming row is more preferred than the current worst, replace it
            if (desiredComparator(row, worst) < 0) {
              heap.replace(row);
            }
          }
        }
      }

      // Drain and sort results in exact desired comparator order
      const drained = heap.drain();
      drained.sort(desiredComparator);

      if (drained.length > 0) {
        yield {
          rows: drained,
          offset: 0,
        };
      }
    })();
  };
}
