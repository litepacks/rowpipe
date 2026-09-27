import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { Heap } from "../utils/heap.js";
import {
  compareKeys,
  extractSortKey,
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

interface TopEntry {
  row: Row;
  seq: number;
  k0?: unknown;
  k1?: unknown;
  keys?: unknown[];
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

  const numSpecs = rawSpecs.length;
  const spec0 = rawSpecs[0]!;
  const col0 = spec0.column;
  let spec1: SortKeySpec | undefined;
  let col1: string | undefined;
  if (numSpecs > 1) {
    spec1 = rawSpecs[1]!;
    col1 = spec1.column;
  }

  function extractTopEntry(row: Row, seq: number): TopEntry {
    if (numSpecs === 1) {
      return { row, seq, k0: extractSortKey(row[col0], spec0) };
    }
    if (numSpecs === 2) {
      return {
        row,
        seq,
        k0: extractSortKey(row[col0], spec0),
        k1: extractSortKey(row[col1!], spec1!),
      };
    }
    const keys = new Array(numSpecs);
    for (let s = 0; s < numSpecs; s++) {
      keys[s] = extractSortKey(row[rawSpecs[s]!.column], rawSpecs[s]!);
    }
    return { row, seq, keys };
  }

  function compareTopEntries(a: TopEntry, b: TopEntry): number {
    if (numSpecs === 1) {
      const cmp = compareKeys(a.k0, b.k0, spec0);
      return spec0.direction === "desc" ? -cmp : cmp;
    }
    if (numSpecs === 2) {
      let cmp = compareKeys(a.k0, b.k0, spec0);
      if (cmp !== 0) return spec0.direction === "desc" ? -cmp : cmp;
      cmp = compareKeys(a.k1, b.k1, spec1!);
      return spec1!.direction === "desc" ? -cmp : cmp;
    }
    const keysA = a.keys!;
    const keysB = b.keys!;
    for (let i = 0; i < numSpecs; i++) {
      const spec = rawSpecs[i]!;
      const cmp = compareKeys(keysA[i], keysB[i], spec);
      if (cmp !== 0) return spec.direction === "desc" ? -cmp : cmp;
    }
    return 0;
  }

  return (stream: DataStream): DataStream => {
    return (async function* () {
      // Min-Heap where root is the LEAST preferred element among current top K
      // If compareTopEntries(a, b) > 0 ('a' is worse than 'b'), then heapComparator(a, b) < 0 (floats to root)
      const heapComparator = (a: TopEntry, b: TopEntry) => {
        const cmp = -compareTopEntries(a, b);
        return cmp !== 0 ? cmp : a.seq - b.seq;
      };
      const heap = new Heap<TopEntry>(heapComparator);

      let worst: TopEntry | undefined;
      let seq = 0;

      for await (const batch of stream) {
        const rows = batch.rows;
        const len = rows.length;

        for (let i = 0; i < len; i++) {
          const row = rows[i]!;
          const currentSeq = seq++;

          if (heap.size < k) {
            heap.push(extractTopEntry(row, currentSeq));
            if (heap.size === k) {
              worst = heap.peek();
            }
            continue;
          }

          // Heap is full (size === k). Filter out non-qualifying rows with zero object allocations.
          if (numSpecs === 1) {
            const k0 = extractSortKey(row[col0], spec0);
            const cmp = compareKeys(k0, worst!.k0, spec0);
            const diff = spec0.direction === "desc" ? -cmp : cmp;
            if (diff >= 0) {
              continue; // Worse or equal to current worst; reject with zero allocations
            }
            heap.replace({ row, seq: currentSeq, k0 });
            worst = heap.peek();
          } else if (numSpecs === 2) {
            const k0 = extractSortKey(row[col0], spec0);
            let cmp = compareKeys(k0, worst!.k0, spec0);
            let diff = spec0.direction === "desc" ? -cmp : cmp;
            if (diff > 0) {
              continue; // Strictly worse on primary key; reject without touching col1
            }
            if (diff === 0) {
              // Primary key ties with worst; check secondary key
              const k1 = extractSortKey(row[col1!], spec1!);
              cmp = compareKeys(k1, worst!.k1, spec1!);
              diff = spec1!.direction === "desc" ? -cmp : cmp;
              if (diff >= 0) {
                continue; // Secondary key is worse or equal; reject
              }
              heap.replace({ row, seq: currentSeq, k0, k1 });
              worst = heap.peek();
              continue;
            }
            // diff < 0: strictly better on primary key
            const k1 = extractSortKey(row[col1!], spec1!);
            heap.replace({ row, seq: currentSeq, k0, k1 });
            worst = heap.peek();
          } else {
            // General N specs (numSpecs > 2)
            const k0 = extractSortKey(row[col0], spec0);
            const worstK0 = worst!.keys ? worst!.keys[0] : worst!.k0;
            const cmp = compareKeys(k0, worstK0, spec0);
            const diff = spec0.direction === "desc" ? -cmp : cmp;
            if (diff > 0) {
              continue; // Strictly worse on primary key; reject
            }
            const entry = extractTopEntry(row, currentSeq);
            if (compareTopEntries(entry, worst!) < 0) {
              heap.replace(entry);
              worst = heap.peek();
            }
          }
        }
      }

      // Drain and sort results in exact desired comparator order (with stable tie-breaking)
      const drained = heap.drain();
      drained.sort((a, b) => {
        const cmp = compareTopEntries(a, b);
        return cmp !== 0 ? cmp : a.seq - b.seq;
      });

      if (drained.length > 0) {
        yield {
          rows: drained.map((e) => e.row),
          offset: 0,
        };
      }
    })();
  };
}
