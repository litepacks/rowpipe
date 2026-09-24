import type { DataStream, Row } from "../core/types.js";
import { HyperLogLog } from "../analytics/stats.js";
import { encodeCompositeKey } from "../diff/key.js";
import { formatNumber } from "../utils/formatting.js";

export interface CountOptions {
  by?: string | string[];
  distinct?: string;
  approx?: boolean;
  json?: boolean;
}

export interface CountResult {
  totalRows: number;
  distinctCount?: number;
  isApproximate?: boolean;
  groups?: Array<{ key: Record<string, unknown>; count: number }>;
}

/**
 * Executes high-performance streaming count analysis over DataStream.
 */
export async function countStream(
  stream: DataStream,
  options: CountOptions = {}
): Promise<CountResult> {
  const byCols = options.by
    ? (Array.isArray(options.by) ? options.by : options.by.split(",").map((s) => s.trim()).filter(Boolean))
    : undefined;

  let totalRows = 0;
  const distinctCol = options.distinct;
  const useApprox = options.approx ?? false;

  const hll = distinctCol && useApprox ? new HyperLogLog(12) : null;
  const exactDistinctSet = distinctCol && !useApprox ? new Set<string>() : null;
  const groupCounts = byCols ? new Map<string, { key: Record<string, unknown>; count: number }>() : null;

  for await (const batch of stream) {
    totalRows += batch.rows.length;

    if (distinctCol) {
      for (let i = 0; i < batch.rows.length; i++) {
        const val = batch.rows[i]![distinctCol];
        if (val !== null && val !== undefined) {
          if (hll) {
            hll.add(val);
          } else if (exactDistinctSet) {
            exactDistinctSet.add(String(val));
          }
        }
      }
    }

    if (byCols && groupCounts) {
      for (let i = 0; i < batch.rows.length; i++) {
        const row = batch.rows[i]!;
        const { encoded, rawKey } = encodeCompositeKey(row, byCols);
        const existing = groupCounts.get(encoded);
        if (existing) {
          existing.count++;
        } else {
          groupCounts.set(encoded, { key: rawKey, count: 1 });
        }
      }
    }
  }

  let distinctCount: number | undefined;
  if (distinctCol) {
    if (hll) {
      distinctCount = hll.count();
    } else if (exactDistinctSet) {
      distinctCount = exactDistinctSet.size;
    }
  }

  let groups: Array<{ key: Record<string, unknown>; count: number }> | undefined;
  if (groupCounts) {
    groups = Array.from(groupCounts.values()).sort((a, b) => b.count - a.count);
  }

  return {
    totalRows,
    distinctCount,
    isApproximate: useApprox && Boolean(distinctCol),
    groups,
  };
}
