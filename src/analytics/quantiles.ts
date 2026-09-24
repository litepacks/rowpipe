import type { DataStream } from "../core/types.js";

export interface QuantilesResult {
  column: string;
  count: number;
  min: number;
  max: number;
  mean: number;
  median: number;
  q1: number;
  q3: number;
  iqr: number;
  percentiles: Record<string, number>;
}

/**
 * Computes exact/interpolated percentiles from numeric stream.
 */
export async function computeQuantiles(
  stream: DataStream,
  column: string,
  requestedPercentiles: number[] = [25, 50, 75, 90, 95, 99]
): Promise<QuantilesResult> {
  const values: number[] = [];
  let sum = 0;

  for await (const batch of stream) {
    for (const row of batch.rows) {
      const rawVal = row[column];
      if (rawVal === null || rawVal === undefined || rawVal === "") continue;

      const num = typeof rawVal === "number" ? rawVal : parseFloat(String(rawVal));
      if (Number.isFinite(num)) {
        values.push(num);
        sum += num;
      }
    }
  }

  if (values.length === 0) {
    throw new Error(`No numeric values found in column: "${column}"`);
  }

  // Sort values
  values.sort((a, b) => a - b);
  const n = values.length;
  const mean = sum / n;
  const min = values[0]!;
  const max = values[n - 1]!;

  function getPercentile(p: number): number {
    if (p <= 0) return min;
    if (p >= 100) return max;
    const rank = (p / 100) * (n - 1);
    const low = Math.floor(rank);
    const high = Math.ceil(rank);
    const weight = rank - low;
    return values[low]! * (1 - weight) + values[high]! * weight;
  }

  const q1 = getPercentile(25);
  const median = getPercentile(50);
  const q3 = getPercentile(75);
  const iqr = q3 - q1;

  const percentiles: Record<string, number> = {};
  const allPs = Array.from(new Set([...requestedPercentiles, 25, 50, 75])).sort((a, b) => a - b);
  for (const p of allPs) {
    const key = `p${p}`;
    percentiles[key] = parseFloat(getPercentile(p).toFixed(4));
  }

  return {
    column,
    count: n,
    min,
    max,
    mean: parseFloat(mean.toFixed(4)),
    median: parseFloat(median.toFixed(4)),
    q1: parseFloat(q1.toFixed(4)),
    q3: parseFloat(q3.toFixed(4)),
    iqr: parseFloat(iqr.toFixed(4)),
    percentiles,
  };
}
