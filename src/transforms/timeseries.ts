import type { DataStream, Row, TransformFunction } from "../core/types.js";

export type GapFillMethod = "ffill" | "bfill" | "zero" | "null" | "linear";

export interface TimeseriesAggSpec {
  fn: "avg" | "sum" | "min" | "max" | "first" | "last" | "count";
  column: string;
  alias?: string;
}

export interface TimeseriesOptions {
  timeCol: string;
  interval?: string;
  aggregations?: TimeseriesAggSpec[];
  fillGaps?: GapFillMethod;
  timeFormat?: "iso" | "epoch_ms" | "epoch_s" | "date_only";
}

/**
 * Parses time interval strings like '5s', '1m', '15m', '1h', '1d', '1w', '1mo', '1y' into milliseconds or interval units.
 */
export function parseTimeInterval(intervalStr = "1m"): { ms: number; unit?: "mo" | "y" } {
  const clean = intervalStr.trim().toLowerCase();
  const match = clean.match(/^(\d+)?\s*(s|sec|seconds?|m|min|minutes?|h|hr|hours?|d|days?|w|weeks?|mo|months?|y|yrs?|years?)$/);

  if (!match) {
    throw new Error(
      `Invalid time interval: "${intervalStr}". Valid examples: 10s, 1m, 5m, 1h, 1d, 1w, 1mo, 1y`
    );
  }

  const num = match[1] ? parseInt(match[1], 10) : 1;
  const unit = match[2]!;

  if (unit.startsWith("s")) return { ms: num * 1000 };
  if (unit.startsWith("m") && !unit.startsWith("mo")) return { ms: num * 60 * 1000 };
  if (unit.startsWith("h")) return { ms: num * 60 * 60 * 1000 };
  if (unit.startsWith("d")) return { ms: num * 24 * 60 * 60 * 1000 };
  if (unit.startsWith("w")) return { ms: num * 7 * 24 * 60 * 60 * 1000 };
  if (unit.startsWith("mo")) return { ms: num * 30 * 24 * 60 * 60 * 1000, unit: "mo" };
  if (unit.startsWith("y")) return { ms: num * 365 * 24 * 60 * 60 * 1000, unit: "y" };

  return { ms: num * 60 * 1000 };
}

/**
 * Robust timestamp parser supporting ISO-8601 strings, UNIX epoch seconds/ms, and Date representations.
 */
export function parseTimestamp(val: any): number | null {
  if (val === null || val === undefined || val === "") return null;
  if (val instanceof Date) return val.getTime();

  if (typeof val === "number") {
    // If < 1e11 (around year 1973 in ms), assume unix seconds
    return val < 100_000_000_000 ? Math.floor(val * 1000) : Math.floor(val);
  }

  const str = String(val).trim();
  const numericVal = Number(str);
  if (!Number.isNaN(numericVal) && Number.isFinite(numericVal)) {
    // Check if it looks like a year only (e.g. 2021)
    if (numericVal >= 1900 && numericVal <= 2100 && str.length === 4) {
      return Date.UTC(numericVal, 0, 1);
    }
    return numericVal < 100_000_000_000 ? Math.floor(numericVal * 1000) : Math.floor(numericVal);
  }

  const parsed = Date.parse(str);
  if (!Number.isNaN(parsed)) {
    return parsed;
  }

  return null;
}

/**
 * Floors a timestamp to the closest time bucket boundary.
 */
export function floorToBucket(ts: number, parsedInterval: { ms: number; unit?: "mo" | "y" }): number {
  if (parsedInterval.unit === "y") {
    const d = new Date(ts);
    return Date.UTC(d.getUTCFullYear(), 0, 1);
  }
  if (parsedInterval.unit === "mo") {
    const d = new Date(ts);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
  }
  return Math.floor(ts / parsedInterval.ms) * parsedInterval.ms;
}

/**
 * Formats a millisecond timestamp for output.
 */
export function formatTimestamp(ts: number, format: "iso" | "epoch_ms" | "epoch_s" | "date_only" = "iso"): string | number {
  if (format === "epoch_ms") return ts;
  if (format === "epoch_s") return Math.floor(ts / 1000);
  const iso = new Date(ts).toISOString();
  if (format === "date_only") return iso.split("T")[0]!;
  return iso;
}

export function parseTimeseriesAggSpecs(specs: string[]): TimeseriesAggSpec[] {
  const result: TimeseriesAggSpec[] = [];
  for (const spec of specs) {
    const match = spec.trim().match(/^(\w+)\(([^)]+)\)(?:\s*(?:as|=)\s*(\w+))?$/i);
    if (match) {
      const fn = match[1]!.toLowerCase() as TimeseriesAggSpec["fn"];
      const col = match[2]!.trim();
      const alias = match[3] || `${fn}_${col}`;
      result.push({ fn, column: col, alias });
    } else {
      // Default avg
      result.push({ fn: "avg", column: spec.trim(), alias: `avg_${spec.trim()}` });
    }
  }
  return result;
}

interface BucketAccumulator {
  timestamp: number;
  count: number;
  sums: Record<string, number>;
  mins: Record<string, number>;
  maxs: Record<string, number>;
  counts: Record<string, number>;
  firsts: Record<string, any>;
  lasts: Record<string, any>;
}

/**
 * Streaming timeseries resampler and gap filler transform.
 */
export function resampleTimeseries(options: TimeseriesOptions): TransformFunction {
  const interval = parseTimeInterval(options.interval || "1m");
  const timeCol = options.timeCol;
  const aggs = options.aggregations && options.aggregations.length > 0
    ? options.aggregations
    : [{ fn: "count", column: "*", alias: "count" } as TimeseriesAggSpec];

  return async function* (input: DataStream): DataStream {
    const buckets = new Map<number, BucketAccumulator>();

    for await (const batch of input) {
      for (const row of batch.rows) {
        const rawTime = row[timeCol];
        const ts = parseTimestamp(rawTime);
        if (ts === null) continue;

        const bucketTs = floorToBucket(ts, interval);
        let acc = buckets.get(bucketTs);
        if (!acc) {
          acc = {
            timestamp: bucketTs,
            count: 0,
            sums: {},
            mins: {},
            maxs: {},
            counts: {},
            firsts: {},
            lasts: {},
          };
          buckets.set(bucketTs, acc);
        }

        acc.count++;

        for (const agg of aggs) {
          const col = agg.column;
          if (col === "*") continue;

          const val = row[col];
          const num = typeof val === "number" ? val : parseFloat(String(val));
          const isValidNum = Number.isFinite(num);

          if (!acc.firsts.hasOwnProperty(col)) {
            acc.firsts[col] = val;
          }
          acc.lasts[col] = val;

          if (isValidNum) {
            acc.sums[col] = (acc.sums[col] || 0) + num;
            acc.counts[col] = (acc.counts[col] || 0) + 1;
            acc.mins[col] = acc.mins[col] !== undefined ? Math.min(acc.mins[col], num) : num;
            acc.maxs[col] = acc.maxs[col] !== undefined ? Math.max(acc.maxs[col], num) : num;
          }
        }
      }
    }

    // Sort buckets chronologically
    const sortedBuckets = Array.from(buckets.keys()).sort((a, b) => a - b);
    if (sortedBuckets.length === 0) return;

    const minTs = sortedBuckets[0]!;
    const maxTs = sortedBuckets[sortedBuckets.length - 1]!;

    // Build bucket rows
    const rows: Row[] = [];

    function makeRowFromAcc(acc: BucketAccumulator, ts: number): Row {
      const outRow: Row = {
        [timeCol]: formatTimestamp(ts, options.timeFormat || "iso"),
      };

      for (const agg of aggs) {
        const col = agg.column;
        const alias = agg.alias || `${agg.fn}_${col}`;

        if (agg.fn === "count") {
          outRow[alias] = col === "*" ? acc.count : (acc.counts[col] || 0);
        } else if (agg.fn === "sum") {
          outRow[alias] = acc.sums[col] ?? 0;
        } else if (agg.fn === "avg") {
          const count = acc.counts[col] || 0;
          outRow[alias] = count > 0 ? (acc.sums[col] || 0) / count : null;
        } else if (agg.fn === "min") {
          outRow[alias] = acc.mins[col] ?? null;
        } else if (agg.fn === "max") {
          outRow[alias] = acc.maxs[col] ?? null;
        } else if (agg.fn === "first") {
          outRow[alias] = acc.firsts[col] ?? null;
        } else if (agg.fn === "last") {
          outRow[alias] = acc.lasts[col] ?? null;
        }
      }
      return outRow;
    }

    if (options.fillGaps) {
      // Gap filling between minTs and maxTs
      let currentTs = minTs;
      let lastFilledRow: Row | null = null;

      while (currentTs <= maxTs) {
        const existingAcc = buckets.get(currentTs);
        if (existingAcc) {
          const row = makeRowFromAcc(existingAcc, currentTs);
          rows.push(row);
          lastFilledRow = row;
        } else {
          // Fill gap
          const emptyRow: Row = {
            [timeCol]: formatTimestamp(currentTs, options.timeFormat || "iso"),
          };

          for (const agg of aggs) {
            const alias = agg.alias || `${agg.fn}_${agg.column}`;
            if (options.fillGaps === "zero") {
              emptyRow[alias] = 0;
            } else if (options.fillGaps === "ffill") {
              emptyRow[alias] = lastFilledRow ? lastFilledRow[alias] : null;
            } else {
              emptyRow[alias] = null;
            }
          }
          rows.push(emptyRow);
        }

        // Advance currentTs
        if (interval.unit === "y") {
          const d = new Date(currentTs);
          d.setUTCFullYear(d.getUTCFullYear() + 1);
          currentTs = d.getTime();
        } else if (interval.unit === "mo") {
          const d = new Date(currentTs);
          d.setUTCMonth(d.getUTCMonth() + 1);
          currentTs = d.getTime();
        } else {
          currentTs += interval.ms;
        }
      }

      // Linear interpolation if requested
      if (options.fillGaps === "linear") {
        for (const agg of aggs) {
          const alias = agg.alias || `${agg.fn}_${agg.column}`;
          let i = 0;
          while (i < rows.length) {
            if (rows[i]![alias] === null) {
              const startIdx = i - 1;
              let endIdx = i;
              while (endIdx < rows.length && rows[endIdx]![alias] === null) {
                endIdx++;
              }
              if (startIdx >= 0 && endIdx < rows.length) {
                const startVal = Number(rows[startIdx]![alias]);
                const endVal = Number(rows[endIdx]![alias]);
                const step = (endVal - startVal) / (endIdx - startIdx);
                for (let k = startIdx + 1; k < endIdx; k++) {
                  rows[k]![alias] = startVal + step * (k - startIdx);
                }
              }
              i = endIdx;
            } else {
              i++;
            }
          }
        }
      }
    } else {
      // No gap filling: emit only existing buckets
      for (const ts of sortedBuckets) {
        const acc = buckets.get(ts)!;
        rows.push(makeRowFromAcc(acc, ts));
      }
    }

    yield { rows, offset: 0 };
  };
}
