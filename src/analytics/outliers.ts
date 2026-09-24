import type { DataStream, Row, TransformFunction } from "../core/types.js";

export type OutlierMethod = "zscore" | "iqr" | "mad";

export interface OutlierOptions {
  column: string;
  method?: OutlierMethod;
  threshold?: number;
  onlyOutliers?: boolean;
  addColumns?: boolean;
  invert?: boolean;
}

export interface OutlierStats {
  column: string;
  method: OutlierMethod;
  threshold: number;
  totalRows: number;
  outlierCount: number;
  outlierPercent: number;
  lowerBound?: number;
  upperBound?: number;
}

/**
 * Streaming Outlier & Anomaly Detection Transform.
 */
export function outliersTransform(options: OutlierOptions): TransformFunction {
  const col = options.column;
  const method = options.method || "zscore";
  const threshold = options.threshold !== undefined
    ? options.threshold
    : method === "iqr"
      ? 1.5
      : 3.0;

  return async function* (input: DataStream): DataStream {
    // Pass 1: Collect numeric stats
    const allRows: Row[] = [];
    const values: number[] = [];
    let sum = 0;

    for await (const batch of input) {
      for (const row of batch.rows) {
        allRows.push(row);
        const rawVal = row[col];
        if (rawVal !== null && rawVal !== undefined && rawVal !== "") {
          const num = typeof rawVal === "number" ? rawVal : parseFloat(String(rawVal));
          if (Number.isFinite(num)) {
            values.push(num);
            sum += num;
          }
        }
      }
    }

    if (values.length === 0) {
      yield { rows: allRows, offset: 0 };
      return;
    }

    // Compute statistical boundaries
    let isOutlierFn: (val: number) => { isOutlier: boolean; score: number };

    if (method === "zscore") {
      const n = values.length;
      const mean = sum / n;
      const variance = values.reduce((acc, v) => acc + (v - mean) ** 2, 0) / (n > 1 ? n - 1 : 1);
      const stddev = Math.sqrt(variance);

      isOutlierFn = (v: number) => {
        if (stddev === 0) return { isOutlier: false, score: 0 };
        const z = (v - mean) / stddev;
        const score = parseFloat(z.toFixed(3));
        return { isOutlier: Math.abs(z) > threshold, score };
      };
    } else if (method === "iqr") {
      values.sort((a, b) => a - b);
      const n = values.length;
      const q1 = values[Math.floor(0.25 * (n - 1))]!;
      const q3 = values[Math.floor(0.75 * (n - 1))]!;
      const iqr = q3 - q1;
      const lower = q1 - threshold * iqr;
      const upper = q3 + threshold * iqr;

      isOutlierFn = (v: number) => {
        const isOut = v < lower || v > upper;
        const dist = v < lower ? lower - v : v > upper ? v - upper : 0;
        const score = iqr > 0 ? parseFloat((dist / iqr).toFixed(3)) : 0;
        return { isOutlier: isOut, score };
      };
    } else {
      // MAD (Median Absolute Deviation)
      values.sort((a, b) => a - b);
      const median = values[Math.floor(values.length / 2)]!;
      const absDevs = values.map((v) => Math.abs(v - median)).sort((a, b) => a - b);
      const mad = absDevs[Math.floor(absDevs.length / 2)] || 0.0001;

      isOutlierFn = (v: number) => {
        const modZ = (0.6745 * Math.abs(v - median)) / mad;
        const score = parseFloat(modZ.toFixed(3));
        return { isOutlier: modZ > threshold, score };
      };
    }

    // Pass 2: Filter or annotate rows
    const outRows: Row[] = [];

    for (const row of allRows) {
      const rawVal = row[col];
      const num = typeof rawVal === "number" ? rawVal : parseFloat(String(rawVal));
      const hasValidNum = Number.isFinite(num);

      const check = hasValidNum ? isOutlierFn(num) : { isOutlier: false, score: 0 };

      if (options.onlyOutliers) {
        if (check.isOutlier) {
          if (options.addColumns) {
            outRows.push({ ...row, _is_outlier: true, _outlier_score: check.score });
          } else {
            outRows.push(row);
          }
        }
      } else if (options.invert) {
        if (!check.isOutlier) {
          outRows.push(row);
        }
      } else {
        if (options.addColumns) {
          outRows.push({
            ...row,
            _is_outlier: check.isOutlier,
            _outlier_score: check.score,
          });
        } else {
          outRows.push(row);
        }
      }
    }

    yield { rows: outRows, offset: 0 };
  };
}
