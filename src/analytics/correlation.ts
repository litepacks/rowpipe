import type { DataStream, Row } from "../core/types.js";
import { formatTable } from "../writers/table.js";

export interface CorrelationMatrixResult {
  columns: string[];
  matrix: Record<string, Record<string, number>>;
  sampleSize: number;
}

/**
 * Computes Pearson correlation matrix across numeric columns in a single streaming pass.
 */
export async function computeCorrelationMatrix(
  stream: DataStream,
  targetColumns?: string[]
): Promise<CorrelationMatrixResult> {
  let cols: string[] = targetColumns ? [...targetColumns] : [];
  let colsDiscovered = Boolean(targetColumns && targetColumns.length > 0);

  let n = 0;
  const means: Record<string, number> = {};
  const m2: Record<string, number> = {};
  const coMoments: Record<string, Record<string, number>> = {};

  function initCols(discoveredCols: string[]) {
    cols = discoveredCols;
    for (const c of cols) {
      means[c] = 0;
      m2[c] = 0;
      coMoments[c] = {};
      for (const c2 of cols) {
        coMoments[c]![c2] = 0;
      }
    }
    colsDiscovered = true;
  }

  if (colsDiscovered) {
    initCols(cols);
  }

  for await (const batch of stream) {
    for (const row of batch.rows) {
      if (!colsDiscovered) {
        const numericCols: string[] = [];
        for (const [k, v] of Object.entries(row)) {
          if (typeof v === "number" && Number.isFinite(v)) {
            numericCols.push(k);
          } else if (v !== null && v !== undefined && v !== "") {
            const parsed = Number(v);
            if (!Number.isNaN(parsed) && Number.isFinite(parsed)) {
              numericCols.push(k);
            }
          }
        }
        if (numericCols.length > 0) {
          initCols(numericCols);
        }
      }

      if (cols.length === 0) continue;

      // Extract numeric values for this row
      const rowVals: Record<string, number> = {};
      let rowValid = true;
      for (const c of cols) {
        const val = row[c];
        const num = typeof val === "number" ? val : parseFloat(String(val));
        if (!Number.isFinite(num)) {
          rowValid = false;
          break;
        }
        rowVals[c] = num;
      }

      if (!rowValid) continue;

      n++;
      const deltaX: Record<string, number> = {};

      for (const c of cols) {
        const x = rowVals[c]!;
        deltaX[c] = x - means[c]!;
        means[c] = means[c]! + deltaX[c]! / n;
        m2[c] = m2[c]! + deltaX[c]! * (x - means[c]!);
      }

      for (let i = 0; i < cols.length; i++) {
        const c1 = cols[i]!;
        for (let j = 0; j < cols.length; j++) {
          const c2 = cols[j]!;
          coMoments[c1]![c2] = (coMoments[c1]![c2] || 0) + deltaX[c1]! * (rowVals[c2]! - means[c2]!);
        }
      }
    }
  }

  const resultMatrix: Record<string, Record<string, number>> = {};
  for (const c1 of cols) {
    resultMatrix[c1] = {};
    for (const c2 of cols) {
      if (n < 2) {
        resultMatrix[c1]![c2] = c1 === c2 ? 1.0 : 0.0;
        continue;
      }

      const var1 = m2[c1]! / (n - 1);
      const var2 = m2[c2]! / (n - 1);
      const cov = coMoments[c1]![c2]! / (n - 1);

      if (var1 <= 0 || var2 <= 0) {
        resultMatrix[c1]![c2] = c1 === c2 ? 1.0 : 0.0;
      } else {
        const r = cov / (Math.sqrt(var1) * Math.sqrt(var2));
        resultMatrix[c1]![c2] = Math.max(-1, Math.min(1, parseFloat(r.toFixed(4))));
      }
    }
  }

  return {
    columns: cols,
    matrix: resultMatrix,
    sampleSize: n,
  };
}

/**
 * Converts a correlation matrix result into tabular Rows suitable for table or CSV export.
 */
export function formatCorrelationRows(result: CorrelationMatrixResult): Row[] {
  return result.columns.map((c1) => {
    const rowObj: Row = { column: c1 };
    for (const c2 of result.columns) {
      rowObj[c2] = result.matrix[c1]?.[c2] ?? 0;
    }
    return rowObj;
  });
}
