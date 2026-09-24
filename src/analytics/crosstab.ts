import type { DataStream, Row } from "../core/types.js";

export interface CrosstabResult {
  rowCol: string;
  colCol: string;
  rowCategories: string[];
  colCategories: string[];
  counts: Record<string, Record<string, number>>;
  rowTotals: Record<string, number>;
  colTotals: Record<string, number>;
  grandTotal: number;
  chiSquare: number;
  degreesOfFreedom: number;
}

export interface CrosstabOptions {
  topRows?: number;
  topCols?: number;
  normalize?: "row" | "col" | "all";
}

/**
 * Computes 2D contingency matrix (crosstab) and Chi-Square statistic.
 */
export async function computeCrosstab(
  stream: DataStream,
  rowCol: string,
  colCol: string,
  options: CrosstabOptions = {}
): Promise<CrosstabResult> {
  const counts: Record<string, Record<string, number>> = {};
  const rowTotals: Record<string, number> = {};
  const colTotals: Record<string, number> = {};
  let grandTotal = 0;

  for await (const batch of stream) {
    for (const row of batch.rows) {
      const rVal = String(row[rowCol] ?? "<null>");
      const cVal = String(row[colCol] ?? "<null>");

      if (!counts[rVal]) counts[rVal] = {};
      counts[rVal]![cVal] = (counts[rVal]![cVal] || 0) + 1;

      rowTotals[rVal] = (rowTotals[rVal] || 0) + 1;
      colTotals[cVal] = (colTotals[cVal] || 0) + 1;
      grandTotal++;
    }
  }

  // Sort and limit categories
  const sortedRowCats = Object.keys(rowTotals).sort((a, b) => rowTotals[b]! - rowTotals[a]!);
  const sortedColCats = Object.keys(colTotals).sort((a, b) => colTotals[b]! - colTotals[a]!);

  const rowCategories = options.topRows ? sortedRowCats.slice(0, options.topRows) : sortedRowCats;
  const colCategories = options.topCols ? sortedColCats.slice(0, options.topCols) : sortedColCats;

  // Compute Chi-Square
  let chiSquare = 0;
  if (grandTotal > 0 && rowCategories.length > 1 && colCategories.length > 1) {
    for (const r of rowCategories) {
      for (const c of colCategories) {
        const observed = counts[r]?.[c] || 0;
        const expected = ((rowTotals[r] || 0) * (colTotals[c] || 0)) / grandTotal;
        if (expected > 0) {
          chiSquare += ((observed - expected) ** 2) / expected;
        }
      }
    }
  }

  const degreesOfFreedom = Math.max(0, (rowCategories.length - 1) * (colCategories.length - 1));

  return {
    rowCol,
    colCol,
    rowCategories,
    colCategories,
    counts,
    rowTotals,
    colTotals,
    grandTotal,
    chiSquare: parseFloat(chiSquare.toFixed(4)),
    degreesOfFreedom,
  };
}

/**
 * Formats a CrosstabResult as tabular rows for table / CSV rendering.
 */
export function formatCrosstabRows(result: CrosstabResult, normalize?: "row" | "col" | "all"): Row[] {
  const rows: Row[] = [];

  for (const r of result.rowCategories) {
    const rowObj: Row = { [result.rowCol]: r };
    const rTotal = result.rowTotals[r] || 0;

    for (const c of result.colCategories) {
      const count = result.counts[r]?.[c] || 0;
      if (normalize === "row") {
        rowObj[c] = rTotal > 0 ? `${((count / rTotal) * 100).toFixed(1)}%` : "0.0%";
      } else if (normalize === "col") {
        const cTotal = result.colTotals[c] || 0;
        rowObj[c] = cTotal > 0 ? `${((count / cTotal) * 100).toFixed(1)}%` : "0.0%";
      } else if (normalize === "all") {
        rowObj[c] = result.grandTotal > 0 ? `${((count / result.grandTotal) * 100).toFixed(1)}%` : "0.0%";
      } else {
        rowObj[c] = count;
      }
    }

    rowObj["Total"] = normalize === "row" ? "100.0%" : rTotal;
    rows.push(rowObj);
  }

  // Add summary total row
  const totalRow: Row = { [result.rowCol]: "Total" };
  for (const c of result.colCategories) {
    const cTotal = result.colTotals[c] || 0;
    totalRow[c] = normalize === "col" ? "100.0%" : cTotal;
  }
  totalRow["Total"] = result.grandTotal;
  rows.push(totalRow);

  return rows;
}
