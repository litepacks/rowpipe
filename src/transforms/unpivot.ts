import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";

export interface UnpivotOptions {
  index: string | string[];
  columns?: string | string[];
  varCol?: string;
  valCol?: string;
  dropNull?: boolean;
  batchSize?: number;
}

/**
 * Creates a stream transform that reshapes tabular data from wide format to long format (melt/unpivot).
 */
export function unpivotTransform(options: UnpivotOptions): TransformFunction {
  const indexCols = Array.isArray(options.index)
    ? options.index
    : options.index.split(",").map((s) => s.trim()).filter(Boolean);

  if (indexCols.length === 0) {
    throw new InvalidArgumentError("Unpivot requires at least one index column via --index / -i");
  }

  const indexSet = new Set(indexCols);
  const targetCols = options.columns
    ? (Array.isArray(options.columns)
        ? options.columns
        : options.columns.split(",").map((s) => s.trim()).filter(Boolean))
    : undefined;

  const varCol = options.varCol || "variable";
  const valCol = options.valCol || "value";
  const dropNull = Boolean(options.dropNull);
  const batchSize = options.batchSize || 1000;

  return async function* (stream: DataStream): DataStream {
    let outRows: Row[] = [];
    let offset = 0;

    for await (const batch of stream) {
      for (const row of batch.rows) {
        const indexValues: Record<string, unknown> = {};
        for (const col of indexCols) {
          indexValues[col] = row[col] ?? null;
        }

        const colsToMelt = targetCols || Object.keys(row).filter((k) => !indexSet.has(k));

        for (const col of colsToMelt) {
          const val = row[col];
          if (dropNull && (val === null || val === undefined)) {
            continue;
          }

          const outRow: Row = {
            ...indexValues,
            [varCol]: col,
            [valCol]: val ?? null,
          };

          outRows.push(outRow);
          if (outRows.length >= batchSize) {
            yield { rows: outRows, offset };
            offset += outRows.length;
            outRows = [];
          }
        }
      }
    }

    if (outRows.length > 0) {
      yield { rows: outRows, offset };
    }
  };
}
