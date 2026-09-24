import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";
import { encodeCompositeKey } from "../diff/key.js";

export type PivotAggregator = "sum" | "avg" | "mean" | "min" | "max" | "count" | "first" | "last";

export interface PivotOptions {
  index: string | string[];
  columns: string;
  values?: string;
  agg?: PivotAggregator;
  fill?: unknown;
  batchSize?: number;
  sortColumns?: boolean;
}

interface CellAccumulator {
  add(val: unknown): void;
  value(): unknown;
}

function createCellAccumulator(agg: PivotAggregator): CellAccumulator {
  switch (agg) {
    case "sum": {
      let sum = 0;
      let hasVal = false;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined && val !== "") {
            const num = Number(val);
            if (!Number.isNaN(num)) {
              sum += num;
              hasVal = true;
            }
          }
        },
        value() {
          return hasVal ? (Number.isInteger(sum) ? sum : Math.round(sum * 1e6) / 1e6) : 0;
        },
      };
    }
    case "avg":
    case "mean": {
      let sum = 0;
      let count = 0;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined && val !== "") {
            const num = Number(val);
            if (!Number.isNaN(num)) {
              sum += num;
              count++;
            }
          }
        },
        value() {
          return count > 0 ? Math.round((sum / count) * 1e6) / 1e6 : null;
        },
      };
    }
    case "min": {
      let min: number | null = null;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined && val !== "") {
            const num = Number(val);
            if (!Number.isNaN(num)) {
              min = min === null ? num : Math.min(min, num);
            }
          }
        },
        value() {
          return min;
        },
      };
    }
    case "max": {
      let max: number | null = null;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined && val !== "") {
            const num = Number(val);
            if (!Number.isNaN(num)) {
              max = max === null ? num : Math.max(max, num);
            }
          }
        },
        value() {
          return max;
        },
      };
    }
    case "count": {
      let count = 0;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined) {
            count++;
          }
        },
        value() {
          return count;
        },
      };
    }
    case "first": {
      let firstVal: unknown = undefined;
      return {
        add(val: unknown) {
          if (firstVal === undefined && val !== null && val !== undefined) {
            firstVal = val;
          }
        },
        value() {
          return firstVal !== undefined ? firstVal : null;
        },
      };
    }
    case "last": {
      let lastVal: unknown = null;
      return {
        add(val: unknown) {
          if (val !== null && val !== undefined) {
            lastVal = val;
          }
        },
        value() {
          return lastVal;
        },
      };
    }
    default:
      throw new InvalidArgumentError(`Unsupported pivot aggregator: ${agg}`);
  }
}

/**
 * Creates a stream transform that reshapes tabular data into a pivot table.
 */
export function pivotTransform(options: PivotOptions): TransformFunction {
  const indexCols = Array.isArray(options.index)
    ? options.index
    : options.index.split(",").map((s) => s.trim()).filter(Boolean);

  if (indexCols.length === 0) {
    throw new InvalidArgumentError("Pivot requires at least one index column via --index / -i");
  }

  const pivotCol = options.columns?.trim();
  if (!pivotCol) {
    throw new InvalidArgumentError("Pivot requires a pivot column via --columns / -c");
  }

  const valCol = options.values?.trim();
  const aggType: PivotAggregator = options.agg || "sum";
  const defaultFill = options.fill !== undefined ? options.fill : (aggType === "sum" || aggType === "count" ? 0 : null);
  const batchSize = options.batchSize || 1000;
  const sortColumns = options.sortColumns !== false;

  return async function* (stream: DataStream): DataStream {
    const pivotColSet = new Set<string>();
    const groupMap = new Map<
      string,
      {
        indexValues: Record<string, unknown>;
        cells: Map<string, CellAccumulator>;
      }
    >();

    for await (const batch of stream) {
      for (const row of batch.rows) {
        const key = encodeCompositeKey(row, indexCols).encoded;

        let group = groupMap.get(key);
        if (!group) {
          const indexValues: Record<string, unknown> = {};
          for (const col of indexCols) {
            indexValues[col] = row[col] ?? null;
          }
          group = { indexValues, cells: new Map() };
          groupMap.set(key, group);
        }

        const rawColVal = row[pivotCol];
        const colHeader = rawColVal !== null && rawColVal !== undefined ? String(rawColVal) : "null";
        pivotColSet.add(colHeader);

        let cell = group.cells.get(colHeader);
        if (!cell) {
          cell = createCellAccumulator(aggType);
          group.cells.set(colHeader, cell);
        }

        const cellVal = valCol ? row[valCol] : 1;
        cell.add(cellVal);
      }
    }

    const colHeaders = Array.from(pivotColSet);
    if (sortColumns) {
      colHeaders.sort((a, b) => {
        const numA = Number(a);
        const numB = Number(b);
        if (!Number.isNaN(numA) && !Number.isNaN(numB)) {
          return numA - numB;
        }
        return a.localeCompare(b);
      });
    }

    let outRows: Row[] = [];
    let offset = 0;

    for (const group of groupMap.values()) {
      const outRow: Row = { ...group.indexValues };
      for (const header of colHeaders) {
        const cell = group.cells.get(header);
        outRow[header] = cell ? cell.value() : defaultFill;
      }

      outRows.push(outRow);
      if (outRows.length >= batchSize) {
        yield { rows: outRows, offset };
        offset += outRows.length;
        outRows = [];
      }
    }

    if (outRows.length > 0) {
      yield { rows: outRows, offset };
    }
  };
}
