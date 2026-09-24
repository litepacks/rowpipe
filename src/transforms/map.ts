import { InvalidArgumentError } from "../core/errors.js";
import { RowErrorHandler } from "../core/error-handler.js";
import type { DataBatch, DataStream, ErrorHandlingStrategy, Row, TransformFunction } from "../core/types.js";
import { compileValueExpression } from "./expression.js";

/**
 * Parses CLI map argument specs into a column-to-expression dictionary.
 * Supports formats: "profit = revenue - cost", "tax=revenue*0.2"
 */
export function parseMapSpecs(specs: string[]): Record<string, string> {
  const result: Record<string, string> = {};

  for (const spec of specs) {
    const trimmed = spec.trim();
    if (!trimmed) continue;

    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) {
      throw new InvalidArgumentError(
        `Invalid map specification "${spec}". Expected format "column_name = expression"`
      );
    }

    const colName = trimmed.slice(0, eqIdx).trim();
    const expr = trimmed.slice(eqIdx + 1).trim();

    if (!colName) {
      throw new InvalidArgumentError(
        `Missing target column name in map specification "${spec}"`
      );
    }
    if (!expr) {
      throw new InvalidArgumentError(
        `Missing expression in map specification "${spec}"`
      );
    }

    result[colName] = expr;
  }

  return result;
}

export type RowMapper = (row: Row) => Row;

export interface MapOptions {
  onError?: ErrorHandlingStrategy;
  badRowsLog?: string;
}

/**
 * Creates a high-performance streaming transform that computes/derives columns per row.
 */
export function mapRows(
  specs: Record<string, string> | RowMapper,
  options: MapOptions = {}
): TransformFunction {
  const errorHandler = new RowErrorHandler({
    strategy: options.onError,
    badRowsLog: options.badRowsLog,
  });

  if (typeof specs === "function") {
    const customMapper = specs;
    return async function* (stream: DataStream): DataStream {
      try {
        for await (const batch of stream) {
          const len = batch.rows.length;
          const mappedRows: Row[] = [];
          for (let i = 0; i < len; i++) {
            const src = batch.rows[i]!;
            try {
              mappedRows.push(customMapper(src));
            } catch (err) {
              errorHandler.handle(err as Error, {
                row: batch.offset + i + 1,
                raw: src,
              });
            }
          }
          yield {
            rows: mappedRows,
            offset: batch.offset,
          };
        }
      } finally {
        errorHandler.close();
      }
    };
  }

  // Pre-compile all AST expression evaluators once before processing stream
  const compiledEntries = Object.entries(specs).map(([targetCol, exprStr]) => ({
    targetCol,
    evaluator: compileValueExpression(exprStr),
  }));

  return async function* (stream: DataStream): DataStream {
    try {
      for await (const batch of stream) {
        const rowCount = batch.rows.length;
        const mappedRows: Row[] = [];
        const compiledLen = compiledEntries.length;

        for (let i = 0; i < rowCount; i++) {
          const srcRow = batch.rows[i]!;
          // Clone row to avoid mutating original
          const outRow: Row = { ...srcRow };
          let failed = false;

          for (let j = 0; j < compiledLen; j++) {
            const entry = compiledEntries[j]!;
            try {
              outRow[entry.targetCol] = entry.evaluator(srcRow);
            } catch (err) {
              errorHandler.handle(err as Error, {
                row: batch.offset + i + 1,
                column: entry.targetCol,
                raw: srcRow,
              });
              failed = true;
              break;
            }
          }

          if (!failed) {
            mappedRows.push(outRow);
          }
        }

        yield {
          rows: mappedRows,
          offset: batch.offset,
        };
      }
    } finally {
      errorHandler.close();
    }
  };
}
