import type { DataStream, Row, TransformFunction } from "../core/types.js";
import { compileExpression } from "./expression.js";

/**
 * Creates a transform that filters rows based on a safe compiled filter expression.
 */
export function filterRows(
  predicateOrExpression: string | ((row: Row) => boolean)
): TransformFunction {
  const predicate =
    typeof predicateOrExpression === "function"
      ? predicateOrExpression
      : compileExpression(predicateOrExpression);

  return function (stream: DataStream): DataStream {
    return (async function* () {
      for await (const batch of stream) {
        const rows = batch.rows;
        const len = rows.length;
        const filteredRows: Row[] = [];

        for (let i = 0; i < len; i++) {
          const row = rows[i]!;
          if (predicate(row)) {
            filteredRows.push(row);
          }
        }

        if (filteredRows.length > 0) {
          yield {
            rows: filteredRows,
            offset: batch.offset,
          };
        }
      }
    })();
  };
}
