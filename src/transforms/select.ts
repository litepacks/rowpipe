import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

/**
 * Creates a transform that projects only the specified columns in order.
 */
export function selectColumns(columns: string[]): TransformFunction {
  const columnSet = columns.map((c) => c.trim()).filter((c) => c.length > 0);
  const len = columnSet.length;

  let projectRow: (row: Row) => Row;
  if (len === 1) {
    const c0 = columnSet[0]!;
    projectRow = (row) => ({ [c0]: row[c0] !== undefined ? row[c0] : "" });
  } else if (len === 2) {
    const c0 = columnSet[0]!, c1 = columnSet[1]!;
    projectRow = (row) => ({
      [c0]: row[c0] !== undefined ? row[c0] : "",
      [c1]: row[c1] !== undefined ? row[c1] : "",
    });
  } else if (len === 3) {
    const c0 = columnSet[0]!, c1 = columnSet[1]!, c2 = columnSet[2]!;
    projectRow = (row) => ({
      [c0]: row[c0] !== undefined ? row[c0] : "",
      [c1]: row[c1] !== undefined ? row[c1] : "",
      [c2]: row[c2] !== undefined ? row[c2] : "",
    });
  } else if (len === 4) {
    const c0 = columnSet[0]!, c1 = columnSet[1]!, c2 = columnSet[2]!, c3 = columnSet[3]!;
    projectRow = (row) => ({
      [c0]: row[c0] !== undefined ? row[c0] : "",
      [c1]: row[c1] !== undefined ? row[c1] : "",
      [c2]: row[c2] !== undefined ? row[c2] : "",
      [c3]: row[c3] !== undefined ? row[c3] : "",
    });
  } else {
    projectRow = (row) => {
      const newRow: Row = {};
      for (let j = 0; j < len; j++) {
        const col = columnSet[j]!;
        newRow[col] = row[col] !== undefined ? row[col] : "";
      }
      return newRow;
    };
  }

  return function (stream: DataStream): DataStream {
    return (async function* () {
      for await (const batch of stream) {
        const rows = batch.rows;
        const rowCount = rows.length;
        const projectedRows: Row[] = new Array(rowCount);

        for (let i = 0; i < rowCount; i++) {
          projectedRows[i] = projectRow(rows[i]!);
        }

        yield {
          rows: projectedRows,
          offset: batch.offset,
        };
      }
    })();
  };
}
