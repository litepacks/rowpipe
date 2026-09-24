import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

export interface ExplodeOptions {
  column: string;
  delimiter?: string | RegExp;
  trim?: boolean;
  dropEmpty?: boolean;
  preserveNullAndEmpty?: boolean;
}

/**
 * High-performance streaming Explode transform.
 * Expands arrays or delimited string values into multiple rows (1 -> N).
 */
export function explodeRows(
  columnOrOptions: string | ExplodeOptions,
  options?: Partial<ExplodeOptions>
): TransformFunction {
  const opts: ExplodeOptions =
    typeof columnOrOptions === "string"
      ? { column: columnOrOptions, ...options }
      : columnOrOptions;

  const column = opts.column;
  const delimiter = opts.delimiter ?? ",";
  const trim = opts.trim !== false;
  const dropEmpty = opts.dropEmpty !== false;
  const preserveNull = opts.preserveNullAndEmpty === true;

  return (input: DataStream): DataStream => {
    return (async function* () {
      let offset = 0;

      for await (const batch of input) {
        const explodedRows: Row[] = [];

        for (const row of batch.rows) {
          const val = row[column];

          if (val === null || val === undefined) {
            if (preserveNull) {
              explodedRows.push(row);
            }
            continue;
          }

          let items: unknown[] = [];

          if (Array.isArray(val)) {
            items = val;
          } else if (typeof val === "string") {
            const trimmed = val.trim();
            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
              try {
                const parsed = JSON.parse(trimmed);
                if (Array.isArray(parsed)) {
                  items = parsed;
                }
              } catch {
                // Not valid JSON array, proceed to delimiter split
              }
            }

            if (items.length === 0) {
              items = val.split(delimiter);
            }
          } else {
            items = [val];
          }

          let produced = 0;
          for (let item of items) {
            if (typeof item === "string") {
              if (trim) item = item.trim();
              if (dropEmpty && item === "") continue;
            } else if (item === null || item === undefined) {
              if (dropEmpty) continue;
            }

            explodedRows.push({ ...row, [column]: item });
            produced++;
          }

          if (produced === 0 && preserveNull) {
            explodedRows.push({ ...row, [column]: null });
          }
        }

        if (explodedRows.length > 0) {
          yield {
            rows: explodedRows,
            offset,
          };
          offset += explodedRows.length;
        }
      }
    })();
  };
}
