import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

export interface FlattenOptions {
  separator?: string;
  maxDepth?: number;
  arrays?: boolean;
}

function isPlainObject(val: unknown): val is Record<string, unknown> {
  return (
    val !== null &&
    typeof val === "object" &&
    !Array.isArray(val) &&
    !(val instanceof Date) &&
    !Buffer.isBuffer(val) &&
    !(val instanceof Uint8Array)
  );
}

function flattenObject(
  obj: Record<string, unknown>,
  separator: string,
  maxDepth: number,
  flattenArrays: boolean,
  currentDepth = 1,
  prefix = ""
): Row {
  const result: Row = {};

  for (const [key, val] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}${separator}${key}` : key;

    if (currentDepth < maxDepth && isPlainObject(val)) {
      const nested = flattenObject(val, separator, maxDepth, flattenArrays, currentDepth + 1, fullKey);
      Object.assign(result, nested);
    } else if (currentDepth < maxDepth && flattenArrays && Array.isArray(val)) {
      if (val.length === 0) {
        result[fullKey] = val;
      } else {
        val.forEach((item, index) => {
          const arrayKey = `${fullKey}${separator}${index}`;
          if (isPlainObject(item)) {
            const nested = flattenObject(item, separator, maxDepth, flattenArrays, currentDepth + 1, arrayKey);
            Object.assign(result, nested);
          } else {
            result[arrayKey] = item;
          }
        });
      }
    } else {
      result[fullKey] = val;
    }
  }

  return result;
}

/**
 * High-performance streaming Flatten transform.
 * Flattens nested JSON objects into flat dot-notated columns.
 */
export function flattenRows(options?: FlattenOptions): TransformFunction {
  const separator = options?.separator ?? ".";
  const maxDepth = options?.maxDepth ?? 10;
  const flattenArrays = options?.arrays === true;

  return (input: DataStream): DataStream => {
    return (async function* () {
      let offset = 0;

      for await (const batch of input) {
        const flattenedRows: Row[] = new Array(batch.rows.length);

        for (let i = 0; i < batch.rows.length; i++) {
          flattenedRows[i] = flattenObject(batch.rows[i]!, separator, maxDepth, flattenArrays);
        }

        yield {
          rows: flattenedRows,
          offset,
        };
        offset += flattenedRows.length;
      }
    })();
  };
}
