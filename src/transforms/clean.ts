import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

export interface CleanOptions {
  trim?: boolean | string[];
  nullValues?: string[];
  fillNulls?: Record<string, unknown> | unknown;
  coerce?: boolean | string[];
  stripChars?: string | RegExp;
  case?: Record<string, "lower" | "upper" | "title"> | "lower" | "upper" | "title";
  dropEmptyRows?: boolean;
  batchSize?: number;
}

function toTitleCase(str: string): string {
  return str.replace(/\b\w+/g, (txt) => txt.charAt(0).toUpperCase() + txt.slice(1).toLowerCase());
}

function coerceValue(val: unknown): unknown {
  if (val === null || val === undefined) return val;
  if (typeof val !== "string") return val;

  const trimmed = val.trim();
  if (trimmed === "") return null;

  // Booleans
  const lower = trimmed.toLowerCase();
  if (lower === "true") return true;
  if (lower === "false") return false;

  // Integer
  if (/^-?\d+$/.test(trimmed)) {
    const num = Number.parseInt(trimmed, 10);
    if (!Number.isNaN(num) && Number.isSafeInteger(num)) return num;
  }

  // Float
  if (/^-?\d+\.\d+$/.test(trimmed) || /^-?\d+(?:\.\d+)?e[+-]?\d+$/i.test(trimmed)) {
    const num = Number.parseFloat(trimmed);
    if (!Number.isNaN(num)) return num;
  }

  return val;
}

/**
 * Streaming Data Cleaner Transform.
 * Cleans whitespace, normalizes null representations, fills defaults, coerces types,
 * and strips control characters with bounded O(1) memory.
 */
export function cleanRows(options: CleanOptions = {}): TransformFunction {
  const nullValuesSet = new Set(
    (options.nullValues || ["N/A", "NA", "null", "NULL", "none", "NONE", "-"]).map((s) => s.trim())
  );

  const trimAll = options.trim === true || options.trim === undefined;
  const trimCols = Array.isArray(options.trim) ? new Set(options.trim) : undefined;

  const coerceAll = options.coerce === true;
  const coerceCols = Array.isArray(options.coerce) ? new Set(options.coerce) : undefined;

  const fillNullsMap =
    typeof options.fillNulls === "object" && options.fillNulls !== null && !Array.isArray(options.fillNulls)
      ? (options.fillNulls as Record<string, unknown>)
      : undefined;
  const globalFillNull =
    options.fillNulls !== undefined && fillNullsMap === undefined ? options.fillNulls : undefined;

  const stripRegex = options.stripChars
    ? typeof options.stripChars === "string"
      ? new RegExp(`[${options.stripChars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}]`, "g")
      : options.stripChars
    : undefined;

  return (stream: DataStream): DataStream => {
    return (async function* () {
      for await (const batch of stream) {
        const cleanedRows: Row[] = [];

        for (const row of batch.rows) {
          const cleanedRow: Row = {};
          let hasNonEmptyField = false;

          for (const [key, rawVal] of Object.entries(row)) {
            let val = rawVal;

            // 1. String trimming & control character stripping
            if (typeof val === "string") {
              if (stripRegex) {
                val = val.replace(stripRegex, "");
              }

              if (trimAll || (trimCols && trimCols.has(key))) {
                val = (val as string).trim();
              }

              // 2. Case transformation
              if (options.case) {
                const targetCase =
                  typeof options.case === "string" ? options.case : options.case[key];
                if (targetCase === "lower") val = (val as string).toLowerCase();
                else if (targetCase === "upper") val = (val as string).toUpperCase();
                else if (targetCase === "title") val = toTitleCase(val as string);
              }

              // 3. Null values conversion
              if (nullValuesSet.has(val as string) || val === "") {
                val = null;
              }
            }

            // 4. Type coercion
            if (val !== null && val !== undefined) {
              if (coerceAll || (coerceCols && coerceCols.has(key))) {
                val = coerceValue(val);
              }
            }

            // 5. Fill nulls
            if (val === null || val === undefined) {
              if (fillNullsMap && key in fillNullsMap) {
                val = fillNullsMap[key];
              } else if (globalFillNull !== undefined) {
                val = globalFillNull;
              }
            }

            if (val !== null && val !== undefined && val !== "") {
              hasNonEmptyField = true;
            }

            cleanedRow[key] = val;
          }

          if (!options.dropEmptyRows || hasNonEmptyField) {
            cleanedRows.push(cleanedRow);
          }
        }

        if (cleanedRows.length > 0) {
          yield {
            rows: cleanedRows,
            offset: batch.offset,
          };
        }
      }
    })();
  };
}
