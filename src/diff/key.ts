import type { Row } from "../core/types.js";
import { safeJsonStringify } from "../utils/formatting.js";

/**
 * Encodes a single primitive key value into a collision-free typed string.
 */
export function encodeSingleKeyValue(value: unknown, coerce = false): string {
  if (value === undefined) {
    return "U";
  }
  if (value === null) {
    return "N";
  }

  if (typeof value === "boolean") {
    return value ? "B:1" : "B:0";
  }

  if (typeof value === "number") {
    if (coerce) {
      // In coerce mode, represent numeric values uniformly
      return `NUM:${value}`;
    }
    return Number.isInteger(value) ? `I:${value}` : `D:${value}`;
  }

  if (typeof value === "string") {
    if (coerce) {
      const trimmed = value.trim();
      const lower = trimmed.toLowerCase();
      if (lower === "true") return "B:1";
      if (lower === "false") return "B:0";

      // Test integer / float
      if (/^-?\d+$/.test(trimmed)) {
        return `NUM:${Number.parseInt(trimmed, 10)}`;
      }
      if (/^-?\d+\.\d+$/.test(trimmed)) {
        return `NUM:${Number.parseFloat(trimmed)}`;
      }
      return `S:${trimmed.length}:${trimmed}`;
    }
    return `S:${value.length}:${value}`;
  }

  if (typeof value === "bigint") {
    if (coerce) {
      return `NUM:${value.toString()}`;
    }
    return `BI:${value.toString()}`;
  }

  if (value instanceof Date) {
    return `T:${value.getTime()}`;
  }

  const json = safeJsonStringify(value);
  return `O:${json.length}:${json}`;
}

export interface EncodedKeyResult {
  encoded: string;
  rawKey: Record<string, unknown>;
  hasMissingOrNull: boolean;
}

/**
 * Encodes row key columns into a deterministic, collision-proof typed composite key string.
 */
export function encodeCompositeKey(
  row: Row,
  keyColumns: string[],
  options: { coerce?: boolean } = {}
): EncodedKeyResult {
  if (keyColumns.length === 1) {
    const col = keyColumns[0]!;
    const val = row[col];
    return {
      encoded: encodeSingleKeyValue(val, options.coerce),
      rawKey: { [col]: val },
      hasMissingOrNull: val === null || val === undefined,
    };
  }

  const rawKey: Record<string, unknown> = {};
  let encoded = "";
  let hasMissingOrNull = false;

  for (let i = 0; i < keyColumns.length; i++) {
    const col = keyColumns[i]!;
    const val = row[col];
    rawKey[col] = val;

    if (val === null || val === undefined) {
      hasMissingOrNull = true;
    }

    const encodedVal = encodeSingleKeyValue(val, options.coerce);
    if (i > 0) {
      encoded += "|";
    }
    encoded += encodedVal;
  }

  return {
    encoded,
    rawKey,
    hasMissingOrNull,
  };
}

/**
 * Formats a key object for human-readable output (e.g. "id=42" or "country=TR, id=101").
 */
export function formatKeyForDisplay(rawKey: Record<string, unknown>): string {
  const entries = Object.entries(rawKey);
  if (entries.length === 0) return "(none)";
  return entries
    .map(([col, val]) => {
      let displayVal: string;
      if (val === undefined) {
        displayVal = "<missing>";
      } else if (val === null) {
        displayVal = "null";
      } else if (typeof val === "object") {
        displayVal = JSON.stringify(val);
      } else {
        displayVal = String(val);
      }
      return `${col}=${displayVal}`;
    })
    .join(", ");
}
