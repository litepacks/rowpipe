import type { Row } from "../core/types.js";
import { safeJsonStringify } from "../utils/formatting.js";

/**
 * Computes a fast deterministic 32-bit FNV-1a hash over canonicalized column-value pairs in a row.
 */
export function computeRowFingerprint(row: Row, compareColumns: string[]): number {
  let hash = 2166136261;

  for (let i = 0; i < compareColumns.length; i++) {
    const col = compareColumns[i]!;
    const val = row[col];

    // Hash column name
    for (let c = 0; c < col.length; c++) {
      hash ^= col.charCodeAt(c);
      hash = Math.imul(hash, 16777619);
    }

    // Hash separator
    hash ^= 58; // ':'
    hash = Math.imul(hash, 16777619);

    // Hash value representation
    if (val === undefined) {
      hash ^= 85; // 'U'
      hash = Math.imul(hash, 16777619);
    } else if (val === null) {
      hash ^= 78; // 'N'
      hash = Math.imul(hash, 16777619);
    } else if (typeof val === "number" || typeof val === "bigint") {
      // Convert number or bigint to string representation
      const str = String(val);
      for (let c = 0; c < str.length; c++) {
        hash ^= str.charCodeAt(c);
        hash = Math.imul(hash, 16777619);
      }
    } else if (typeof val === "boolean") {
      hash ^= val ? 49 : 48; // '1' or '0'
      hash = Math.imul(hash, 16777619);
    } else {
      const str = typeof val === "string" ? val : safeJsonStringify(val);
      for (let c = 0; c < str.length; c++) {
        hash ^= str.charCodeAt(c);
        hash = Math.imul(hash, 16777619);
      }
    }

    hash ^= 124; // '|'
    hash = Math.imul(hash, 16777619);
  }

  return hash >>> 0;
}
