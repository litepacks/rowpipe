import type { Row } from "../core/types.js";
import type { ColumnChange } from "./types.js";

export interface ValueComparisonOptions {
  coerce?: boolean;
  epsilon?: number;
  ignoreCase?: boolean;
  trim?: boolean;
}

/**
 * Checks if two arbitrary values are considered equal under the specified options.
 */
export function areValuesEqual(
  a: unknown,
  b: unknown,
  options: ValueComparisonOptions = {}
): boolean {
  // Identical reference or primitive
  if (a === b) {
    // Note: NaN === NaN is false in JS, handle NaN equality
    if (typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b)) {
      return true;
    }
    return true;
  }

  // Strict undefined (missing) vs null check
  if (a === undefined || b === undefined) {
    return a === b;
  }
  if (a === null || b === null) {
    if (options.coerce) {
      // Under coerce, null and empty string can match
      const other = a === null ? b : a;
      return other === "" || other === null || other === undefined;
    }
    return a === b;
  }

  // Type Coercion Mode
  if (options.coerce) {
    // If one is boolean and other is boolean string / number
    if (typeof a === "boolean" || typeof b === "boolean") {
      const boolA = toCoercedBoolean(a);
      const boolB = toCoercedBoolean(b);
      if (boolA !== null && boolB !== null) {
        return boolA === boolB;
      }
    }

    // If both can be converted to numbers
    const numA = toCoercedNumber(a);
    const numB = toCoercedNumber(b);
    if (numA !== null && numB !== null) {
      if (options.epsilon && options.epsilon > 0) {
        return Math.abs(numA - numB) <= options.epsilon;
      }
      return numA === numB;
    }

    // Date coercion
    const dateA = toDateTimestamp(a);
    const dateB = toDateTimestamp(b);
    if (dateA !== null && dateB !== null) {
      return dateA === dateB;
    }
  }

  // Numbers comparison (with epsilon if supplied)
  if (typeof a === "number" && typeof b === "number") {
    if (options.epsilon && options.epsilon > 0) {
      return Math.abs(a - b) <= options.epsilon;
    }
    return a === b;
  }

  // String comparison (with trim / ignoreCase)
  if (typeof a === "string" && typeof b === "string") {
    let strA = a;
    let strB = b;
    if (options.trim) {
      strA = strA.trim();
      strB = strB.trim();
    }
    if (options.ignoreCase) {
      strA = strA.toLowerCase();
      strB = strB.toLowerCase();
    }
    return strA === strB;
  }

  // Date object comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  // Arrays comparison
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!areValuesEqual(a[i], b[i], options)) return false;
    }
    return true;
  }

  // Object comparison
  if (typeof a === "object" && typeof b === "object" && a !== null && b !== null) {
    const objA = a as Record<string, unknown>;
    const objB = b as Record<string, unknown>;
    const keysA = Object.keys(objA);
    const keysB = Object.keys(objB);
    if (keysA.length !== keysB.length) return false;

    for (const key of keysA) {
      if (!Object.prototype.hasOwnProperty.call(objB, key)) return false;
      if (!areValuesEqual(objA[key], objB[key], options)) return false;
    }
    return true;
  }

  return false;
}

function toCoercedBoolean(val: unknown): boolean | null {
  if (typeof val === "boolean") return val;
  if (typeof val === "number") return val === 1 ? true : val === 0 ? false : null;
  if (typeof val === "string") {
    const s = val.trim().toLowerCase();
    if (s === "true" || s === "1" || s === "yes" || s === "t") return true;
    if (s === "false" || s === "0" || s === "no" || s === "f") return false;
  }
  return null;
}

function toCoercedNumber(val: unknown): number | null {
  if (typeof val === "number") return Number.isNaN(val) ? null : val;
  if (typeof val === "string") {
    const s = val.trim();
    if (!s) return null;
    const num = Number(s);
    return Number.isNaN(num) ? null : num;
  }
  return null;
}

function toDateTimestamp(val: unknown): number | null {
  if (val instanceof Date) return val.getTime();
  if (typeof val === "string" && /^\d{4}-\d{2}-\d{2}/.test(val.trim())) {
    const time = Date.parse(val.trim());
    return Number.isNaN(time) ? null : time;
  }
  return null;
}

/**
 * Compares two rows across specified columns and returns a list of differences.
 */
export function compareRows(
  leftRow: Row,
  rightRow: Row,
  compareColumns: string[],
  options: ValueComparisonOptions = {}
): ColumnChange[] {
  const changes: ColumnChange[] = [];

  for (let i = 0; i < compareColumns.length; i++) {
    const col = compareColumns[i]!;
    const leftVal = leftRow[col];
    const rightVal = rightRow[col];

    if (!areValuesEqual(leftVal, rightVal, options)) {
      changes.push({
        column: col,
        oldVal: leftVal,
        newVal: rightVal,
      });
    }
  }

  return changes;
}
