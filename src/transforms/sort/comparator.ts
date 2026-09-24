import type { Row } from "../../core/types.js";

export interface SortKeySpec {
  column: string;
  direction?: "asc" | "desc";
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
}

export interface SortOptions {
  by: string | string[] | SortKeySpec[];
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
  memoryLimit?: number | string;
  tempDir?: string;
  batchSize?: number;
}

export interface SequencedRow {
  row: Row;
  seq: number;
}

/**
 * Parses sort specifications from strings (e.g. "country,revenue:desc", "age:desc") or objects.
 */
export function parseSortSpecs(
  input: string | string[] | SortKeySpec[],
  defaults: {
    defaultDirection?: "asc" | "desc";
    nulls?: "first" | "last";
    ignoreCase?: boolean;
    natural?: boolean;
  } = {}
): SortKeySpec[] {
  const fallbackDir = defaults.defaultDirection || "asc";
  if (Array.isArray(input) && input.length > 0 && typeof input[0] === "object") {
    return (input as SortKeySpec[]).map((spec) => ({
      ...spec,
      direction: spec.direction || fallbackDir,
      nulls: spec.nulls || defaults.nulls || "last",
      ignoreCase: spec.ignoreCase ?? defaults.ignoreCase ?? false,
      natural: spec.natural ?? defaults.natural ?? false,
    }));
  }

  const rawSpecs: string[] = [];
  if (typeof input === "string") {
    rawSpecs.push(...input.split(",").map((s) => s.trim()).filter(Boolean));
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (typeof item === "string") {
        rawSpecs.push(...item.split(",").map((s) => s.trim()).filter(Boolean));
      }
    }
  }

  return rawSpecs.map((spec) => {
    const parts = spec.split(":");
    const column = parts[0]!.trim();
    let direction: "asc" | "desc";
    if (parts[1]) {
      const dirStr = parts[1].trim().toLowerCase();
      direction = dirStr === "desc" ? "desc" : "asc";
    } else {
      direction = fallbackDir;
    }

    return {
      column,
      direction,
      nulls: defaults.nulls || "last",
      ignoreCase: defaults.ignoreCase ?? false,
      natural: defaults.natural ?? false,
    };
  });
}

/**
 * Compares two primitive values with typed semantics (numbers, dates, booleans, strings).
 */
export function compareValues(
  a: unknown,
  b: unknown,
  spec: SortKeySpec
): number {
  const isANull = a === null || a === undefined || a === "";
  const isBNull = b === null || b === undefined || b === "";

  if (isANull && isBNull) return 0;
  if (isANull) return spec.nulls === "first" ? -1 : 1;
  if (isBNull) return spec.nulls === "first" ? 1 : -1;

  // Number comparison (including numeric strings)
  const isANum = typeof a === "number" || (typeof a === "string" && /^-?\d+(\.\d+)?$/.test(a.trim()));
  const isBNum = typeof b === "number" || (typeof b === "string" && /^-?\d+(\.\d+)?$/.test(b.trim()));

  if (isANum && isBNum) {
    const numA = typeof a === "number" ? a : Number.parseFloat(a as string);
    const numB = typeof b === "number" ? b : Number.parseFloat(b as string);
    if (!Number.isNaN(numA) && !Number.isNaN(numB)) {
      return numA < numB ? -1 : numA > numB ? 1 : 0;
    }
  }

  // Boolean comparison
  if (typeof a === "boolean" || typeof b === "boolean") {
    const boolA = Boolean(a);
    const boolB = Boolean(b);
    return boolA === boolB ? 0 : boolA ? 1 : -1;
  }

  // Date comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  // String comparison
  const strA = String(a);
  const strB = String(b);

  if (spec.natural) {
    return strA.localeCompare(strB, undefined, {
      numeric: true,
      sensitivity: spec.ignoreCase ? "base" : "variant",
    });
  }

  if (spec.ignoreCase) {
    const lowerA = strA.toLowerCase();
    const lowerB = strB.toLowerCase();
    return lowerA < lowerB ? -1 : lowerA > lowerB ? 1 : 0;
  }

  return strA < strB ? -1 : strA > strB ? 1 : 0;
}

/**
 * Creates a deterministic, multi-column, stable row comparator.
 */
export function createRowComparator(specs: SortKeySpec[]): (a: SequencedRow, b: SequencedRow) => number {
  return (aObj: SequencedRow, bObj: SequencedRow): number => {
    const a = aObj.row;
    const b = bObj.row;

    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const valA = a[spec.column];
      const valB = b[spec.column];

      const cmp = compareValues(valA, valB, spec);
      if (cmp !== 0) {
        return spec.direction === "desc" ? -cmp : cmp;
      }
    }

    // Preserve original insertion sequence for stability
    return aObj.seq - bObj.seq;
  };
}

/**
 * Creates a raw Row comparator (without seq).
 */
export function createRawRowComparator(specs: SortKeySpec[]): (a: Row, b: Row) => number {
  return (a: Row, b: Row): number => {
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const valA = a[spec.column];
      const valB = b[spec.column];

      const cmp = compareValues(valA, valB, spec);
      if (cmp !== 0) {
        return spec.direction === "desc" ? -cmp : cmp;
      }
    }
    return 0;
  };
}
