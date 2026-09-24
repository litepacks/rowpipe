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
 * Fast zero-allocation numeric check for strings without RegExp or .trim() allocations.
 */
function parseIfNumeric(val: string): number | null {
  const len = val.length;
  if (len === 0) return null;
  let i = 0;
  while (i < len && val.charCodeAt(i) <= 32) i++;
  if (i === len) return null;

  const first = val.charCodeAt(i);
  if ((first < 48 || first > 57) && first !== 45 && first !== 43 && first !== 46) {
    return null;
  }

  let hasDot = first === 46;
  let j = (first === 45 || first === 43 || first === 46) ? i + 1 : i;
  if (j === len) return null;

  for (; j < len; j++) {
    const c = val.charCodeAt(j);
    if (c >= 48 && c <= 57) continue;
    if (c === 46 && !hasDot) {
      hasDot = true;
      continue;
    }
    if (c <= 32) {
      for (let k = j + 1; k < len; k++) {
        if (val.charCodeAt(k) > 32) {
          return null;
        }
      }
      break;
    }
    return null;
  }

  const num = Number(val);
  return Number.isNaN(num) ? null : num;
}

/**
 * Compares two primitive values with typed semantics (numbers, dates, booleans, strings).
 */
export function compareValues(
  a: unknown,
  b: unknown,
  spec: SortKeySpec
): number {
  if (a === b) return 0;

  const isANull = a === null || a === undefined || a === "";
  const isBNull = b === null || b === undefined || b === "";

  if (isANull || isBNull) {
    if (isANull && isBNull) return 0;
    if (isANull) return spec.nulls === "first" ? -1 : 1;
    return spec.nulls === "first" ? 1 : -1;
  }

  const typeA = typeof a;
  const typeB = typeof b;

  // 1. Both are Numbers (Fastest path)
  if (typeA === "number" && typeB === "number") {
    const numA = a as number;
    const numB = b as number;
    return numA < numB ? -1 : numA > numB ? 1 : 0;
  }

  // 2. Both are Strings (Extremely common in CSV / tabular datasets)
  if (typeA === "string" && typeB === "string") {
    const strA = a as string;
    const strB = b as string;

    const codeA = strA.charCodeAt(0);
    const codeB = strB.charCodeAt(0);

    // Fast check: if either starts with a normal non-numeric character (e.g. 'A'-'Z', 'a'-'z'),
    // skip numeric parsing completely!
    const isPotentiallyNumA = (codeA >= 48 && codeA <= 57) || codeA === 45 || codeA === 43 || codeA === 46;
    const isPotentiallyNumB = (codeB >= 48 && codeB <= 57) || codeB === 45 || codeB === 43 || codeB === 46;

    if (isPotentiallyNumA && isPotentiallyNumB) {
      const numA = parseIfNumeric(strA);
      const numB = parseIfNumeric(strB);
      if (numA !== null && numB !== null) {
        return numA < numB ? -1 : numA > numB ? 1 : 0;
      }
    }

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

  // 3. Mixed Types (one number, one string, etc.)
  const numA = typeA === "number" ? (a as number) : (typeA === "string" ? parseIfNumeric(a as string) : null);
  const numB = typeB === "number" ? (b as number) : (typeB === "string" ? parseIfNumeric(b as string) : null);

  if (numA !== null && numB !== null) {
    return numA < numB ? -1 : numA > numB ? 1 : 0;
  }

  // 4. Boolean comparison
  if (typeA === "boolean" || typeB === "boolean") {
    const boolA = Boolean(a);
    const boolB = Boolean(b);
    return boolA === boolB ? 0 : boolA ? 1 : -1;
  }

  // 5. Date comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  // 6. General fallback
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
  if (specs.length === 1) {
    const spec = specs[0]!;
    const col = spec.column;
    const isDesc = spec.direction === "desc";
    return (aObj: SequencedRow, bObj: SequencedRow): number => {
      const cmp = compareValues(aObj.row[col], bObj.row[col], spec);
      if (cmp !== 0) return isDesc ? -cmp : cmp;
      return aObj.seq - bObj.seq;
    };
  }

  if (specs.length === 2) {
    const spec0 = specs[0]!;
    const col0 = spec0.column;
    const isDesc0 = spec0.direction === "desc";

    const spec1 = specs[1]!;
    const col1 = spec1.column;
    const isDesc1 = spec1.direction === "desc";

    return (aObj: SequencedRow, bObj: SequencedRow): number => {
      const a = aObj.row;
      const b = bObj.row;

      let cmp = compareValues(a[col0], b[col0], spec0);
      if (cmp !== 0) return isDesc0 ? -cmp : cmp;

      cmp = compareValues(a[col1], b[col1], spec1);
      if (cmp !== 0) return isDesc1 ? -cmp : cmp;

      return aObj.seq - bObj.seq;
    };
  }

  return (aObj: SequencedRow, bObj: SequencedRow): number => {
    const a = aObj.row;
    const b = bObj.row;

    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const cmp = compareValues(a[spec.column], b[spec.column], spec);
      if (cmp !== 0) {
        return spec.direction === "desc" ? -cmp : cmp;
      }
    }

    return aObj.seq - bObj.seq;
  };
}

/**
 * Creates a raw Row comparator (without seq).
 */
export function createRawRowComparator(specs: SortKeySpec[]): (a: Row, b: Row) => number {
  if (specs.length === 1) {
    const spec = specs[0]!;
    const col = spec.column;
    const isDesc = spec.direction === "desc";
    return (a: Row, b: Row): number => {
      const cmp = compareValues(a[col], b[col], spec);
      return isDesc ? -cmp : cmp;
    };
  }

  if (specs.length === 2) {
    const spec0 = specs[0]!;
    const col0 = spec0.column;
    const isDesc0 = spec0.direction === "desc";

    const spec1 = specs[1]!;
    const col1 = spec1.column;
    const isDesc1 = spec1.direction === "desc";

    return (a: Row, b: Row): number => {
      let cmp = compareValues(a[col0], b[col0], spec0);
      if (cmp !== 0) return isDesc0 ? -cmp : cmp;

      cmp = compareValues(a[col1], b[col1], spec1);
      if (cmp !== 0) return isDesc1 ? -cmp : cmp;

      return 0;
    };
  }

  return (a: Row, b: Row): number => {
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const cmp = compareValues(a[spec.column], b[spec.column], spec);
      if (cmp !== 0) {
        return spec.direction === "desc" ? -cmp : cmp;
      }
    }
    return 0;
  };
}
