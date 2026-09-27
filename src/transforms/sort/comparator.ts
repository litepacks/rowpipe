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
  k0?: unknown;
  k1?: unknown;
  keys?: unknown[];
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
 * Pre-extracts and normalizes sort key from row value for single-pass O(N) key preparation.
 * Avoids repeated character-by-character numeric parsing inside O(N log N) comparison loops.
 */
export function extractSortKey(val: unknown, spec: SortKeySpec): unknown {
  if (val === null || val === undefined || val === "") {
    return null;
  }
  if (typeof val === "number") {
    return val;
  }
  if (typeof val === "string") {
    const code = val.charCodeAt(0);
    const isPotentiallyNum =
      (code >= 48 && code <= 57) || code === 45 || code === 43 || code === 46;
    if (isPotentiallyNum) {
      const num = parseIfNumeric(val);
      if (num !== null) {
        return num;
      }
    }
    if (spec.natural) {
      return val;
    }
    if (spec.ignoreCase) {
      return val.toLowerCase();
    }
    return val;
  }
  if (val instanceof Date) {
    return val.getTime();
  }
  if (typeof val === "boolean") {
    return val ? 1 : 0;
  }
  return val;
}

/**
 * Fast comparison of two pre-extracted keys with typed fast paths.
 */
export function compareKeys(
  kA: unknown,
  kB: unknown,
  spec: SortKeySpec
): number {
  if (kA === kB) return 0;

  const isANull = kA === null || kA === undefined;
  const isBNull = kB === null || kB === undefined;

  if (isANull || isBNull) {
    if (isANull && isBNull) return 0;
    if (isANull) return spec.nulls === "first" ? -1 : 1;
    return spec.nulls === "first" ? 1 : -1;
  }

  const typeA = typeof kA;
  const typeB = typeof kB;

  if (typeA === "number" && typeB === "number") {
    return (kA as number) < (kB as number) ? -1 : 1;
  }

  if (typeA === "string" && typeB === "string") {
    const strA = kA as string;
    const strB = kB as string;
    if (spec.natural) {
      return strA.localeCompare(strB, undefined, {
        numeric: true,
        sensitivity: spec.ignoreCase ? "base" : "variant",
      });
    }
    return strA < strB ? -1 : 1;
  }

  return compareValues(kA, kB, spec);
}

/**
 * Creates a deterministic, multi-column, stable row comparator using pre-extracted keys.
 */
export function createRowComparator(specs: SortKeySpec[]): (a: SequencedRow, b: SequencedRow) => number {
  if (specs.length === 1) {
    const spec = specs[0]!;
    const col = spec.column;
    const isDesc = spec.direction === "desc";
    const nullsFirst = spec.nulls === "first";
    const isNatural = Boolean(spec.natural);
    const ignoreCase = Boolean(spec.ignoreCase);

    return (aObj: SequencedRow, bObj: SequencedRow): number => {
      const kA = aObj.k0 !== undefined ? aObj.k0 : extractSortKey(aObj.row[col], spec);
      const kB = bObj.k0 !== undefined ? bObj.k0 : extractSortKey(bObj.row[col], spec);

      if (kA === kB) return aObj.seq - bObj.seq;

      if (kA === null || kB === null) {
        const cmp = kA === null ? (nullsFirst ? -1 : 1) : (nullsFirst ? 1 : -1);
        return isDesc ? -cmp : cmp;
      }

      if (typeof kA === "number" && typeof kB === "number") {
        const diff = kA < kB ? -1 : 1;
        return isDesc ? -diff : diff;
      }

      if (typeof kA === "string" && typeof kB === "string") {
        let diff: number;
        if (isNatural) {
          diff = kA.localeCompare(kB, undefined, {
            numeric: true,
            sensitivity: ignoreCase ? "base" : "variant",
          });
        } else {
          diff = kA < kB ? -1 : 1;
        }
        return isDesc ? -diff : diff;
      }

      const cmp = compareValues(kA, kB, spec);
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
      const kA0 = aObj.k0 !== undefined ? aObj.k0 : extractSortKey(aObj.row[col0], spec0);
      const kB0 = bObj.k0 !== undefined ? bObj.k0 : extractSortKey(bObj.row[col0], spec0);

      let cmp = compareKeys(kA0, kB0, spec0);
      if (cmp !== 0) return isDesc0 ? -cmp : cmp;

      const kA1 = aObj.k1 !== undefined ? aObj.k1 : extractSortKey(aObj.row[col1], spec1);
      const kB1 = bObj.k1 !== undefined ? bObj.k1 : extractSortKey(bObj.row[col1], spec1);

      cmp = compareKeys(kA1, kB1, spec1);
      if (cmp !== 0) return isDesc1 ? -cmp : cmp;

      return aObj.seq - bObj.seq;
    };
  }

  return (aObj: SequencedRow, bObj: SequencedRow): number => {
    const keysA = aObj.keys;
    const keysB = bObj.keys;

    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const kA = keysA !== undefined ? keysA[i] : extractSortKey(aObj.row[spec.column], spec);
      const kB = keysB !== undefined ? keysB[i] : extractSortKey(bObj.row[spec.column], spec);

      const cmp = compareKeys(kA, kB, spec);
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
      const kA = extractSortKey(a[col], spec);
      const kB = extractSortKey(b[col], spec);
      const cmp = compareKeys(kA, kB, spec);
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
      let cmp = compareKeys(extractSortKey(a[col0], spec0), extractSortKey(b[col0], spec0), spec0);
      if (cmp !== 0) return isDesc0 ? -cmp : cmp;

      cmp = compareKeys(extractSortKey(a[col1], spec1), extractSortKey(b[col1], spec1), spec1);
      if (cmp !== 0) return isDesc1 ? -cmp : cmp;

      return 0;
    };
  }

  return (a: Row, b: Row): number => {
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i]!;
      const cmp = compareKeys(extractSortKey(a[spec.column], spec), extractSortKey(b[spec.column], spec), spec);
      if (cmp !== 0) {
        return spec.direction === "desc" ? -cmp : cmp;
      }
    }
    return 0;
  };
}
