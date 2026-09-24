import type { Row } from "../core/types.js";
import type { ASTNode } from "./expression.js";

/**
 * Safely accesses a nested path in an object or parsed JSON (e.g. user.address.city).
 */
function getNestedValue(obj: unknown, path: string): unknown {
  if (obj === null || obj === undefined) return null;
  let current: unknown = obj;

  if (typeof current === "string") {
    try {
      current = JSON.parse(current);
    } catch {
      return null;
    }
  }

  const parts = path.split(".");
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== "object") {
      return null;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function resolveIdentifier(name: string, row: Row): unknown {
  if (name in row && row[name] !== undefined) {
    const val = row[name];
    return val === null || val === "" ? null : val;
  }

  const parts = name.split(".");
  for (let i = 1; i <= parts.length; i++) {
    const rootKey = parts.slice(0, i).join(".");
    if (rootKey in row && row[rootKey] !== undefined) {
      const restPath = parts.slice(i).join(".");
      if (restPath === "") {
        const val = row[rootKey];
        return val === null || val === "" ? null : val;
      }
      const nested = getNestedValue(row[rootKey], restPath);
      if (nested !== undefined && nested !== null && nested !== "") {
        return nested;
      }
      return null;
    }
  }

  const firstPart = parts[0];
  if (firstPart === "payload" || firstPart === "row" || firstPart === "data") {
    const restPath = parts.slice(1).join(".");
    const nested = getNestedValue(row, restPath);
    if (nested !== undefined && nested !== null && nested !== "") {
      return nested;
    }
  }

  return null;
}

/**
 * Common inline helper functions passed to JIT compiled functions.
 */
export const JIT_HELPERS = {
  get: (row: Row, name: string): unknown => resolveIdentifier(name, row),
  eq: (a: unknown, b: unknown): boolean => {
    if (a === b) return true;
    if (a === null || a === undefined || b === null || b === undefined) return a == b;
    if (typeof a === "number" || typeof b === "number") {
      return Number(a) === Number(b);
    }
    return String(a) === String(b);
  },
  neq: (a: unknown, b: unknown): boolean => {
    if (a === b) return false;
    if (a === null || a === undefined || b === null || b === undefined) return a != b;
    if (typeof a === "number" || typeof b === "number") {
      return Number(a) !== Number(b);
    }
    return String(a) !== String(b);
  },
  gt: (a: unknown, b: unknown): boolean => {
    if (a === null || a === undefined || b === null || b === undefined) return false;
    return Number(a) > Number(b);
  },
  gte: (a: unknown, b: unknown): boolean => {
    if (a === null || a === undefined || b === null || b === undefined) return false;
    return Number(a) >= Number(b);
  },
  lt: (a: unknown, b: unknown): boolean => {
    if (a === null || a === undefined || b === null || b === undefined) return false;
    return Number(a) < Number(b);
  },
  lte: (a: unknown, b: unknown): boolean => {
    if (a === null || a === undefined || b === null || b === undefined) return false;
    return Number(a) <= Number(b);
  },
  add: (a: unknown, b: unknown): unknown => {
    if (typeof a === "string" || typeof b === "string") {
      return String(a ?? "") + String(b ?? "");
    }
    return Number(a ?? 0) + Number(b ?? 0);
  },
  contains: (a: unknown, b: unknown): boolean => {
    return String(a ?? "").toLowerCase().includes(String(b ?? "").toLowerCase());
  },
  startswith: (a: unknown, b: unknown): boolean => {
    return String(a ?? "").startsWith(String(b ?? ""));
  },
  endswith: (a: unknown, b: unknown): boolean => {
    return String(a ?? "").endsWith(String(b ?? ""));
  },
  lower: (a: unknown): string => String(a ?? "").toLowerCase(),
  upper: (a: unknown): string => String(a ?? "").toUpperCase(),
  trim: (a: unknown): string => String(a ?? "").trim(),
  length: (a: unknown): number => String(a ?? "").length,
  concat: (...args: unknown[]): string => args.map((a) => (a === null || a === undefined ? "" : String(a))).join(""),
  substr: (s: unknown, start = 0, len?: number): string => {
    const str = String(s ?? "");
    return len !== undefined ? str.slice(Number(start), Number(start) + Number(len)) : str.slice(Number(start));
  },
  substring: (s: unknown, start = 0, len?: number): string => {
    const str = String(s ?? "");
    return len !== undefined ? str.slice(Number(start), Number(start) + Number(len)) : str.slice(Number(start));
  },
  replace: (s: unknown, search: unknown, replacement: unknown): string => {
    return String(s ?? "").replaceAll(String(search ?? ""), String(replacement ?? ""));
  },
  round: (a: unknown, decimals = 0): number => {
    const num = Number(a);
    if (Number.isNaN(num)) return 0;
    const factor = Math.pow(10, Number(decimals) || 0);
    return Math.round(num * factor) / factor;
  },
  abs: (a: unknown): number => Math.abs(Number(a) || 0),
  ceil: (a: unknown): number => Math.ceil(Number(a) || 0),
  floor: (a: unknown): number => Math.floor(Number(a) || 0),
  sqrt: (a: unknown): number => Math.sqrt(Number(a) || 0),
  min: (...args: unknown[]): number => Math.min(...args.map((a) => Number(a) || 0)),
  max: (...args: unknown[]): number => Math.max(...args.map((a) => Number(a) || 0)),
  coalesce: (...args: unknown[]): unknown => {
    for (const arg of args) {
      if (arg !== null && arg !== undefined && arg !== "") return arg;
    }
    return null;
  },
  if: (cond: unknown, thenVal: unknown, elseVal: unknown): unknown => (cond ? thenVal : elseVal),
  iif: (cond: unknown, thenVal: unknown, elseVal: unknown): unknown => (cond ? thenVal : elseVal),
  isnull: (a: unknown): boolean => a === null || a === undefined || a === "",
  isnotnull: (a: unknown): boolean => a !== null && a !== undefined && a !== "",
  jsonget: (obj: unknown, path: unknown): unknown => getNestedValue(obj, String(path ?? "")),
};

/**
 * Translates an ASTNode into a safe, high-speed JavaScript code string.
 * Returns null if the expression contains unsupported dynamic features.
 */
function generateJsCode(node: ASTNode): string | null {
  switch (node.type) {
    case "Literal":
      return JSON.stringify(node.value);

    case "Identifier": {
      if (!node.name.includes(".")) {
        const escapedCol = JSON.stringify(node.name);
        return `((row[${escapedCol}] !== undefined && row[${escapedCol}] !== "") ? row[${escapedCol}] : null)`;
      }
      return `(h.get(row, ${JSON.stringify(node.name)}))`;
    }

    case "UnaryOp": {
      const argCode = generateJsCode(node.argument);
      if (!argCode) return null;
      if (node.operator === "!") return `(!Boolean(${argCode}))`;
      if (node.operator === "-") return `(-Number(${argCode} || 0))`;
      if (node.operator === "+") return `(+Number(${argCode} || 0))`;
      return null;
    }

    case "BinaryOp": {
      const leftCode = generateJsCode(node.left);
      const rightCode = generateJsCode(node.right);
      if (!leftCode || !rightCode) return null;

      switch (node.operator) {
        case "&&":
          return `(Boolean(${leftCode}) && Boolean(${rightCode}))`;
        case "||":
          return `(Boolean(${leftCode}) || Boolean(${rightCode}))`;
        case "==":
          return `(h.eq(${leftCode}, ${rightCode}))`;
        case "!=":
          return `(h.neq(${leftCode}, ${rightCode}))`;
        case ">":
          return `(h.gt(${leftCode}, ${rightCode}))`;
        case ">=":
          return `(h.gte(${leftCode}, ${rightCode}))`;
        case "<":
          return `(h.lt(${leftCode}, ${rightCode}))`;
        case "<=":
          return `(h.lte(${leftCode}, ${rightCode}))`;
        case "+":
          return `(h.add(${leftCode}, ${rightCode}))`;
        case "-":
          return `((Number(${leftCode}) || 0) - (Number(${rightCode}) || 0))`;
        case "*":
          return `((Number(${leftCode}) || 0) * (Number(${rightCode}) || 0))`;
        case "/":
          return `((Number(${leftCode}) || 0) / (Number(${rightCode}) || 1))`;
        case "%":
          return `((Number(${leftCode}) || 0) % (Number(${rightCode}) || 1))`;
        default:
          return null;
      }
    }

    case "CallExpr": {
      const fnName = node.callee.toLowerCase();
      const argCodes: string[] = [];
      for (const arg of node.args) {
        const c = generateJsCode(arg);
        if (!c) return null;
        argCodes.push(c);
      }

      if (fnName in JIT_HELPERS) {
        return `(h.${fnName}(${argCodes.join(", ")}))`;
      }

      // If function is not in JIT helpers list, return null to fallback to tree evaluator
      return null;
    }

    default:
      return null;
  }
}

/**
 * Attempts to compile an ASTNode into a native V8 boolean predicate function.
 * Returns null if the expression cannot be JIT-compiled safely.
 */
export function tryCompileJITPredicate(ast: ASTNode): ((row: Row) => boolean) | null {
  try {
    const bodyExpr = generateJsCode(ast);
    if (!bodyExpr) return null;

    // Create single-shot compiled function with helpers bound in closure
    const fnConstructor = new Function("h", `return function(row) { return Boolean(${bodyExpr}); };`);
    const compiled = fnConstructor(JIT_HELPERS) as (row: Row) => boolean;
    return compiled;
  } catch {
    return null;
  }
}

/**
 * Attempts to compile an ASTNode into a native V8 value evaluator function.
 * Returns null if the expression cannot be JIT-compiled safely.
 */
export function tryCompileJITValue(ast: ASTNode): ((row: Row) => unknown) | null {
  try {
    const bodyExpr = generateJsCode(ast);
    if (!bodyExpr) return null;

    const fnConstructor = new Function("h", `return function(row) { return ${bodyExpr}; };`);
    const compiled = fnConstructor(JIT_HELPERS) as (row: Row) => unknown;
    return compiled;
  } catch {
    return null;
  }
}
