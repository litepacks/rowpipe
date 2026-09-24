import { InvalidArgumentError } from "../core/errors.js";
import type { Row } from "../core/types.js";
import { detectSemanticType } from "../analytics/semantic-types.js";
import { formatBytes, formatNumber, parseBytes } from "../utils/formatting.js";
import { tryCompileJITPredicate, tryCompileJITValue } from "./jit-compiler.js";

// Token Types
type TokenType =
  | "NUMBER"
  | "STRING"
  | "BOOLEAN"
  | "NULL"
  | "IDENTIFIER"
  | "OP"
  | "PIPE"
  | "LPAREN"
  | "RPAREN"
  | "COMMA"
  | "EOF";

interface Token {
  type: TokenType;
  value: string | number | boolean | null;
  pos: number;
}

// Tokenizer
export class Lexer {
  private input: string;
  private cursor = 0;

  constructor(input: string) {
    this.input = input;
  }

  private isWhitespace(c: string): boolean {
    return c === " " || c === "\t" || c === "\n" || c === "\r";
  }

  private isDigit(c: string): boolean {
    return c >= "0" && c <= "9";
  }

  private isAlpha(c: string): boolean {
    return (
      (c >= "a" && c <= "z") ||
      (c >= "A" && c <= "Z") ||
      c === "_" ||
      c === "$" ||
      c === "."
    );
  }

  tokenize(): Token[] {
    const tokens: Token[] = [];

    while (this.cursor < this.input.length) {
      const char = this.input[this.cursor]!;

      if (this.isWhitespace(char)) {
        this.cursor++;
        continue;
      }

      const pos = this.cursor;

      // Parentheses & comma
      if (char === "(") {
        tokens.push({ type: "LPAREN", value: "(", pos });
        this.cursor++;
        continue;
      }
      if (char === ")") {
        tokens.push({ type: "RPAREN", value: ")", pos });
        this.cursor++;
        continue;
      }
      if (char === ",") {
        tokens.push({ type: "COMMA", value: ",", pos });
        this.cursor++;
        continue;
      }

      // Pipe operator | vs ||
      if (char === "|") {
        if (this.cursor + 1 < this.input.length && this.input[this.cursor + 1] === "|") {
          tokens.push({ type: "OP", value: "||", pos });
          this.cursor += 2;
          continue;
        } else {
          tokens.push({ type: "PIPE", value: "|", pos });
          this.cursor++;
          continue;
        }
      }

      // Strings (single or double quoted)
      if (char === '"' || char === "'") {
        const quoteChar = char;
        let strVal = "";
        this.cursor++;
        while (this.cursor < this.input.length) {
          const c = this.input[this.cursor]!;
          if (c === "\\") {
            this.cursor++;
            if (this.cursor < this.input.length) {
              strVal += this.input[this.cursor]!;
              this.cursor++;
            }
          } else if (c === quoteChar) {
            this.cursor++;
            break;
          } else {
            strVal += c;
            this.cursor++;
          }
        }
        tokens.push({ type: "STRING", value: strVal, pos });
        continue;
      }

      // Numbers
      if (this.isDigit(char) || (char === "-" && this.isDigit(this.input[this.cursor + 1] || ""))) {
        let numStr = char;
        this.cursor++;
        let hasDot = false;
        while (this.cursor < this.input.length) {
          const c = this.input[this.cursor]!;
          if (this.isDigit(c)) {
            numStr += c;
            this.cursor++;
          } else if (c === "." && !hasDot) {
            hasDot = true;
            numStr += c;
            this.cursor++;
          } else {
            break;
          }
        }
        tokens.push({ type: "NUMBER", value: Number.parseFloat(numStr), pos });
        continue;
      }

      // Multi-character operators: ==, !=, >=, <=, &&
      const twoChar = this.input.slice(this.cursor, this.cursor + 2);
      if (["==", "!=", ">=", "<=", "&&"].includes(twoChar)) {
        tokens.push({ type: "OP", value: twoChar, pos });
        this.cursor += 2;
        continue;
      }

      // Single character operators: >, <, !, +, -, *, /, %
      if ([">", "<", "!", "+", "-", "*", "/", "%"].includes(char)) {
        tokens.push({ type: "OP", value: char, pos });
        this.cursor++;
        continue;
      }

      // Identifiers / Keywords / Column Names (may be backtick quoted e.g. `user name`)
      if (char === "`") {
        let identVal = "";
        this.cursor++;
        while (this.cursor < this.input.length && this.input[this.cursor] !== "`") {
          identVal += this.input[this.cursor]!;
          this.cursor++;
        }
        if (this.cursor < this.input.length && this.input[this.cursor] === "`") {
          this.cursor++;
        }
        tokens.push({ type: "IDENTIFIER", value: identVal, pos });
        continue;
      }

      if (this.isAlpha(char)) {
        let ident = "";
        while (this.cursor < this.input.length && (this.isAlpha(this.input[this.cursor]!) || this.isDigit(this.input[this.cursor]!))) {
          ident += this.input[this.cursor]!;
          this.cursor++;
        }

        if (ident === "true") {
          tokens.push({ type: "BOOLEAN", value: true, pos });
        } else if (ident === "false") {
          tokens.push({ type: "BOOLEAN", value: false, pos });
        } else if (ident === "null" || ident === "NULL") {
          tokens.push({ type: "NULL", value: null, pos });
        } else if (ident === "and" || ident === "AND") {
          tokens.push({ type: "OP", value: "&&", pos });
        } else if (ident === "or" || ident === "OR") {
          tokens.push({ type: "OP", value: "||", pos });
        } else if (ident === "not" || ident === "NOT") {
          tokens.push({ type: "OP", value: "!", pos });
        } else {
          tokens.push({ type: "IDENTIFIER", value: ident, pos });
        }
        continue;
      }

      throw new InvalidArgumentError(
        `Unexpected character "${char}" in filter expression at position ${pos}`
      );
    }

    tokens.push({ type: "EOF", value: null, pos: this.cursor });
    return tokens;
  }
}

// AST Nodes
export type ASTNode =
  | { type: "Literal"; value: unknown }
  | { type: "Identifier"; name: string }
  | { type: "UnaryOp"; operator: string; argument: ASTNode }
  | { type: "BinaryOp"; operator: string; left: ASTNode; right: ASTNode }
  | { type: "CallExpr"; callee: string; args: ASTNode[] };

export class Parser {
  private tokens: Token[];
  private current = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  private peek(): Token {
    return this.tokens[this.current] || { type: "EOF", value: null, pos: -1 };
  }

  private advance(): Token {
    const t = this.peek();
    this.current++;
    return t;
  }

  private match(...expectedValues: string[]): boolean {
    const t = this.peek();
    if (expectedValues.includes(String(t.value))) {
      this.advance();
      return true;
    }
    return false;
  }

  parse(): ASTNode {
    const expr = this.parseLogicalOr();
    if (this.peek().type !== "EOF") {
      throw new InvalidArgumentError(
        `Unexpected token "${String(this.peek().value)}" at position ${this.peek().pos}`
      );
    }
    return expr;
  }

  private parseLogicalOr(): ASTNode {
    let left = this.parseLogicalAnd();

    while (this.match("||")) {
      const right = this.parseLogicalAnd();
      left = { type: "BinaryOp", operator: "||", left, right };
    }

    return left;
  }

  private parseLogicalAnd(): ASTNode {
    let left = this.parseEquality();

    while (this.match("&&")) {
      const right = this.parseEquality();
      left = { type: "BinaryOp", operator: "&&", left, right };
    }

    return left;
  }

  private parseEquality(): ASTNode {
    let left = this.parseRelational();

    while (this.peek().value === "==" || this.peek().value === "!=") {
      const op = String(this.advance().value);
      const right = this.parseRelational();
      left = { type: "BinaryOp", operator: op, left, right };
    }

    return left;
  }

  private parseRelational(): ASTNode {
    let left = this.parseAdditive();

    while (
      this.peek().value === ">" ||
      this.peek().value === ">=" ||
      this.peek().value === "<" ||
      this.peek().value === "<="
    ) {
      const op = String(this.advance().value);
      const right = this.parseAdditive();
      left = { type: "BinaryOp", operator: op, left, right };
    }

    return left;
  }

  private parseAdditive(): ASTNode {
    let left = this.parseMultiplicative();

    while (this.peek().value === "+" || this.peek().value === "-") {
      const op = String(this.advance().value);
      const right = this.parseMultiplicative();
      left = { type: "BinaryOp", operator: op, left, right };
    }

    return left;
  }

  private parseMultiplicative(): ASTNode {
    let left = this.parsePipe();

    while (
      this.peek().value === "*" ||
      this.peek().value === "/" ||
      this.peek().value === "%"
    ) {
      const op = String(this.advance().value);
      const right = this.parsePipe();
      left = { type: "BinaryOp", operator: op, left, right };
    }

    return left;
  }

  /**
   * Parses pipe expressions e.g. email | trim | lower | startsWith("admin")
   */
  private parsePipe(): ASTNode {
    let left = this.parseUnary();

    while (this.peek().type === "PIPE") {
      this.advance(); // consume '|'

      const token = this.peek();
      if (token.type !== "IDENTIFIER") {
        throw new InvalidArgumentError(
          `Expected function or identifier after '|' at position ${token.pos}`
        );
      }

      const fnName = String(this.advance().value);
      const args: ASTNode[] = [left]; // left becomes first argument

      if (this.peek().type === "LPAREN") {
        this.advance(); // consume '('
        if (this.peek().type !== "RPAREN") {
          args.push(this.parseLogicalOr());
          while (this.peek().type === "COMMA") {
            this.advance(); // consume ','
            args.push(this.parseLogicalOr());
          }
        }

        if (this.peek().type !== "RPAREN") {
          throw new InvalidArgumentError(
            `Expected closing ')' at position ${this.peek().pos}`
          );
        }
        this.advance(); // consume ')'
      }

      left = { type: "CallExpr", callee: fnName, args };
    }

    return left;
  }

  private parseUnary(): ASTNode {
    if (this.peek().value === "!" || this.peek().value === "-") {
      const op = String(this.advance().value);
      const argument = this.parseUnary();
      return { type: "UnaryOp", operator: op, argument };
    }

    return this.parsePrimary();
  }

  private parsePrimary(): ASTNode {
    const token = this.peek();

    if (token.type === "NUMBER" || token.type === "STRING" || token.type === "BOOLEAN" || token.type === "NULL") {
      this.advance();
      return { type: "Literal", value: token.value };
    }

    if (token.type === "IDENTIFIER") {
      const name = String(this.advance().value);

      // Check if this is a function call: contains(name, "Ahmet")
      if (this.peek().type === "LPAREN") {
        this.advance(); // consume '('
        const args: ASTNode[] = [];

        if (this.peek().type !== "RPAREN") {
          args.push(this.parseLogicalOr());
          while (this.peek().type === "COMMA") {
            this.advance(); // consume ','
            args.push(this.parseLogicalOr());
          }
        }

        if (this.peek().type !== "RPAREN") {
          throw new InvalidArgumentError(
            `Expected closing ')' after function arguments at position ${this.peek().pos}`
          );
        }
        this.advance(); // consume ')'

        return { type: "CallExpr", callee: name, args };
      }

      return { type: "Identifier", name };
    }

    if (token.type === "LPAREN") {
      this.advance(); // consume '('
      const expr = this.parseLogicalOr();
      if (this.peek().type !== "RPAREN") {
        throw new InvalidArgumentError(
          `Expected closing ')' at position ${this.peek().pos}`
        );
      }
      this.advance(); // consume ')'
      return expr;
    }

    throw new InvalidArgumentError(
      `Unexpected token "${String(token.value)}" at position ${token.pos}`
    );
  }
}

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

function resolveIdentifier(name: string, parts: string[] | null, row: Row): unknown {
  if (!parts) {
    const val = row[name];
    return val === undefined || val === null || val === "" ? null : val;
  }

  if (name in row && row[name] !== undefined) {
    const val = row[name];
    return val === null || val === "" ? null : val;
  }

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

type CompiledNodeFn = (row: Row) => unknown;

function compileCallFunction(callee: string, argFns: CompiledNodeFn[]): CompiledNodeFn {
  const fnName = callee.toLowerCase();

  switch (fnName) {
    // --- 1. Set & Range ---
    case "in":
      return (row) => {
        const val = argFns[0]!(row);
        for (let i = 1; i < argFns.length; i++) {
          const t = argFns[i]!(row);
          if (String(t) === String(val) || (typeof t === "number" && Number(t) === Number(val))) return true;
        }
        return false;
      };

    case "notin":
      return (row) => {
        const val = argFns[0]!(row);
        for (let i = 1; i < argFns.length; i++) {
          const t = argFns[i]!(row);
          if (String(t) === String(val) || (typeof t === "number" && Number(t) === Number(val))) return false;
        }
        return true;
      };

    case "between": {
      const [valFn, minFn, maxFn] = argFns;
      return (row) => {
        const val = Number(valFn!(row));
        const min = Number(minFn!(row));
        const max = Number(maxFn!(row));
        if (Number.isNaN(val) || Number.isNaN(min) || Number.isNaN(max)) return false;
        return val >= min && val <= max;
      };
    }

    // --- 2. Date & Time ---
    case "year": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCFullYear();
      };
    }

    case "month": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCMonth() + 1;
      };
    }

    case "day": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCDate();
      };
    }

    case "hour": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCHours();
      };
    }

    case "minute": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCMinutes();
      };
    }

    case "dayofweek": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? null : d.getUTCDay();
      };
    }

    case "datediff": {
      const [d1Fn, d2Fn, unitFn] = argFns;
      return (row) => {
        const d1 = new Date(String(d1Fn!(row) ?? ""));
        const d2 = new Date(String(d2Fn!(row) ?? ""));
        if (Number.isNaN(d1.getTime()) || Number.isNaN(d2.getTime())) return null;

        const diffMs = d1.getTime() - d2.getTime();
        const unit = String(unitFn ? unitFn(row) ?? "days" : "days").toLowerCase();

        if (unit.startsWith("hour")) return Math.round(diffMs / (1000 * 60 * 60));
        if (unit.startsWith("minute")) return Math.round(diffMs / (1000 * 60));
        if (unit.startsWith("second")) return Math.round(diffMs / 1000);
        if (unit.startsWith("year")) return Math.round(diffMs / (1000 * 60 * 60 * 24 * 365.25));
        return Math.round(diffMs / (1000 * 60 * 60 * 24));
      };
    }

    case "ispast": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? false : d.getTime() < Date.now();
      };
    }

    case "isfuture": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return Number.isNaN(d.getTime()) ? false : d.getTime() > Date.now();
      };
    }

    case "istoday": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        if (Number.isNaN(d.getTime())) return false;
        const today = new Date();
        return (
          d.getUTCFullYear() === today.getUTCFullYear() &&
          d.getUTCMonth() === today.getUTCMonth() &&
          d.getUTCDate() === today.getUTCDate()
        );
      };
    }

    // --- 3. String & Text ---
    case "concat":
      return (row) => argFns.map((f) => {
        const a = f(row);
        return a === null || a === undefined ? "" : String(a);
      }).join("");

    case "substr":
    case "substring": {
      const [sFn, startFn, lenFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const start = Number(startFn ? startFn(row) ?? 0 : 0);
        const length = lenFn ? Number(lenFn(row)) : undefined;
        return length !== undefined ? str.slice(start, start + length) : str.slice(start);
      };
    }

    case "replace": {
      const [sFn, searchFn, repFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const search = String(searchFn!(row) ?? "");
        const replacement = String(repFn!(row) ?? "");
        return str.replaceAll(search, replacement);
      };
    }

    case "splitindex": {
      const [sFn, delimFn, idxFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const delimiter = String(delimFn ? delimFn(row) ?? "," : ",");
        const index = Number(idxFn ? idxFn(row) ?? 0 : 0);
        const parts = str.split(delimiter);
        return parts[index] ?? "";
      };
    }

    case "indexof": {
      const [sFn, searchFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const search = String(searchFn!(row) ?? "");
        return str.indexOf(search);
      };
    }

    case "padleft": {
      const [sFn, lenFn, charFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const len = Number(lenFn!(row) ?? 0);
        const char = String(charFn ? charFn(row) ?? " " : " ");
        return str.padStart(len, char);
      };
    }

    case "padright": {
      const [sFn, lenFn, charFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const len = Number(lenFn!(row) ?? 0);
        const char = String(charFn ? charFn(row) ?? " " : " ");
        return str.padEnd(len, char);
      };
    }

    case "contains": {
      const [sFn, tFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "").toLowerCase();
        const target = String(tFn!(row) ?? "").toLowerCase();
        return str.includes(target);
      };
    }

    case "startswith": {
      const [sFn, pFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const prefix = String(pFn!(row) ?? "");
        return str.startsWith(prefix);
      };
    }

    case "endswith": {
      const [sFn, pFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const suffix = String(pFn!(row) ?? "");
        return str.endsWith(suffix);
      };
    }

    case "lower": {
      const [fn] = argFns;
      return (row) => String(fn!(row) ?? "").toLowerCase();
    }

    case "upper": {
      const [fn] = argFns;
      return (row) => String(fn!(row) ?? "").toUpperCase();
    }

    case "trim": {
      const [fn] = argFns;
      return (row) => String(fn!(row) ?? "").trim();
    }

    case "length": {
      const [fn] = argFns;
      return (row) => String(fn!(row) ?? "").length;
    }

    case "matches": {
      const [sFn, pFn] = argFns;
      return (row) => {
        const str = String(sFn!(row) ?? "");
        const pattern = String(pFn!(row) ?? "");
        try {
          return new RegExp(pattern).test(str);
        } catch {
          return false;
        }
      };
    }

    // --- 4. Math & Numeric ---
    case "abs": {
      const [fn] = argFns;
      return (row) => Math.abs(Number(fn!(row) ?? 0));
    }

    case "round": {
      const [valFn, decFn] = argFns;
      return (row) => {
        const num = Number(valFn!(row) ?? 0);
        const decimals = decFn ? Number(decFn(row) ?? 0) : 0;
        const factor = 10 ** decimals;
        return Math.round(num * factor) / factor;
      };
    }

    case "ceil": {
      const [fn] = argFns;
      return (row) => Math.ceil(Number(fn!(row) ?? 0));
    }

    case "floor": {
      const [fn] = argFns;
      return (row) => Math.floor(Number(fn!(row) ?? 0));
    }

    case "clamp": {
      const [valFn, minFn, maxFn] = argFns;
      return (row) => {
        const num = Number(valFn!(row) ?? 0);
        const min = minFn ? Number(minFn(row) ?? Number.NEGATIVE_INFINITY) : Number.NEGATIVE_INFINITY;
        const max = maxFn ? Number(maxFn(row) ?? Number.POSITIVE_INFINITY) : Number.POSITIVE_INFINITY;
        return Math.min(Math.max(num, min), max);
      };
    }

    case "min": {
      if (argFns.length === 2) {
        const [aFn, bFn] = argFns;
        return (row) => Math.min(Number(aFn!(row) ?? 0), Number(bFn!(row) ?? 0));
      }
      return (row) => {
        let min = Number.POSITIVE_INFINITY;
        for (let i = 0; i < argFns.length; i++) {
          const v = Number(argFns[i]!(row) ?? 0);
          if (v < min) min = v;
        }
        return min;
      };
    }

    case "max": {
      if (argFns.length === 2) {
        const [aFn, bFn] = argFns;
        return (row) => Math.max(Number(aFn!(row) ?? 0), Number(bFn!(row) ?? 0));
      }
      return (row) => {
        let max = Number.NEGATIVE_INFINITY;
        for (let i = 0; i < argFns.length; i++) {
          const v = Number(argFns[i]!(row) ?? 0);
          if (v > max) max = v;
        }
        return max;
      };
    }

    case "sqrt": {
      const [fn] = argFns;
      return (row) => Math.sqrt(Number(fn!(row) ?? 0));
    }

    case "pow": {
      const [bFn, eFn] = argFns;
      return (row) => Number(bFn!(row) ?? 0) ** Number(eFn ? eFn(row) ?? 1 : 1);
    }

    case "log": {
      const [fn] = argFns;
      return (row) => Math.log(Number(fn!(row) ?? 1));
    }

    // --- 5. Control Flow & Nulls ---
    case "if":
    case "iif": {
      const [condFn, thenFn, elseFn] = argFns;
      return (row) => Boolean(condFn!(row)) ? thenFn!(row) : (elseFn ? elseFn(row) : null);
    }

    case "isnull": {
      const [fn] = argFns;
      return (row) => {
        const val = fn!(row);
        return val === null || val === undefined || val === "";
      };
    }

    case "isnotnull": {
      const [fn] = argFns;
      return (row) => {
        const val = fn!(row);
        return val !== null && val !== undefined && val !== "";
      };
    }

    case "coalesce":
      return (row) => {
        for (const f of argFns) {
          const val = f(row);
          if (val !== null && val !== undefined && val !== "") return val;
        }
        return null;
      };

    case "nullif": {
      const [aFn, bFn] = argFns;
      return (row) => {
        const a = aFn!(row);
        const b = bFn!(row);
        return a === b ? null : a;
      };
    }

    case "nvl": {
      const [aFn, bFn] = argFns;
      return (row) => {
        const a = aFn!(row);
        return a === null || a === undefined || a === "" ? bFn!(row) : a;
      };
    }

    // --- 6. Type Inspection & Conversion ---
    case "isnumber": {
      const [fn] = argFns;
      return (row) => {
        const val = fn!(row);
        if (val === null || val === undefined || val === "") return false;
        return !Number.isNaN(Number(val));
      };
    }

    case "isemail": {
      const [fn] = argFns;
      return (row) => detectSemanticType(fn!(row)) === "email";
    }

    case "isurl": {
      const [fn] = argFns;
      return (row) => detectSemanticType(fn!(row)) === "url";
    }

    case "isuuid": {
      const [fn] = argFns;
      return (row) => detectSemanticType(fn!(row)) === "uuid";
    }

    case "isdate": {
      const [fn] = argFns;
      return (row) => {
        const d = new Date(String(fn!(row) ?? ""));
        return !Number.isNaN(d.getTime());
      };
    }

    case "toint": {
      const [fn] = argFns;
      return (row) => {
        const num = Number.parseInt(String(fn!(row) ?? ""), 10);
        return Number.isNaN(num) ? null : num;
      };
    }

    case "tofloat": {
      const [fn] = argFns;
      return (row) => {
        const num = Number.parseFloat(String(fn!(row) ?? ""));
        return Number.isNaN(num) ? null : num;
      };
    }

    case "tostring": {
      const [fn] = argFns;
      return (row) => {
        const val = fn!(row);
        return val !== null && val !== undefined ? String(val) : "";
      };
    }

    case "tobool": {
      const [fn] = argFns;
      return (row) => {
        const s = String(fn!(row) ?? "").toLowerCase();
        return ["true", "1", "yes", "t", "y"].includes(s);
      };
    }

    case "formatbytes":
    case "prettybytes":
    case "humanbytes": {
      const [fn] = argFns;
      return (row) => formatBytes(Number(fn!(row) ?? 0));
    }

    case "parsebytes": {
      const [fn] = argFns;
      return (row) => parseBytes(fn!(row) as string | number);
    }

    case "formatnumber": {
      const [fn] = argFns;
      return (row) => formatNumber(Number(fn!(row) ?? 0));
    }

    // --- 7. JSON & Nested Access ---
    case "jsonget": {
      const [objFn, pathFn] = argFns;
      return (row) => getNestedValue(objFn!(row), String(pathFn!(row) ?? ""));
    }

    default:
      throw new InvalidArgumentError(`Unknown function "${callee}" in filter expression`);
  }
}

/**
 * Compiles an AST node into a high-performance closure with zero runtime AST traversals.
 */
function compileASTNode(node: ASTNode): CompiledNodeFn {
  switch (node.type) {
    case "Literal": {
      const val = node.value;
      return () => val;
    }

    case "Identifier": {
      const name = node.name;
      if (!name.includes(".")) {
        return (row: Row) => {
          const val = row[name];
          return val === undefined || val === "" ? null : val;
        };
      }
      const parts = name.split(".");
      return (row: Row) => resolveIdentifier(name, parts, row);
    }

    case "UnaryOp": {
      const argFn = compileASTNode(node.argument);
      if (node.operator === "!") {
        return (row: Row) => !argFn(row);
      }
      if (node.operator === "-") {
        return (row: Row) => -Number(argFn(row));
      }
      return () => false;
    }

    case "BinaryOp": {
      const leftNode = node.left;
      const rightNode = node.right;
      const leftFn = compileASTNode(leftNode);
      const rightFn = compileASTNode(rightNode);

      // JIT Fast Paths for Literal comparisons
      if (rightNode.type === "Literal") {
        const rVal = rightNode.value;
        if (typeof rVal === "number") {
          switch (node.operator) {
            case ">": return (row) => Number(leftFn(row)) > rVal;
            case ">=": return (row) => Number(leftFn(row)) >= rVal;
            case "<": return (row) => Number(leftFn(row)) < rVal;
            case "<=": return (row) => Number(leftFn(row)) <= rVal;
            case "==": return (row) => Number(leftFn(row)) === rVal;
            case "!=": return (row) => Number(leftFn(row)) !== rVal;
          }
        }
        if (typeof rVal === "string") {
          switch (node.operator) {
            case "==": return (row) => String(leftFn(row) ?? "") === rVal;
            case "!=": return (row) => String(leftFn(row) ?? "") !== rVal;
          }
        }
        if (typeof rVal === "boolean") {
          switch (node.operator) {
            case "==": return (row) => Boolean(leftFn(row)) === rVal;
            case "!=": return (row) => Boolean(leftFn(row)) !== rVal;
          }
        }
        if (rVal === null) {
          switch (node.operator) {
            case "==": return (row) => { const v = leftFn(row); return v === null || v === undefined || v === ""; };
            case "!=": return (row) => { const v = leftFn(row); return v !== null && v !== undefined && v !== ""; };
          }
        }
      }

      switch (node.operator) {
        case "&&":
          return (row) => Boolean(leftFn(row)) && Boolean(rightFn(row));

        case "||":
          return (row) => Boolean(leftFn(row)) || Boolean(rightFn(row));

        case "==":
          return (row) => {
            const leftVal = leftFn(row);
            const rightVal = rightFn(row);
            if (leftVal === rightVal) return true;
            if (leftVal === null || rightVal === null) return leftVal === rightVal;
            if (typeof leftVal === "number" || typeof rightVal === "number") {
              return Number(leftVal) === Number(rightVal);
            }
            return String(leftVal) === String(rightVal);
          };

        case "!=":
          return (row) => {
            const leftVal = leftFn(row);
            const rightVal = rightFn(row);
            if (leftVal === rightVal) return false;
            if (leftVal === null || rightVal === null) return leftVal !== rightVal;
            if (typeof leftVal === "number" || typeof rightVal === "number") {
              return Number(leftVal) !== Number(rightVal);
            }
            return String(leftVal) !== String(rightVal);
          };

        case ">":
          return (row) => {
            const leftVal = leftFn(row);
            if (leftVal === null) return false;
            const rightVal = rightFn(row);
            if (rightVal === null) return false;
            return Number(leftVal) > Number(rightVal);
          };

        case ">=":
          return (row) => {
            const leftVal = leftFn(row);
            if (leftVal === null) return false;
            const rightVal = rightFn(row);
            if (rightVal === null) return false;
            return Number(leftVal) >= Number(rightVal);
          };

        case "<":
          return (row) => {
            const leftVal = leftFn(row);
            if (leftVal === null) return false;
            const rightVal = rightFn(row);
            if (rightVal === null) return false;
            return Number(leftVal) < Number(rightVal);
          };

        case "<=":
          return (row) => {
            const leftVal = leftFn(row);
            if (leftVal === null) return false;
            const rightVal = rightFn(row);
            if (rightVal === null) return false;
            return Number(leftVal) <= Number(rightVal);
          };

        case "+":
          return (row) => {
            const leftVal = leftFn(row);
            const rightVal = rightFn(row);
            if (typeof leftVal === "string" || typeof rightVal === "string") {
              return String(leftVal ?? "") + String(rightVal ?? "");
            }
            return Number(leftVal ?? 0) + Number(rightVal ?? 0);
          };

        case "-":
          return (row) => Number(leftFn(row) ?? 0) - Number(rightFn(row) ?? 0);

        case "*":
          return (row) => Number(leftFn(row) ?? 0) * Number(rightFn(row) ?? 0);

        case "/":
          return (row) => Number(leftFn(row) ?? 0) / Number(rightFn(row) ?? 0);

        case "%":
          return (row) => Number(leftFn(row) ?? 0) % Number(rightFn(row) ?? 0);

        default:
          return () => false;
      }
    }

    case "CallExpr": {
      const argFns = node.args.map(compileASTNode);
      return compileCallFunction(node.callee, argFns);
    }
  }
}

/**
 * Compiles a filter expression string into an optimized predicate function.
 */
export function compileExpression(expressionStr: string): (row: Row) => boolean {
  if (!expressionStr || expressionStr.trim().length === 0) {
    return () => true;
  }

  const lexer = new Lexer(expressionStr);
  const tokens = lexer.tokenize();
  const parser = new Parser(tokens);
  const ast = parser.parse();

  // Try fast single-shot V8 JIT code generation first
  const jitFn = tryCompileJITPredicate(ast);
  if (jitFn) {
    return jitFn;
  }

  // Safe fallback to AST closure tree
  const compiled = compileASTNode(ast);
  return function (row: Row): boolean {
    return Boolean(compiled(row));
  };
}

/**
 * Compiles an expression string into an optimized value evaluator returning computed result (number, string, etc.).
 */
export function compileValueExpression(expressionStr: string): (row: Row) => unknown {
  if (!expressionStr || expressionStr.trim().length === 0) {
    return () => null;
  }

  const lexer = new Lexer(expressionStr);
  const tokens = lexer.tokenize();
  const parser = new Parser(tokens);
  const ast = parser.parse();

  // Try fast single-shot V8 JIT code generation first
  const jitFn = tryCompileJITValue(ast);
  if (jitFn) {
    return jitFn;
  }

  return compileASTNode(ast);
}

