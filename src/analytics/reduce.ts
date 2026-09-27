import { InvalidArgumentError } from "../core/errors.js";
import type { Aggregator, DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { compileValueExpression } from "../transforms/expression.js";
import { HyperLogLog } from "./stats.js";

export type AggregationFunction =
  | "sum"
  | "avg"
  | "mean"
  | "min"
  | "max"
  | "count"
  | "countdistinct"
  | "stddev"
  | "variance"
  | "first"
  | "last";

export interface AggregationSpec {
  targetField: string;
  func: AggregationFunction;
  sourceExpr?: string;
}

/**
 * Parses CLI reduce specs into structured AggregationSpec objects.
 * Supports syntax: "total_rev = sum(revenue)", "avg_margin = avg(margin)", "orders = count()"
 */
export function parseReduceSpecs(specs: string[]): AggregationSpec[] {
  const result: AggregationSpec[] = [];

  for (const spec of specs) {
    const trimmed = spec.trim();
    if (!trimmed) continue;

    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) {
      throw new InvalidArgumentError(
        `Invalid reduce specification "${spec}". Expected format "target_column = func(source_expr)"`
      );
    }

    const targetField = trimmed.slice(0, eqIdx).trim();
    const rhs = trimmed.slice(eqIdx + 1).trim();

    const match = rhs.match(/^([a-zA-Z_]+)\s*\((.*)\)$/);
    if (!match) {
      throw new InvalidArgumentError(
        `Invalid aggregation function call in "${spec}". Expected format "func(column)" e.g. "sum(revenue)" or "count()"`
      );
    }

    const rawFunc = match[1]!.toLowerCase();
    const sourceExpr = match[2]!.trim() || undefined;

    const validFuncs = [
      "sum",
      "avg",
      "mean",
      "min",
      "max",
      "count",
      "countdistinct",
      "stddev",
      "variance",
      "first",
      "last",
    ];

    if (!validFuncs.includes(rawFunc)) {
      throw new InvalidArgumentError(
        `Unknown aggregation function "${rawFunc}" in "${spec}". Supported functions: ${validFuncs.join(", ")}`
      );
    }

    result.push({
      targetField,
      func: rawFunc as AggregationFunction,
      sourceExpr,
    });
  }

  return result;
}

interface Accumulator {
  add(val: unknown): void;
  result(): unknown;
}

class SumAccumulator implements Accumulator {
  private sum = 0;
  add(val: unknown): void {
    if (val === null || val === undefined || val === "") return;
    const num = typeof val === "number" ? val : Number(val);
    if (num === num) {
      this.sum += num;
    }
  }
  result(): number {
    return this.sum;
  }
}

class AvgAccumulator implements Accumulator {
  private sum = 0;
  private count = 0;
  add(val: unknown): void {
    if (val === null || val === undefined || val === "") return;
    const num = typeof val === "number" ? val : Number(val);
    if (num === num) {
      this.sum += num;
      this.count++;
    }
  }
  result(): number | null {
    return this.count > 0 ? this.sum / this.count : null;
  }
}

class MinAccumulator implements Accumulator {
  private min: number | null = null;
  add(val: unknown): void {
    if (val === null || val === undefined || val === "") return;
    const num = typeof val === "number" ? val : Number(val);
    if (num === num) {
      if (this.min === null || num < this.min) this.min = num;
    }
  }
  result(): number | null {
    return this.min;
  }
}

class MaxAccumulator implements Accumulator {
  private max: number | null = null;
  add(val: unknown): void {
    if (val === null || val === undefined || val === "") return;
    const num = typeof val === "number" ? val : Number(val);
    if (num === num) {
      if (this.max === null || num > this.max) this.max = num;
    }
  }
  result(): number | null {
    return this.max;
  }
}

class CountAccumulator implements Accumulator {
  private count = 0;
  private hasSource: boolean;
  constructor(hasSource: boolean) {
    this.hasSource = hasSource;
  }
  add(val: unknown): void {
    if (!this.hasSource) {
      this.count++;
    } else if (val !== null && val !== undefined && val !== "") {
      this.count++;
    }
  }
  result(): number {
    return this.count;
  }
}

class CountDistinctAccumulator implements Accumulator {
  private hll = new HyperLogLog();
  add(val: unknown): void {
    if (val !== null && val !== undefined && val !== "") {
      this.hll.add(val);
    }
  }
  result(): number {
    return this.hll.count();
  }
}

class WelfordAccumulator implements Accumulator {
  private count = 0;
  private mean = 0;
  private M2 = 0;
  private mode: "stddev" | "variance";

  constructor(mode: "stddev" | "variance") {
    this.mode = mode;
  }

  add(val: unknown): void {
    if (val === null || val === undefined || val === "") return;
    const num = typeof val === "number" ? val : Number(val);
    if (num !== num) return;

    this.count++;
    const delta = num - this.mean;
    this.mean += delta / this.count;
    const delta2 = num - this.mean;
    this.M2 += delta * delta2;
  }

  result(): number | null {
    if (this.count <= 1) return this.count === 1 ? 0 : null;
    const variance = this.M2 / (this.count - 1);
    return this.mode === "stddev" ? Math.sqrt(variance) : variance;
  }
}

class FirstAccumulator implements Accumulator {
  private value: unknown = null;
  private found = false;
  add(val: unknown): void {
    if (!this.found && val !== null && val !== undefined && val !== "") {
      this.value = val;
      this.found = true;
    }
  }
  result(): unknown {
    return this.value;
  }
}

class LastAccumulator implements Accumulator {
  private value: unknown = null;
  add(val: unknown): void {
    if (val !== null && val !== undefined && val !== "") {
      this.value = val;
    }
  }
  result(): unknown {
    return this.value;
  }
}

function createAccumulatorFor(func: AggregationFunction, hasSource: boolean): Accumulator {
  switch (func) {
    case "sum":
      return new SumAccumulator();
    case "avg":
    case "mean":
      return new AvgAccumulator();
    case "min":
      return new MinAccumulator();
    case "max":
      return new MaxAccumulator();
    case "count":
      return new CountAccumulator(hasSource);
    case "countdistinct":
      return new CountDistinctAccumulator();
    case "stddev":
      return new WelfordAccumulator("stddev");
    case "variance":
      return new WelfordAccumulator("variance");
    case "first":
      return new FirstAccumulator();
    case "last":
      return new LastAccumulator();
  }
}

interface CompiledAggregation {
  targetField: string;
  func: AggregationFunction;
  directCol?: string;
  evaluator?: (row: Row) => unknown;
  createAccumulator: () => Accumulator;
}

export interface ReduceOptions {
  by?: string[];
  aggregations: AggregationSpec[] | string[];
}

/**
 * Streaming Reduce & Group-by Aggregator.
 */
export class ReduceAggregator implements Aggregator<Row[]> {
  private byCols: string[];
  private byColsCount: number;
  private col0?: string;
  private col1?: string;
  private compiled: CompiledAggregation[];
  private compiledCount: number;
  private globalAccs?: Accumulator[];
  private groupMap?: Map<string, { groupValues: Record<string, unknown>; accs: Accumulator[] }>;

  constructor(options: ReduceOptions) {
    this.byCols = options.by && options.by.length > 0 ? options.by : [];
    this.byColsCount = this.byCols.length;
    if (this.byColsCount === 1) {
      this.col0 = this.byCols[0];
    } else if (this.byColsCount === 2) {
      this.col0 = this.byCols[0];
      this.col1 = this.byCols[1];
    }

    const rawSpecs: AggregationSpec[] =
      typeof options.aggregations[0] === "string"
        ? parseReduceSpecs(options.aggregations as string[])
        : (options.aggregations as AggregationSpec[]);

    this.compiled = rawSpecs.map((spec) => {
      let directCol: string | undefined = undefined;
      let evaluator: ((row: Row) => unknown) | undefined = undefined;

      if (spec.sourceExpr) {
        if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(spec.sourceExpr)) {
          directCol = spec.sourceExpr;
        } else {
          evaluator = compileValueExpression(spec.sourceExpr);
        }
      }

      return {
        targetField: spec.targetField,
        func: spec.func,
        directCol,
        evaluator,
        createAccumulator: () => createAccumulatorFor(spec.func, Boolean(spec.sourceExpr)),
      };
    });
    this.compiledCount = this.compiled.length;

    if (this.byColsCount === 0) {
      this.globalAccs = this.compiled.map((c) => c.createAccumulator());
    } else {
      this.groupMap = new Map();
    }
  }

  private createGroupAccumulators(): Accumulator[] {
    const accs = new Array<Accumulator>(this.compiledCount);
    for (let i = 0; i < this.compiledCount; i++) {
      accs[i] = this.compiled[i]!.createAccumulator();
    }
    return accs;
  }

  add(row: Row): void {
    if (this.globalAccs) {
      const globalAccs = this.globalAccs;
      const compiled = this.compiled;
      const numCompiled = this.compiledCount;
      for (let i = 0; i < numCompiled; i++) {
        const item = compiled[i]!;
        const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
        globalAccs[i]!.add(val);
      }
      return;
    }

    if (this.groupMap) {
      let key: string;
      const byColsCount = this.byColsCount;
      if (byColsCount === 1) {
        const v0 = row[this.col0!] as unknown;
        key = v0 === null || v0 === undefined ? "" : (typeof v0 === "string" ? v0 : String(v0));
      } else if (byColsCount === 2) {
        const v0 = row[this.col0!] as unknown;
        const v1 = row[this.col1!] as unknown;
        const s0 = v0 === null || v0 === undefined ? "" : (typeof v0 === "string" ? v0 : String(v0));
        const s1 = v1 === null || v1 === undefined ? "" : (typeof v1 === "string" ? v1 : String(v1));
        key = s0 + "\x1f" + s1;
      } else {
        key = "";
        const byCols = this.byCols;
        for (let i = 0; i < byColsCount; i++) {
          const v = row[byCols[i]!] as unknown;
          const s = v === null || v === undefined ? "" : (typeof v === "string" ? v : String(v));
          key += (i > 0 ? "\x1f" : "") + s;
        }
      }

      let group = this.groupMap.get(key);
      if (!group) {
        const groupValues: Record<string, unknown> = {};
        const byCols = this.byCols;
        for (let i = 0; i < byColsCount; i++) {
          const col = byCols[i]!;
          groupValues[col] = row[col] ?? null;
        }
        group = {
          groupValues,
          accs: this.createGroupAccumulators(),
        };
        this.groupMap.set(key, group);
      }

      const accs = group.accs;
      const numCompiled = this.compiledCount;
      const compiled = this.compiled;
      for (let i = 0; i < numCompiled; i++) {
        const item = compiled[i]!;
        const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
        accs[i]!.add(val);
      }
    }
  }

  addBatch(rows: Row[]): void {
    const len = rows.length;
    if (len === 0) return;

    if (this.globalAccs) {
      const globalAccs = this.globalAccs;
      const compiled = this.compiled;
      const numCompiled = this.compiledCount;

      for (let r = 0; r < len; r++) {
        const row = rows[r]!;
        for (let i = 0; i < numCompiled; i++) {
          const item = compiled[i]!;
          const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
          globalAccs[i]!.add(val);
        }
      }
      return;
    }

    if (!this.groupMap) return;

    const groupMap = this.groupMap;
    const compiled = this.compiled;
    const numCompiled = this.compiledCount;
    const byColsCount = this.byColsCount;

    if (byColsCount === 1) {
      const col0 = this.col0!;
      for (let r = 0; r < len; r++) {
        const row = rows[r]!;
        const v0 = row[col0] as unknown;
        const key = v0 === null || v0 === undefined ? "" : (typeof v0 === "string" ? v0 : String(v0));
        let group = groupMap.get(key);
        if (!group) {
          const groupValues: Record<string, unknown> = { [col0]: v0 ?? null };
          group = {
            groupValues,
            accs: this.createGroupAccumulators(),
          };
          groupMap.set(key, group);
        }
        const accs = group.accs;
        for (let i = 0; i < numCompiled; i++) {
          const item = compiled[i]!;
          const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
          accs[i]!.add(val);
        }
      }
      return;
    }

    if (byColsCount === 2) {
      const col0 = this.col0!;
      const col1 = this.col1!;
      for (let r = 0; r < len; r++) {
        const row = rows[r]!;
        const v0 = row[col0] as unknown;
        const v1 = row[col1] as unknown;
        const s0 = v0 === null || v0 === undefined ? "" : (typeof v0 === "string" ? v0 : String(v0));
        const s1 = v1 === null || v1 === undefined ? "" : (typeof v1 === "string" ? v1 : String(v1));
        const key = s0 + "\x1f" + s1;
        let group = groupMap.get(key);
        if (!group) {
          const groupValues: Record<string, unknown> = {
            [col0]: v0 ?? null,
            [col1]: v1 ?? null,
          };
          group = {
            groupValues,
            accs: this.createGroupAccumulators(),
          };
          groupMap.set(key, group);
        }
        const accs = group.accs;
        for (let i = 0; i < numCompiled; i++) {
          const item = compiled[i]!;
          const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
          accs[i]!.add(val);
        }
      }
      return;
    }

    // General case: 3 or more grouping columns
    const byCols = this.byCols;
    for (let r = 0; r < len; r++) {
      const row = rows[r]!;
      let key = "";
      for (let i = 0; i < byColsCount; i++) {
        const v = row[byCols[i]!] as unknown;
        const s = v === null || v === undefined ? "" : (typeof v === "string" ? v : String(v));
        key += (i > 0 ? "\x1f" : "") + s;
      }
      let group = groupMap.get(key);
      if (!group) {
        const groupValues: Record<string, unknown> = {};
        for (let i = 0; i < byColsCount; i++) {
          const col = byCols[i]!;
          groupValues[col] = row[col] ?? null;
        }
        group = {
          groupValues,
          accs: this.createGroupAccumulators(),
        };
        groupMap.set(key, group);
      }
      const accs = group.accs;
      for (let i = 0; i < numCompiled; i++) {
        const item = compiled[i]!;
        const val = item.directCol !== undefined ? row[item.directCol] : (item.evaluator ? item.evaluator(row) : null);
        accs[i]!.add(val);
      }
    }
  }

  result(): Row[] {
    if (this.globalAccs) {
      const summaryRow: Row = {};
      const numCompiled = this.compiledCount;
      const compiled = this.compiled;
      const globalAccs = this.globalAccs;
      for (let i = 0; i < numCompiled; i++) {
        summaryRow[compiled[i]!.targetField] = globalAccs[i]!.result();
      }
      return [summaryRow];
    }

    if (this.groupMap) {
      const rows: Row[] = [];
      const numCompiled = this.compiledCount;
      const compiled = this.compiled;
      for (const group of this.groupMap.values()) {
        const outRow: Row = { ...group.groupValues };
        const accs = group.accs;
        for (let i = 0; i < numCompiled; i++) {
          outRow[compiled[i]!.targetField] = accs[i]!.result();
        }
        rows.push(outRow);
      }
      return rows;
    }

    return [];
  }
}

/**
 * Creates a high-performance streaming transform that reduces/aggregates rows.
 */
export function reduceRows(options: ReduceOptions): TransformFunction {
  return async function* (stream: DataStream): DataStream {
    const aggregator = new ReduceAggregator(options);

    for await (const batch of stream) {
      aggregator.addBatch(batch.rows);
    }

    const finalRows = aggregator.result();

    yield {
      rows: finalRows,
      offset: 0,
    };
  };
}
