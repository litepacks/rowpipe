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
    if (!Number.isNaN(num)) {
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
    if (!Number.isNaN(num)) {
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
    if (!Number.isNaN(num)) {
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
    if (!Number.isNaN(num)) {
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
    if (Number.isNaN(num)) return;

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
  private compiled: CompiledAggregation[];
  private globalAccs?: Accumulator[];
  private groupMap?: Map<string, { groupValues: Record<string, unknown>; accs: Accumulator[] }>;

  constructor(options: ReduceOptions) {
    this.byCols = options.by && options.by.length > 0 ? options.by : [];

    const rawSpecs: AggregationSpec[] =
      typeof options.aggregations[0] === "string"
        ? parseReduceSpecs(options.aggregations as string[])
        : (options.aggregations as AggregationSpec[]);

    this.compiled = rawSpecs.map((spec) => ({
      targetField: spec.targetField,
      func: spec.func,
      evaluator: spec.sourceExpr ? compileValueExpression(spec.sourceExpr) : undefined,
      createAccumulator: () => createAccumulatorFor(spec.func, Boolean(spec.sourceExpr)),
    }));

    if (this.byCols.length === 0) {
      this.globalAccs = this.compiled.map((c) => c.createAccumulator());
    } else {
      this.groupMap = new Map();
    }
  }

  add(row: Row): void {
    if (this.globalAccs) {
      // Global reduction
      for (let i = 0; i < this.compiled.length; i++) {
        const item = this.compiled[i]!;
        const val = item.evaluator ? item.evaluator(row) : null;
        this.globalAccs[i]!.add(val);
      }
    } else if (this.groupMap) {
      // Group-by reduction
      let key = "";
      const groupValues: Record<string, unknown> = {};

      for (let i = 0; i < this.byCols.length; i++) {
        const col = this.byCols[i]!;
        const val = row[col] ?? null;
        groupValues[col] = val;
        key += (i > 0 ? "\x1f" : "") + String(val ?? "");
      }

      let group = this.groupMap.get(key);
      if (!group) {
        group = {
          groupValues,
          accs: this.compiled.map((c) => c.createAccumulator()),
        };
        this.groupMap.set(key, group);
      }

      for (let i = 0; i < this.compiled.length; i++) {
        const item = this.compiled[i]!;
        const val = item.evaluator ? item.evaluator(row) : null;
        group.accs[i]!.add(val);
      }
    }
  }

  result(): Row[] {
    if (this.globalAccs) {
      const summaryRow: Row = {};
      for (let i = 0; i < this.compiled.length; i++) {
        const item = this.compiled[i]!;
        summaryRow[item.targetField] = this.globalAccs[i]!.result();
      }
      return [summaryRow];
    }

    if (this.groupMap) {
      const rows: Row[] = [];
      for (const group of this.groupMap.values()) {
        const outRow: Row = { ...group.groupValues };
        for (let i = 0; i < this.compiled.length; i++) {
          const item = this.compiled[i]!;
          outRow[item.targetField] = group.accs[i]!.result();
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
      const len = batch.rows.length;
      for (let i = 0; i < len; i++) {
        aggregator.add(batch.rows[i]!);
      }
    }

    const finalRows = aggregator.result();

    yield {
      rows: finalRows,
      offset: 0,
    };
  };
}
