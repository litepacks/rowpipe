import type { Aggregator, Row } from "../core/types.js";

/**
 * Fast 32-bit FNV-1a hash function with 32-bit avalanche mixing.
 * Zero BigInt allocations, runs at native CPU register speed.
 */
function hash32(str: string): number {
  let h = 0x811c9dc5;
  const len = str.length;
  for (let i = 0; i < len; i++) {
    h = (h ^ str.charCodeAt(i)) >>> 0;
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  // Avalanche mixing
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b) >>> 0;
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35) >>> 0;
  h ^= h >>> 16;
  return h >>> 0;
}

/**
 * HyperLogLog (HLL) distinct count estimator with bounded O(1) memory.
 * Uses m = 1024 registers (p = 10), standard error ~3.25%.
 * Hardware Math.clz32 for instantaneous zero counting.
 */
export class HyperLogLog {
  private p: number;
  private m: number;
  private registers: Uint8Array;
  private alphaMM: number;

  constructor(p = 10) {
    this.p = p;
    this.m = 1 << p;
    this.registers = new Uint8Array(this.m);

    // Alpha constant calculation
    if (this.m === 16) this.alphaMM = 0.673 * this.m * this.m;
    else if (this.m === 32) this.alphaMM = 0.697 * this.m * this.m;
    else if (this.m === 64) this.alphaMM = 0.709 * this.m * this.m;
    else this.alphaMM = (0.7213 / (1 + 1.079 / this.m)) * this.m * this.m;
  }

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    const str = String(value);
    const hash = hash32(str);

    // Low p bits for register index
    const index = hash & (this.m - 1);
    // Remaining (32 - p) bits for leading zeros
    const w = hash >>> this.p;

    // Single-cycle CPU instruction Math.clz32 for leading zero count
    let leadingZeros: number;
    if (w === 0) {
      leadingZeros = 32 - this.p + 1;
    } else {
      leadingZeros = Math.clz32(w) - this.p + 1;
    }

    if (leadingZeros > this.registers[index]!) {
      this.registers[index] = leadingZeros;
    }
  }

  merge(other: HyperLogLog): void {
    for (let i = 0; i < this.m; i++) {
      if (other.registers[i]! > this.registers[i]!) {
        this.registers[i] = other.registers[i]!;
      }
    }
  }

  count(): number {
    let sum = 0;
    let zeros = 0;

    for (let i = 0; i < this.m; i++) {
      const val = this.registers[i]!;
      sum += 2 ** -val;
      if (val === 0) zeros++;
    }

    let estimate = this.alphaMM / sum;

    // Small range correction (Linear Counting)
    if (estimate <= 2.5 * this.m && zeros > 0) {
      estimate = this.m * Math.log(this.m / zeros);
    }

    return Math.round(estimate);
  }
}

export interface NumericColumnStats {
  count: number;
  nullCount: number;
  min: number | null;
  max: number | null;
  sum: number;
  mean: number | null;
  variance: number | null;
  stddev: number | null;
  approxDistinct: number;
}

export interface StringColumnStats {
  count: number;
  nullCount: number;
  emptyCount: number;
  minLength: number | null;
  maxLength: number | null;
  avgLength: number | null;
  emptyCountRatio?: number;
  approxDistinct: number;
  topValues: Array<{ value: string; count: number }>;
}

export interface ColumnStatsResult {
  column: string;
  type: "numeric" | "string";
  numeric?: NumericColumnStats;
  string?: StringColumnStats;
}

export interface DatasetStatsResult {
  totalRows: number;
  columns: Record<string, ColumnStatsResult>;
}

/**
 * Online Welford statistics collector for a numeric column.
 */
export class NumericStatsCollector {
  public count = 0;
  public nullCount = 0;
  public min: number | null = null;
  public max: number | null = null;
  public sum = 0;
  public mean = 0;
  public M2 = 0; // sum of squared differences from the mean
  public hll = new HyperLogLog();

  add(val: unknown): void {
    if (val === null || val === undefined || val === "") {
      this.nullCount++;
      return;
    }

    const num = typeof val === "number" ? val : Number(val);
    if (Number.isNaN(num)) {
      this.nullCount++;
      return;
    }

    this.count++;
    this.sum += num;
    this.hll.add(num);

    if (this.min === null || num < this.min) this.min = num;
    if (this.max === null || num > this.max) this.max = num;

    // Welford's online algorithm
    const delta = num - this.mean;
    this.mean += delta / this.count;
    const delta2 = num - this.mean;
    this.M2 += delta * delta2;
  }

  merge(other: NumericStatsCollector): void {
    if (other.count === 0) {
      this.nullCount += other.nullCount;
      return;
    }
    if (this.count === 0) {
      this.count = other.count;
      this.nullCount = other.nullCount;
      this.min = other.min;
      this.max = other.max;
      this.sum = other.sum;
      this.mean = other.mean;
      this.M2 = other.M2;
      this.hll.merge(other.hll);
      return;
    }

    const totalCount = this.count + other.count;
    const delta = other.mean - this.mean;

    this.mean = (this.count * this.mean + other.count * other.mean) / totalCount;
    this.M2 = this.M2 + other.M2 + (delta * delta * this.count * other.count) / totalCount;
    this.count = totalCount;
    this.nullCount += other.nullCount;
    this.sum += other.sum;

    if (this.min === null || (other.min !== null && other.min < this.min)) this.min = other.min;
    if (this.max === null || (other.max !== null && other.max > this.max)) this.max = other.max;
    this.hll.merge(other.hll);
  }

  result(): NumericColumnStats {
    const variance = this.count > 1 ? this.M2 / (this.count - 1) : this.count === 1 ? 0 : null;
    const stddev = variance !== null ? Math.sqrt(variance) : null;

    return {
      count: this.count,
      nullCount: this.nullCount,
      min: this.min,
      max: this.max,
      sum: this.sum,
      mean: this.count > 0 ? this.mean : null,
      variance,
      stddev,
      approxDistinct: this.hll.count(),
    };
  }
}

/**
 * Online statistics collector for a string column.
 */
export class StringStatsCollector {
  public count = 0;
  public nullCount = 0;
  public emptyCount = 0;
  public minLength: number | null = null;
  public maxLength: number | null = null;
  public totalLength = 0;
  public hll = new HyperLogLog();
  private frequencyMap = new Map<string, number>();
  private maxTrackedValues = 1000;

  add(val: unknown): void {
    if (val === null || val === undefined) {
      this.nullCount++;
      return;
    }

    const str = String(val);
    if (str === "") {
      this.emptyCount++;
    }

    this.count++;
    this.totalLength += str.length;
    this.hll.add(str);

    if (this.minLength === null || str.length < this.minLength) this.minLength = str.length;
    if (this.maxLength === null || str.length > this.maxLength) this.maxLength = str.length;

    // Track top values bounded
    if (this.frequencyMap.size < this.maxTrackedValues || this.frequencyMap.has(str)) {
      this.frequencyMap.set(str, (this.frequencyMap.get(str) || 0) + 1);
    }
  }

  result(): StringColumnStats {
    const sorted = Array.from(this.frequencyMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([value, count]) => ({ value, count }));

    return {
      count: this.count,
      nullCount: this.nullCount,
      emptyCount: this.emptyCount,
      minLength: this.minLength,
      maxLength: this.maxLength,
      avgLength: this.count > 0 ? this.totalLength / this.count : null,
      approxDistinct: this.hll.count(),
      topValues: sorted,
    };
  }
}

interface ColumnHandler {
  col: string;
  numCollector: NumericStatsCollector;
  strCollector: StringStatsCollector;
  hint: { numericVotes: number; totalVotes: number };
}

/**
 * Dataset-wide Aggregator collecting statistics across all columns in stream.
 */
export class DatasetStatsAggregator implements Aggregator<DatasetStatsResult> {
  private totalRows = 0;
  private targetColumn?: string;
  private numericCollectors = new Map<string, NumericStatsCollector>();
  private stringCollectors = new Map<string, StringStatsCollector>();
  private columnTypeHints = new Map<string, { numericVotes: number; totalVotes: number }>();
  private handlers: ColumnHandler[] = [];
  private initialized = false;

  constructor(options?: { column?: string }) {
    this.targetColumn = options?.column;
  }

  private initHandler(col: string): ColumnHandler {
    let numCollector = this.numericCollectors.get(col);
    let strCollector = this.stringCollectors.get(col);
    let hint = this.columnTypeHints.get(col);

    if (!numCollector) {
      numCollector = new NumericStatsCollector();
      strCollector = new StringStatsCollector();
      hint = { numericVotes: 0, totalVotes: 0 };
      this.numericCollectors.set(col, numCollector);
      this.stringCollectors.set(col, strCollector);
      this.columnTypeHints.set(col, hint);
    }

    return { col, numCollector: numCollector!, strCollector: strCollector!, hint: hint! };
  }

  add(row: Row): void {
    this.totalRows++;

    if (!this.initialized) {
      const columns = this.targetColumn ? [this.targetColumn] : Object.keys(row);
      this.handlers = new Array(columns.length);
      for (let i = 0; i < columns.length; i++) {
        this.handlers[i] = this.initHandler(columns[i]!);
      }
      this.initialized = true;
    }

    const handlers = this.handlers;
    const len = handlers.length;

    for (let i = 0; i < len; i++) {
      const h = handlers[i]!;
      const val = row[h.col];

      h.strCollector.add(val);

      if (val !== null && val !== undefined && val !== "") {
        h.hint.totalVotes++;
        const num = typeof val === "number" ? val : Number(val);
        if (!Number.isNaN(num) && typeof val !== "boolean") {
          h.hint.numericVotes++;
          h.numCollector.add(num);
        } else {
          h.numCollector.add(null);
        }
      } else {
        h.numCollector.add(null);
      }
    }
  }

  result(): DatasetStatsResult {
    const columns: Record<string, ColumnStatsResult> = {};

    for (const [col, hint] of this.columnTypeHints.entries()) {
      const isNumeric = hint.totalVotes > 0 && hint.numericVotes / hint.totalVotes >= 0.8;
      const numCollector = this.numericCollectors.get(col)!;
      const strCollector = this.stringCollectors.get(col)!;

      if (isNumeric) {
        columns[col] = {
          column: col,
          type: "numeric",
          numeric: numCollector.result(),
        };
      } else {
        columns[col] = {
          column: col,
          type: "string",
          string: strCollector.result(),
        };
      }
    }

    return {
      totalRows: this.totalRows,
      columns,
    };
  }
}
