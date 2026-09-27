import type { Aggregator, Row } from "../core/types.js";

/**
 * Fast 32-bit FNV-1a hash function with 32-bit avalanche mixing for strings.
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
 * Fast zero-allocation 32-bit integer & float mixer for numeric hashing without string allocations.
 */
function hashNumber32(n: number): number {
  let h: number;
  if ((n | 0) === n) {
    h = (n ^ (n >>> 16) ^ 0x811c9dc5) >>> 0;
  } else if (Number.isInteger(n)) {
    const lo = n | 0;
    const hi = (n / 4294967296) | 0;
    h = (lo ^ hi ^ 0x811c9dc5) >>> 0;
  } else {
    h = (((n * 100000) | 0) ^ 0x811c9dc5) >>> 0;
  }
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
  private mask: number;
  private maxZeros: number;
  private pMinusOne: number;
  private registers: Uint8Array;
  private alphaMM: number;

  constructor(p = 10) {
    this.p = p;
    this.m = 1 << p;
    this.mask = this.m - 1;
    this.maxZeros = 33 - p;
    this.pMinusOne = p - 1;
    this.registers = new Uint8Array(this.m);

    // Alpha constant calculation
    if (this.m === 16) this.alphaMM = 0.673 * this.m * this.m;
    else if (this.m === 32) this.alphaMM = 0.697 * this.m * this.m;
    else if (this.m === 64) this.alphaMM = 0.709 * this.m * this.m;
    else this.alphaMM = (0.7213 / (1 + 1.079 / this.m)) * this.m * this.m;
  }

  addNumber(n: number): void {
    const hash = hashNumber32(n);
    const index = hash & this.mask;
    const w = hash >>> this.p;
    const leadingZeros = w === 0 ? this.maxZeros : Math.clz32(w) - this.pMinusOne;
    if (leadingZeros > this.registers[index]!) {
      this.registers[index] = leadingZeros;
    }
  }

  addString(str: string): void {
    const hash = hash32(str);
    const index = hash & this.mask;
    const w = hash >>> this.p;
    const leadingZeros = w === 0 ? this.maxZeros : Math.clz32(w) - this.pMinusOne;
    if (leadingZeros > this.registers[index]!) {
      this.registers[index] = leadingZeros;
    }
  }

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    if (typeof value === "number") {
      this.addNumber(value);
    } else if (typeof value === "string") {
      this.addString(value);
    } else {
      this.addString(String(value));
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

  addNumber(num: number): void {
    this.count++;
    this.sum += num;
    this.hll.addNumber(num);

    if (this.min === null || num < this.min) this.min = num;
    if (this.max === null || num > this.max) this.max = num;

    // Welford's online algorithm
    const delta = num - this.mean;
    this.mean += delta / this.count;
    const delta2 = num - this.mean;
    this.M2 += delta * delta2;
  }

  addNull(): void {
    this.nullCount++;
  }

  add(val: unknown): void {
    if (val === null || val === undefined || val === "") {
      this.nullCount++;
      return;
    }

    const num = typeof val === "number" ? val : Number(val);
    if (Number.isNaN(num) || typeof val === "boolean") {
      this.nullCount++;
      return;
    }

    this.addNumber(num);
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

  addString(str: string): void {
    const len = str.length;
    if (len === 0) {
      this.emptyCount++;
    }

    this.count++;
    this.totalLength += len;
    this.hll.addString(str);

    if (this.minLength === null || len < this.minLength) this.minLength = len;
    if (this.maxLength === null || len > this.maxLength) this.maxLength = len;

    // Track top values bounded
    const currentCount = this.frequencyMap.get(str);
    if (currentCount !== undefined) {
      this.frequencyMap.set(str, currentCount + 1);
    } else if (this.frequencyMap.size < this.maxTrackedValues) {
      this.frequencyMap.set(str, 1);
    }
  }

  addNull(): void {
    this.nullCount++;
  }

  add(val: unknown): void {
    if (val === null || val === undefined) {
      this.nullCount++;
      return;
    }

    this.addString(typeof val === "string" ? val : String(val));
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
  hint: { numericVotes: number; totalVotes: number; lockedType?: "numeric" | "string" };
}

/**
 * Dataset-wide Aggregator collecting statistics across all columns in stream.
 */
export class DatasetStatsAggregator implements Aggregator<DatasetStatsResult> {
  private totalRows = 0;
  private targetColumn?: string;
  private numericCollectors = new Map<string, NumericStatsCollector>();
  private stringCollectors = new Map<string, StringStatsCollector>();
  private columnTypeHints = new Map<string, { numericVotes: number; totalVotes: number; lockedType?: "numeric" | "string" }>();
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

      if (h.hint.lockedType === "numeric") {
        if (val === null || val === undefined || val === "") {
          h.numCollector.addNull();
        } else if (typeof val === "number") {
          h.numCollector.addNumber(val);
        } else if (typeof val === "boolean") {
          h.numCollector.addNull();
        } else {
          const num = Number(val);
          if (!Number.isNaN(num)) {
            h.numCollector.addNumber(num);
          } else {
            h.numCollector.addNull();
          }
        }
      } else if (h.hint.lockedType === "string") {
        if (val === null || val === undefined) {
          h.strCollector.addNull();
        } else if (typeof val === "string") {
          h.strCollector.addString(val);
        } else {
          h.strCollector.addString(String(val));
        }
      } else {
        // Warmup sampling phase (first 200 rows)
        if (val === null || val === undefined) {
          h.strCollector.addNull();
          h.numCollector.addNull();
        } else {
          const str = typeof val === "string" ? val : String(val);
          h.strCollector.addString(str);

          if (str !== "") {
            h.hint.totalVotes++;
            if (typeof val === "number") {
              h.hint.numericVotes++;
              h.numCollector.addNumber(val);
            } else if (typeof val !== "boolean") {
              const num = Number(str);
              if (!Number.isNaN(num)) {
                h.hint.numericVotes++;
                h.numCollector.addNumber(num);
              } else {
                h.numCollector.addNull();
              }
            } else {
              h.numCollector.addNull();
            }
          } else {
            h.numCollector.addNull();
          }
        }

        if (h.hint.totalVotes >= 200) {
          if (h.hint.numericVotes / h.hint.totalVotes >= 0.8) {
            h.hint.lockedType = "numeric";
          } else {
            h.hint.lockedType = "string";
          }
        }
      }
    }
  }

  result(): DatasetStatsResult {
    const columns: Record<string, ColumnStatsResult> = {};

    for (const [col, hint] of this.columnTypeHints.entries()) {
      const isNumeric = hint.lockedType === "numeric" || (hint.totalVotes > 0 && hint.numericVotes / hint.totalVotes >= 0.8);
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
