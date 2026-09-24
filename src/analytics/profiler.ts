import type { Aggregator, ColumnType, Row, SemanticType } from "../core/types.js";
import { classifyPrimitiveType } from "./schema-inference.js";
import { detectSemanticType } from "./semantic-types.js";
import { HyperLogLog } from "./stats.js";
import { formatNumber, formatTable } from "../utils/formatting.js";

export interface ColumnProfileResult {
  name: string;
  inferredType: ColumnType;
  typeConfidence: number;
  typeBreakdown: Record<string, number>;
  anomalies: unknown[];
  totalRows: number;
  nullCount: number;
  nullPercentage: number;
  emptyStringCount: number;
  approxDistinctCount: number;
  distinctPercentage: number;
  semanticType?: SemanticType;
  semanticConfidence?: number;

  // Numeric stats (if applicable)
  isNumeric: boolean;
  min?: number;
  max?: number;
  sum?: number;
  mean?: number;
  stddev?: number;
  zerosCount?: number;
  negativeCount?: number;

  // String stats (if applicable)
  isString: boolean;
  minLength?: number;
  maxLength?: number;
  avgLength?: number;
  topValues?: Array<{ value: string; count: number; percentage: number }>;
}

export interface DatasetProfileResult {
  totalRows: number;
  totalColumns: number;
  columns: ColumnProfileResult[];
}

class ColumnProfiler {
  readonly name: string;
  totalCount = 0;
  nullCount = 0;
  emptyStringCount = 0;
  typeCounts: Record<ColumnType, number> = {
    string: 0,
    integer: 0,
    bigint: 0,
    number: 0,
    decimal: 0,
    boolean: 0,
    date: 0,
    datetime: 0,
    binary: 0,
    json: 0,
    null: 0,
    mixed: 0,
  };
  semanticCounts: Record<string, number> = {};
  hll = new HyperLogLog(10);
  frequencies = new Map<string, number>();

  // Numeric tracking (Welford's algorithm)
  numericCount = 0;
  numMin = Number.POSITIVE_INFINITY;
  numMax = Number.NEGATIVE_INFINITY;
  numSum = 0;
  welfordMean = 0;
  welfordM2 = 0;
  zerosCount = 0;
  negativeCount = 0;

  // String tracking
  stringCount = 0;
  strMinLen = Number.POSITIVE_INFINITY;
  strMaxLen = 0;
  strSumLen = 0;

  // Anomaly tracking
  anomalies: unknown[] = [];

  constructor(name: string) {
    this.name = name;
  }

  add(val: unknown): void {
    this.totalCount++;

    if (val === null || val === undefined) {
      this.nullCount++;
      this.typeCounts.null++;
      return;
    }

    if (val === "") {
      this.emptyStringCount++;
      this.typeCounts.null++;
      return;
    }

    const valStr = String(val);
    this.hll.add(valStr);

    // Track top frequency for up to 1000 distinct items
    if (this.frequencies.size < 1000 || this.frequencies.has(valStr)) {
      this.frequencies.set(valStr, (this.frequencies.get(valStr) || 0) + 1);
    }

    const primType = classifyPrimitiveType(val);
    this.typeCounts[primType] = (this.typeCounts[primType] || 0) + 1;

    // Semantic type detection
    if (typeof val === "string" && val.length > 2) {
      const sem = detectSemanticType(val);
      if (sem) {
        this.semanticCounts[sem] = (this.semanticCounts[sem] || 0) + 1;
      }
    }

    // Numeric aggregation
    const numVal = typeof val === "number" ? val : (typeof val === "string" && /^-?\d+(?:\.\d+)?$/.test(val.trim()) ? Number(val) : NaN);
    if (!Number.isNaN(numVal)) {
      this.numericCount++;
      this.numSum += numVal;
      if (numVal < this.numMin) this.numMin = numVal;
      if (numVal > this.numMax) this.numMax = numVal;
      if (numVal === 0) this.zerosCount++;
      if (numVal < 0) this.negativeCount++;

      // Welford's algorithm for online variance
      const delta = numVal - this.welfordMean;
      this.welfordMean += delta / this.numericCount;
      const delta2 = numVal - this.welfordMean;
      this.welfordM2 += delta * delta2;
    }

    // String aggregation
    if (typeof val === "string") {
      this.stringCount++;
      const len = val.length;
      if (len < this.strMinLen) this.strMinLen = len;
      if (len > this.strMaxLen) this.strMaxLen = len;
      this.strSumLen += len;
    }
  }

  computeResult(): ColumnProfileResult {
    const nonNullRows = this.totalCount - this.nullCount - this.emptyStringCount;
    let dominantType: ColumnType = "string";
    let maxTypeCount = 0;

    for (const [t, count] of Object.entries(this.typeCounts)) {
      if (t === "null") continue;
      if (count > maxTypeCount) {
        maxTypeCount = count;
        dominantType = t as ColumnType;
      }
    }

    const typeConfidence = nonNullRows > 0 ? maxTypeCount / nonNullRows : 1;
    const isNumeric = dominantType === "integer" || dominantType === "number" || dominantType === "decimal" || dominantType === "bigint";
    const isString = dominantType === "string" || dominantType === "date" || dominantType === "datetime";

    // Top values calculation
    const sortedFreq = Array.from(this.frequencies.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([value, count]) => ({
        value,
        count,
        percentage: this.totalCount > 0 ? Math.round((count / this.totalCount) * 1000) / 10 : 0,
      }));

    // Semantic type
    let semanticType: SemanticType | undefined;
    let semanticConfidence: number | undefined;
    let maxSemCount = 0;
    for (const [sem, count] of Object.entries(this.semanticCounts)) {
      if (count > maxSemCount) {
        maxSemCount = count;
        semanticType = sem as SemanticType;
      }
    }
    if (semanticType && nonNullRows > 0) {
      semanticConfidence = Math.round((maxSemCount / nonNullRows) * 100) / 100;
    }

    const approxDistinct = this.hll.count();

    const res: ColumnProfileResult = {
      name: this.name,
      inferredType: dominantType,
      typeConfidence: Math.round(typeConfidence * 100) / 100,
      typeBreakdown: { ...this.typeCounts },
      anomalies: this.anomalies,
      totalRows: this.totalCount,
      nullCount: this.nullCount + this.emptyStringCount,
      nullPercentage: this.totalCount > 0 ? Math.round(((this.nullCount + this.emptyStringCount) / this.totalCount) * 1000) / 10 : 0,
      emptyStringCount: this.emptyStringCount,
      approxDistinctCount: approxDistinct,
      distinctPercentage: this.totalCount > 0 ? Math.round((approxDistinct / this.totalCount) * 1000) / 10 : 0,
      semanticType,
      semanticConfidence,
      isNumeric,
      isString,
      topValues: sortedFreq,
    };

    if (isNumeric && this.numericCount > 0) {
      res.min = this.numMin === Number.POSITIVE_INFINITY ? undefined : this.numMin;
      res.max = this.numMax === Number.NEGATIVE_INFINITY ? undefined : this.numMax;
      res.sum = this.numSum;
      res.mean = this.welfordMean;
      res.stddev = this.numericCount > 1 ? Math.sqrt(this.welfordM2 / (this.numericCount - 1)) : 0;
      res.zerosCount = this.zerosCount;
      res.negativeCount = this.negativeCount;
    }

    if (isString && this.stringCount > 0) {
      res.minLength = this.strMinLen === Number.POSITIVE_INFINITY ? 0 : this.strMinLen;
      res.maxLength = this.strMaxLen;
      res.avgLength = Math.round((this.strSumLen / this.stringCount) * 10) / 10;
    }

    return res;
  }
}

/**
 * Single-pass Streaming Dataset Profiler.
 */
export class DatasetProfiler implements Aggregator<DatasetProfileResult> {
  private columnProfilers = new Map<string, ColumnProfiler>();
  private totalRows = 0;

  add(row: Row): void {
    this.totalRows++;
    for (const [col, val] of Object.entries(row)) {
      let profiler = this.columnProfilers.get(col);
      if (!profiler) {
        profiler = new ColumnProfiler(col);
        this.columnProfilers.set(col, profiler);
      }
      profiler.add(val);
    }
  }

  result(): DatasetProfileResult {
    const columns: ColumnProfileResult[] = [];
    for (const profiler of this.columnProfilers.values()) {
      columns.push(profiler.computeResult());
    }

    return {
      totalRows: this.totalRows,
      totalColumns: columns.length,
      columns,
    };
  }
}

/**
 * Formats profile results as an aligned terminal table.
 */
export function formatProfileTerminal(profile: DatasetProfileResult): string {
  const headers = ["COLUMN", "TYPE", "DISTINCT", "NULLS (%)", "MIN / MIN-LEN", "MAX / MAX-LEN", "MEAN / TOP-VALUE"];
  const rows = profile.columns.map((c) => {
    const nullStr = `${formatNumber(c.nullCount)} (${c.nullPercentage}%)`;
    const distinctStr = `${formatNumber(c.approxDistinctCount)} (${c.distinctPercentage}%)`;
    let minStr = "-";
    let maxStr = "-";
    let meanStr = "-";

    if (c.isNumeric && c.min !== undefined && c.max !== undefined) {
      minStr = String(c.min);
      maxStr = String(c.max);
      meanStr = c.mean !== undefined ? (Math.round(c.mean * 100) / 100).toString() : "-";
    } else if (c.isString) {
      minStr = `${c.minLength ?? 0} chars`;
      maxStr = `${c.maxLength ?? 0} chars`;
      if (c.topValues && c.topValues.length > 0) {
        meanStr = `${c.topValues[0]!.value} (${c.topValues[0]!.count})`;
      }
    }

    return [
      c.name,
      c.semanticType ? `${c.inferredType} (${c.semanticType})` : c.inferredType,
      distinctStr,
      nullStr,
      minStr,
      maxStr,
      meanStr,
    ];
  });

  let out = `\n======================================================\n`;
  out += `  Rowpipe Dataset Profile (${formatNumber(profile.totalRows)} rows, ${profile.totalColumns} columns)\n`;
  out += `======================================================\n\n`;
  out += formatTable(headers, rows);
  out += "\n";
  return out;
}

/**
 * Formats profile results as a GitHub Flavored Markdown document.
 */
export function formatProfileMarkdown(profile: DatasetProfileResult): string {
  let md = `# Dataset Profile Report\n\n`;
  md += `* **Total Rows**: ${formatNumber(profile.totalRows)}\n`;
  md += `* **Total Columns**: ${profile.totalColumns}\n\n`;
  md += `| Column | Type | Distinct | Nulls (%) | Min | Max | Mean / Top Value |\n`;
  md += `| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n`;

  for (const c of profile.columns) {
    const typeStr = c.semanticType ? `${c.inferredType} (${c.semanticType})` : c.inferredType;
    const nullStr = `${formatNumber(c.nullCount)} (${c.nullPercentage}%)`;
    const distinctStr = `${formatNumber(c.approxDistinctCount)} (${c.distinctPercentage}%)`;
    let minStr = "-";
    let maxStr = "-";
    let meanStr = "-";

    if (c.isNumeric && c.min !== undefined && c.max !== undefined) {
      minStr = String(c.min);
      maxStr = String(c.max);
      meanStr = c.mean !== undefined ? (Math.round(c.mean * 100) / 100).toString() : "-";
    } else if (c.isString) {
      minStr = `${c.minLength ?? 0} chars`;
      maxStr = `${c.maxLength ?? 0} chars`;
      if (c.topValues && c.topValues.length > 0) {
        meanStr = `${c.topValues[0]!.value} (${c.topValues[0]!.count})`;
      }
    }

    md += `| **${c.name}** | \`${typeStr}\` | ${distinctStr} | ${nullStr} | ${minStr} | ${maxStr} | ${meanStr} |\n`;
  }

  return md;
}
