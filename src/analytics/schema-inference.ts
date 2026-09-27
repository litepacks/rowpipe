import type { Aggregator, ColumnType, Row, SemanticType } from "../core/types.js";
import { detectSemanticType } from "./semantic-types.js";

export interface ColumnInferenceResult {
  name: string;
  type: ColumnType;
  nullable: boolean;
  confidence: number;
  semantic?: SemanticType;
  semanticConfidence?: number;
  typeBreakdown: Record<string, number>;
  sampleCount: number;
  nullCount: number;
}

export interface InferredSchemaResult {
  totalRowsScanned: number;
  columns: ColumnInferenceResult[];
}

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const DATETIME_REGEX = /^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/;

/**
 * Tests primitive type for an individual value.
 */
export function classifyPrimitiveType(value: unknown): ColumnType {
  if (value === null || value === undefined || value === "") {
    return "null";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? "integer" : "number";
  }
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "bigint") return "bigint";
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return "binary";
  }
  if (typeof value === "object") {
    if (value instanceof Date) return "datetime";
    return "json";
  }

  const str = typeof value === "string" ? value.trim() : String(value).trim();
  const len = str.length;
  if (len === 0) return "null";

  // Fast boolean check (zero array allocation)
  if (
    str === "true" ||
    str === "false" ||
    str === "TRUE" ||
    str === "FALSE" ||
    str === "True" ||
    str === "False"
  ) {
    return "boolean";
  }

  // Fast integer check
  let isInt = true;
  let i = 0;
  if (str.charCodeAt(0) === 45 /* - */) {
    if (len === 1) isInt = false;
    i = 1;
  }
  for (; i < len; i++) {
    const c = str.charCodeAt(i);
    if (c < 48 || c > 57) {
      isInt = false;
      break;
    }
  }
  if (isInt && len > 0 && !(len === 1 && str.charCodeAt(0) === 45)) {
    return "integer";
  }

  // Floating point regex
  if (/^-?\d+\.\d+$/.test(str) || /^-?\d+(?:\.\d+)?e[+-]?\d+$/i.test(str)) {
    return "number";
  }

  // Date / Datetime (guarded by length to avoid expensive regex/date parsing on arbitrary strings)
  if (len === 10 && DATE_REGEX.test(str)) {
    const d = new Date(str);
    if (!Number.isNaN(d.getTime())) return "date";
  }

  if (len >= 19 && len <= 35 && DATETIME_REGEX.test(str)) {
    const d = new Date(str);
    if (!Number.isNaN(d.getTime())) return "datetime";
  }

  return "string";
}

export class SchemaInferenceAggregator implements Aggregator<InferredSchemaResult> {
  private totalRows = 0;
  private maxSample: number;
  private columnStats = new Map<
    string,
    {
      counts: Record<ColumnType, number>;
      semanticCounts: Record<string, number>;
      nullCount: number;
      totalCount: number;
    }
  >();

  constructor(options?: { sample?: number }) {
    this.maxSample = options?.sample ?? Number.POSITIVE_INFINITY;
  }

  add(row: Row): void {
    if (this.totalRows >= this.maxSample) return;
    this.totalRows++;

    for (const col in row) {
      let stat = this.columnStats.get(col);
      if (!stat) {
        stat = {
          counts: {
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
          },
          semanticCounts: {},
          nullCount: 0,
          totalCount: 0,
        };
        this.columnStats.set(col, stat);
      }

      stat.totalCount++;
      const val = row[col];
      const pType = classifyPrimitiveType(val);
      stat.counts[pType]++;
      if (pType === "null") {
        stat.nullCount++;
      } else if (pType === "string") {
        const sem = detectSemanticType(val);
        if (sem) {
          stat.semanticCounts[sem] = (stat.semanticCounts[sem] || 0) + 1;
        }
      }
    }
  }

  result(): InferredSchemaResult {
    const columns: ColumnInferenceResult[] = [];

    for (const [col, stat] of this.columnStats.entries()) {
      const nonNullCount = stat.totalCount - stat.nullCount;
      const nullable = stat.nullCount > 0;

      if (nonNullCount === 0) {
        columns.push({
          name: col,
          type: "null",
          nullable: true,
          confidence: 100,
          typeBreakdown: { null: 100 },
          sampleCount: stat.totalCount,
          nullCount: stat.nullCount,
        });
        continue;
      }

      // Calculate confidence percentages
      const breakdown: Record<string, number> = {};
      let dominantType: ColumnType = "string";
      let maxPercentage = 0;

      const candidates: ColumnType[] = [
        "integer",
        "number",
        "boolean",
        "datetime",
        "date",
        "string",
      ];

      for (const t of candidates) {
        const pct = (stat.counts[t] / nonNullCount) * 100;
        if (pct > 0) {
          breakdown[t] = Math.round(pct * 10) / 10;
        }
        if (pct > maxPercentage) {
          maxPercentage = pct;
          dominantType = t;
        }
      }

      // If integer + number together make 100%, consider number
      if (
        dominantType === "integer" &&
        stat.counts["number"] > 0 &&
        (stat.counts["integer"] + stat.counts["number"]) / nonNullCount >= 0.95
      ) {
        dominantType = "number";
        maxPercentage = ((stat.counts["integer"] + stat.counts["number"]) / nonNullCount) * 100;
      }

      let inferredType: ColumnType = dominantType;
      let confidence = Math.round(maxPercentage * 10) / 10;

      if (maxPercentage < 90 && Object.keys(breakdown).length > 1) {
        inferredType = "mixed";
      }

      // Semantic type detection
      let semantic: SemanticType | undefined;
      let semanticConfidence: number | undefined;

      for (const [sem, count] of Object.entries(stat.semanticCounts)) {
        const semPct = (count / nonNullCount) * 100;
        if (semPct >= 80) {
          semantic = sem as SemanticType;
          semanticConfidence = Math.round(semPct * 10) / 10;
          break;
        }
      }

      columns.push({
        name: col,
        type: inferredType,
        nullable,
        confidence,
        semantic,
        semanticConfidence,
        typeBreakdown: breakdown,
        sampleCount: stat.totalCount,
        nullCount: stat.nullCount,
      });
    }

    return {
      totalRowsScanned: this.totalRows,
      columns,
    };
  }
}
