import { parseReduceSpecs, reduceRows, AggregationSpec } from "../analytics/reduce.js";
import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

export interface GroupOptions {
  by?: string | string[];
  count?: boolean;
  sum?: string | string[];
  avg?: string | string[];
  min?: string | string[];
  max?: string | string[];
  first?: string | string[];
  last?: string | string[];
  agg?: string | string[];
  memoryLimit?: number | string;
  tempDir?: string;
  batchSize?: number;
}

/**
 * Builds reduce specifications from group flags and explicit specs.
 */
export function buildGroupReduceSpecs(options: GroupOptions): AggregationSpec[] {
  const rawSpecs: string[] = [];

  if (options.count) {
    rawSpecs.push("count=count()");
  }

  const addFieldAggs = (func: string, fields?: string | string[], suffix?: string) => {
    if (!fields) return;
    const list = Array.isArray(fields) ? fields : fields.split(",").map((s) => s.trim()).filter(Boolean);
    for (const f of list) {
      const alias = suffix ? `${f}_${suffix}` : `${f}_${func}`;
      rawSpecs.push(`${alias}=${func}(${f})`);
    }
  };

  addFieldAggs("sum", options.sum, "sum");
  addFieldAggs("avg", options.avg, "avg");
  addFieldAggs("min", options.min, "min");
  addFieldAggs("max", options.max, "max");
  addFieldAggs("first", options.first, "first");
  addFieldAggs("last", options.last, "last");

  if (options.agg) {
    if (Array.isArray(options.agg)) {
      for (const a of options.agg) {
        rawSpecs.push(...parseRawAggStrings(a));
      }
    } else {
      rawSpecs.push(...parseRawAggStrings(options.agg));
    }
  }

  if (rawSpecs.length === 0) {
    // Default to count() if grouped without explicit aggregations
    rawSpecs.push("count=count()");
  }

  return parseReduceSpecs(rawSpecs);
}

function parseRawAggStrings(input: string): string[] {
  const parts = input.split(",").map((s) => s.trim()).filter(Boolean);
  return parts.map((p) => {
    if (p.includes("=")) return p;
    // Auto-generate alias: sum(revenue) -> revenue_sum=sum(revenue), count() -> count=count()
    const match = /^([a-zA-Z0-9_]+)\((.*)\)$/.exec(p);
    if (match) {
      const fn = match[1]!;
      const col = match[2]?.trim();
      if (!col) return `${fn}=${p}`;
      return `${col}_${fn}=${p}`;
    }
    return p;
  });
}

/**
 * Groups and aggregates tabular stream with streaming single-pass accumulation.
 */
export function groupRows(options: GroupOptions = {}): TransformFunction {
  const byCols = options.by
    ? (Array.isArray(options.by) ? options.by : options.by.split(",").map((s) => s.trim()).filter(Boolean))
    : undefined;

  const aggregations = buildGroupReduceSpecs(options);

  return reduceRows({
    by: byCols,
    aggregations,
  });
}
