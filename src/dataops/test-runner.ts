import type { DataBatch, DataStream, Row } from "../core/types.js";
import { compileExpression } from "../transforms/expression.js";

export interface DataTestRule {
  type: "assert" | "not_null" | "unique";
  target: string;
  description: string;
}

export interface RuleResult {
  rule: DataTestRule;
  passed: boolean;
  violationsCount: number;
  sampleViolations: Row[];
}

export interface DataTestSummary {
  totalRows: number;
  allPassed: boolean;
  rulesCount: number;
  passedRulesCount: number;
  failedRulesCount: number;
  results: RuleResult[];
}

export interface DataTestOptions {
  assert?: string | string[];
  notNull?: string | string[];
  unique?: string | string[];
  minRows?: number;
  maxRows?: number;
  maxErrors?: number;
  failFast?: boolean;
}

function normalizeList(val?: string | string[]): string[] {
  if (!val) return [];
  if (Array.isArray(val)) return val;
  return val.split(",").map((s) => s.trim()).filter(Boolean);
}

/**
 * Runs streaming quality tests and assertions across tabular data stream.
 */
export async function runDataTests(
  stream: DataStream,
  options: DataTestOptions = {}
): Promise<DataTestSummary> {
  const rules: DataTestRule[] = [];

  for (const expr of normalizeList(options.assert)) {
    rules.push({ type: "assert", target: expr, description: `ASSERT: ${expr}` });
  }

  for (const col of normalizeList(options.notNull)) {
    rules.push({ type: "not_null", target: col, description: `NOT NULL: ${col}` });
  }

  for (const col of normalizeList(options.unique)) {
    rules.push({ type: "unique", target: col, description: `UNIQUE: ${col}` });
  }

  const maxErrors = options.maxErrors || 5;
  const failFast = Boolean(options.failFast);

  // Precompile assert expressions
  const compiledAsserts = new Map<string, (row: Row) => boolean>();
  for (const r of rules) {
    if (r.type === "assert") {
      try {
        const fn = compileExpression(r.target);
        compiledAsserts.set(r.target, fn);
      } catch {
        compiledAsserts.set(r.target, () => false);
      }
    }
  }

  const uniqueSets = new Map<string, Set<string>>();
  for (const r of rules) {
    if (r.type === "unique") {
      uniqueSets.set(r.target, new Set());
    }
  }

  const results: RuleResult[] = rules.map((rule) => ({
    rule,
    passed: true,
    violationsCount: 0,
    sampleViolations: [],
  }));

  let totalRows = 0;
  let hasFailed = false;

  for await (const batch of stream) {
    for (const row of batch.rows) {
      totalRows++;

      for (let i = 0; i < rules.length; i++) {
        const rule = rules[i]!;
        const res = results[i]!;
        let isViolation = false;

        if (rule.type === "assert") {
          const checkFn = compiledAsserts.get(rule.target);
          const ok = checkFn ? checkFn(row) : false;
          if (!ok) {
            isViolation = true;
          }
        } else if (rule.type === "not_null") {
          const val = row[rule.target];
          if (val === null || val === undefined || val === "") {
            isViolation = true;
          }
        } else if (rule.type === "unique") {
          const val = row[rule.target];
          const key = val !== null && val !== undefined ? String(val) : "__null__";
          const set = uniqueSets.get(rule.target)!;
          if (set.has(key)) {
            isViolation = true;
          } else {
            set.add(key);
          }
        }

        if (isViolation) {
          res.passed = false;
          res.violationsCount++;
          hasFailed = true;
          if (res.sampleViolations.length < maxErrors) {
            res.sampleViolations.push(row);
          }
          if (failFast) break;
        }
      }

      if (failFast && hasFailed) break;
    }

    if (failFast && hasFailed) break;
  }

  // Row count constraints
  if (options.minRows !== undefined && totalRows < options.minRows) {
    results.push({
      rule: { type: "assert", target: `count >= ${options.minRows}`, description: `MIN ROWS: ${options.minRows}` },
      passed: false,
      violationsCount: options.minRows - totalRows,
      sampleViolations: [],
    });
  }

  if (options.maxRows !== undefined && totalRows > options.maxRows) {
    results.push({
      rule: { type: "assert", target: `count <= ${options.maxRows}`, description: `MAX ROWS: ${options.maxRows}` },
      passed: false,
      violationsCount: totalRows - options.maxRows,
      sampleViolations: [],
    });
  }

  const failedRulesCount = results.filter((r) => !r.passed).length;
  const passedRulesCount = results.length - failedRulesCount;

  return {
    totalRows,
    allPassed: failedRulesCount === 0,
    rulesCount: results.length,
    passedRulesCount,
    failedRulesCount,
    results,
  };
}
