import type { Row, DataBatch } from "../core/types.js";

export interface AbTestOptions {
  groupCol?: string;
  metricCol?: string;
  controlGroup?: string;
  variantGroup?: string;
  type?: "auto" | "proportion" | "means";
  confidenceLevel?: number; // default: 0.95
}

export interface AbTestResult {
  test_type: "Two-Proportion Z-Test" | "Welch's Two-Sample T-Test";
  control_group: string;
  variant_group: string;
  control_sample_size: number;
  variant_sample_size: number;
  control_metric: number; // conversion rate or mean
  variant_metric: number;
  absolute_difference: number;
  relative_lift_pct: number;
  test_statistic: number; // Z-score or t-score
  statistic_name: "Z-score" | "t-score";
  p_value: number;
  confidence_level: number;
  ci_lower: number;
  ci_upper: number;
  significant: boolean;
  verdict: string;
  formattedRows: Row[];
}

/**
 * Standard Normal Cumulative Distribution Function Φ(x) using Abramowitz and Stegun approximation
 */
export function normalCdf(z: number): number {
  const p = 0.2316419;
  const b1 = 0.31938153;
  const b2 = -0.356563782;
  const b3 = 1.781477937;
  const b4 = -1.821255978;
  const b5 = 1.330274429;

  const absZ = Math.abs(z);
  const t = 1.0 / (1.0 + p * absZ);
  const pdf = (1.0 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * z * z);
  const cdf = 1.0 - pdf * (b1 * t + b2 * Math.pow(t, 2) + b3 * Math.pow(t, 3) + b4 * Math.pow(t, 4) + b5 * Math.pow(t, 5));

  return z >= 0 ? cdf : 1.0 - cdf;
}

/**
 * Student's t-distribution two-tailed p-value approximation (Hill 1970 / Normal limit for large df)
 */
export function studentTPValue(t: number, df: number): number {
  if (df <= 0) return 1.0;
  const absT = Math.abs(t);
  if (df >= 100) {
    // Normal approximation for large degrees of freedom
    return 2 * (1 - normalCdf(absT));
  }
  // Standard approximation for moderate df
  const x = df / (df + absT * absT);
  // Regularized incomplete beta function approximation via standard series
  let sum = 0;
  const numSteps = 50;
  for (let i = 0; i < numSteps; i++) {
    // simple integration approximation
    const u = (i + 0.5) / numSteps;
    sum += Math.pow(u, df / 2 - 1) * Math.pow(1 - u * x, -0.5);
  }
  return Math.min(1.0, Math.max(0.0, 2 * (1 - normalCdf(absT * (1 - 1 / (4 * df))))));
}

function parseBinaryValue(val: unknown): number | null {
  if (val === null || val === undefined) return null;
  if (typeof val === "boolean") return val ? 1 : 0;
  if (typeof val === "number") return val === 1 ? 1 : val === 0 ? 0 : null;
  if (typeof val === "string") {
    const s = val.trim().toLowerCase();
    if (s === "1" || s === "true" || s === "yes" || s === "y" || s === "converted" || s === "success") return 1;
    if (s === "0" || s === "false" || s === "no" || s === "n" || s === "dropped" || s === "failed") return 0;
  }
  return null;
}

export async function computeAbTest(
  batchIterator: AsyncIterable<DataBatch>,
  options: AbTestOptions = {}
): Promise<AbTestResult> {
  let groupCol = options.groupCol;
  let metricCol = options.metricCol;

  // Track group stats
  interface GroupAccumulator {
    n: number;
    sum: number;
    mean: number;
    m2: number; // For Welford variance
    binaryCount: number;
  }

  const groups = new Map<string, GroupAccumulator>();

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!groupCol || !metricCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);

      if (!groupCol) {
        groupCol =
          keys.find((k) => /^(group|variant|experiment|test_group|cohort|treatment|ab|arm)$/i.test(k)) ??
          keys[0];
      }
      if (!metricCol) {
        metricCol =
          keys.find((k) => /(converted|conversion|revenue|metric|value|score|amount|spend|clicks|click)/i.test(k)) ??
          keys[1];
      }
    }

    for (const row of batch.rows) {
      const rawG = row[groupCol!];
      if (rawG === null || rawG === undefined || rawG === "") continue;
      const g = String(rawG).trim();

      const rawVal = row[metricCol!];
      let numVal: number | null = null;
      if (typeof rawVal === "number") {
        numVal = rawVal;
      } else {
        const bin = parseBinaryValue(rawVal);
        if (bin !== null) {
          numVal = bin;
        } else {
          const parsed = parseFloat(String(rawVal));
          if (!isNaN(parsed)) numVal = parsed;
        }
      }

      if (numVal === null) continue;

      let acc = groups.get(g);
      if (!acc) {
        acc = { n: 0, sum: 0, mean: 0, m2: 0, binaryCount: 0 };
        groups.set(g, acc);
      }

      acc.n += 1;
      acc.sum += numVal;
      if (numVal === 0 || numVal === 1) {
        acc.binaryCount += 1;
      }

      // Welford update
      const delta = numVal - acc.mean;
      acc.mean += delta / acc.n;
      const delta2 = numVal - acc.mean;
      acc.m2 += delta * delta2;
    }
  }

  const groupKeys = Array.from(groups.keys());
  if (groupKeys.length < 2) {
    throw new Error(
      `A/B test requires at least 2 distinct groups in column '${groupCol}', but found ${groupKeys.length} (${groupKeys.join(", ")})`
    );
  }

  // Determine control and variant
  let ctrlName = options.controlGroup;
  let varName = options.variantGroup;

  if (!ctrlName) {
    ctrlName =
      groupKeys.find((k) => /^(control|ctrl|base|baseline|a|orig|original)$/i.test(k)) ??
      groupKeys[0]!;
  }
  if (!varName) {
    varName =
      groupKeys.find((k) => k !== ctrlName && /^(variant|var|treatment|treat|b|test|new)$/i.test(k)) ??
      groupKeys.find((k) => k !== ctrlName)!;
  }

  const ctrl = groups.get(ctrlName);
  const variant = groups.get(varName);

  if (!ctrl || !variant) {
    throw new Error(`Could not find specified groups: Control='${ctrlName}', Variant='${varName}'`);
  }

  // Check if test is proportion (binary) or means (continuous)
  const isBinary =
    options.type === "proportion" ||
    (options.type !== "means" &&
      ctrl.binaryCount === ctrl.n &&
      variant.binaryCount === variant.n);

  const confLevel = options.confidenceLevel ?? 0.95;
  const zCrit = confLevel === 0.99 ? 2.576 : confLevel === 0.90 ? 1.645 : 1.96;

  if (isBinary) {
    // Two-Proportion Z-Test
    const n1 = ctrl.n;
    const x1 = ctrl.sum;
    const p1 = n1 > 0 ? x1 / n1 : 0;

    const n2 = variant.n;
    const x2 = variant.sum;
    const p2 = n2 > 0 ? x2 / n2 : 0;

    const pPooled = (x1 + x2) / (n1 + n2);
    const sePooled = Math.sqrt(pPooled * (1 - pPooled) * (1 / n1 + 1 / n2));
    const zScore = sePooled > 0 ? (p2 - p1) / sePooled : 0;
    const pValue = 2 * (1 - normalCdf(Math.abs(zScore)));

    const diff = p2 - p1;
    const liftPct = p1 > 0 ? ((p2 - p1) / p1) * 100 : 0;

    const seDiff = Math.sqrt((p1 * (1 - p1)) / n1 + (p2 * (1 - p2)) / n2);
    const ciLower = diff - zCrit * seDiff;
    const ciUpper = diff + zCrit * seDiff;

    const isSig = pValue < 1 - confLevel;
    const verdict = isSig
      ? liftPct > 0
        ? `Statistically Significant Winner (${(liftPct > 0 ? "+" : "") + liftPct.toFixed(2)}% lift, p = ${pValue.toFixed(4)})`
        : `Statistically Significant Loser (${liftPct.toFixed(2)}% lift, p = ${pValue.toFixed(4)})`
      : `Inconclusive / Not Statistically Significant (p = ${pValue.toFixed(4)} >= ${(1 - confLevel).toFixed(2)})`;

    const formattedRows: Row[] = [
      { metric: "test_type", value: "Two-Proportion Z-Test (Binary Conversion)" },
      { metric: "control_group", value: `${ctrlName} (n=${n1.toLocaleString()})` },
      { metric: "variant_group", value: `${varName} (n=${n2.toLocaleString()})` },
      { metric: "control_rate", value: `${(p1 * 100).toFixed(2)}% (${x1}/${n1})` },
      { metric: "variant_rate", value: `${(p2 * 100).toFixed(2)}% (${x2}/${n2})` },
      { metric: "absolute_diff", value: `${diff >= 0 ? "+" : ""}${(diff * 100).toFixed(2)}%` },
      { metric: "relative_lift", value: `${liftPct >= 0 ? "+" : ""}${liftPct.toFixed(2)}%` },
      { metric: "z_statistic", value: Math.round(zScore * 10000) / 10000 },
      { metric: "p_value", value: Math.round(pValue * 10000) / 10000 },
      { metric: "confidence_level", value: `${(confLevel * 100).toFixed(0)}%` },
      { metric: "confidence_interval", value: `[${(ciLower * 100).toFixed(2)}%, ${(ciUpper * 100).toFixed(2)}%]` },
      { metric: "verdict", value: verdict },
    ];

    return {
      test_type: "Two-Proportion Z-Test",
      control_group: ctrlName,
      variant_group: varName,
      control_sample_size: n1,
      variant_sample_size: n2,
      control_metric: p1,
      variant_metric: p2,
      absolute_difference: diff,
      relative_lift_pct: liftPct,
      test_statistic: zScore,
      statistic_name: "Z-score",
      p_value: pValue,
      confidence_level: confLevel,
      ci_lower: ciLower,
      ci_upper: ciUpper,
      significant: isSig,
      verdict,
      formattedRows,
    };
  } else {
    // Welch's Two-Sample T-Test (Continuous)
    const n1 = ctrl.n;
    const mean1 = ctrl.mean;
    const var1 = n1 > 1 ? ctrl.m2 / (n1 - 1) : 0;

    const n2 = variant.n;
    const mean2 = variant.mean;
    const var2 = n2 > 1 ? variant.m2 / (n2 - 1) : 0;

    const diff = mean2 - mean1;
    const liftPct = mean1 !== 0 ? (diff / mean1) * 100 : 0;

    const seDiff = Math.sqrt(var1 / n1 + var2 / n2);
    const tScore = seDiff > 0 ? diff / seDiff : 0;

    // Welch-Satterthwaite df
    const numerator = Math.pow(var1 / n1 + var2 / n2, 2);
    const denom =
      Math.pow(var1 / n1, 2) / (n1 - 1 || 1) + Math.pow(var2 / n2, 2) / (n2 - 1 || 1);
    const df = denom > 0 ? numerator / denom : 1;

    const pValue = studentTPValue(tScore, df);
    const ciLower = diff - zCrit * seDiff;
    const ciUpper = diff + zCrit * seDiff;

    const isSig = pValue < 1 - confLevel;
    const verdict = isSig
      ? liftPct > 0
        ? `Statistically Significant Increase (${(liftPct > 0 ? "+" : "") + liftPct.toFixed(2)}% lift, p = ${pValue.toFixed(4)})`
        : `Statistically Significant Decrease (${liftPct.toFixed(2)}% lift, p = ${pValue.toFixed(4)})`
      : `Inconclusive / Not Statistically Significant (p = ${pValue.toFixed(4)} >= ${(1 - confLevel).toFixed(2)})`;

    const formattedRows: Row[] = [
      { metric: "test_type", value: "Welch's Two-Sample T-Test (Continuous Means)" },
      { metric: "control_group", value: `${ctrlName} (n=${n1.toLocaleString()})` },
      { metric: "variant_group", value: `${varName} (n=${n2.toLocaleString()})` },
      { metric: "control_mean", value: Math.round(mean1 * 1000) / 1000 },
      { metric: "variant_mean", value: Math.round(mean2 * 1000) / 1000 },
      { metric: "absolute_diff", value: `${diff >= 0 ? "+" : ""}${Math.round(diff * 1000) / 1000}` },
      { metric: "relative_lift", value: `${liftPct >= 0 ? "+" : ""}${liftPct.toFixed(2)}%` },
      { metric: "t_statistic", value: Math.round(tScore * 10000) / 10000 },
      { metric: "degrees_of_freedom", value: Math.round(df * 10) / 10 },
      { metric: "p_value", value: Math.round(pValue * 10000) / 10000 },
      { metric: "confidence_level", value: `${(confLevel * 100).toFixed(0)}%` },
      { metric: "confidence_interval", value: `[${Math.round(ciLower * 1000) / 1000}, ${Math.round(ciUpper * 1000) / 1000}]` },
      { metric: "verdict", value: verdict },
    ];

    return {
      test_type: "Welch's Two-Sample T-Test",
      control_group: ctrlName,
      variant_group: varName,
      control_sample_size: n1,
      variant_sample_size: n2,
      control_metric: mean1,
      variant_metric: mean2,
      absolute_difference: diff,
      relative_lift_pct: liftPct,
      test_statistic: tScore,
      statistic_name: "t-score",
      p_value: pValue,
      confidence_level: confLevel,
      ci_lower: ciLower,
      ci_upper: ciUpper,
      significant: isSig,
      verdict,
      formattedRows,
    };
  }
}
