import type { Row, DataBatch } from "../core/types.js";

export interface EntropyOptions {
  targetCol?: string;
  cols?: string | string[];
  bins?: number; // Number of bins for numeric discretization (default: 10)
}

export interface FeatureImportanceResult {
  feature: string;
  entropy: number;
  information_gain: number;
  gain_ratio: number;
  importance_score: number; // Normalized 0-100 score
  distinct_values: number;
}

export interface EntropyAnalysisResult {
  target_column?: string;
  target_entropy?: number;
  total_samples: number;
  features: FeatureImportanceResult[];
  formattedRows: Row[];
}

function discretizeNumeric(val: number, min: number, max: number, bins: number): string {
  if (min === max) return "bin_0";
  const normalized = Math.max(0, Math.min(0.9999, (val - min) / (max - min)));
  const binIdx = Math.floor(normalized * bins);
  return `bin_${binIdx}`;
}

export async function computeEntropy(
  batchIterator: AsyncIterable<DataBatch>,
  options: EntropyOptions = {}
): Promise<EntropyAnalysisResult> {
  const bins = options.bins ?? 10;
  let targetCol = options.targetCol;
  let targetCols: string[] | undefined;

  if (options.cols) {
    targetCols = Array.isArray(options.cols)
      ? options.cols.flatMap((c) => c.split(",").map((s) => s.trim()))
      : options.cols.split(",").map((s) => s.trim());
  }

  // First pass: collect distinct value counts, min/max for numerics, and joint counts
  const singleCounts = new Map<string, Map<string, number>>();
  const jointCounts = new Map<string, Map<string, Map<string, number>>>(); // feature -> targetVal -> featVal -> count
  const numericRanges = new Map<string, { min: number; max: number }>();
  let totalSamples = 0;

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!targetCols) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);
      if (!targetCol && keys.length > 1) {
        targetCol =
          keys.find((k) => /(target|label|class|churn|churned|survived|y|outcome|status)/i.test(k)) ??
          keys[keys.length - 1];
      }
      targetCols = keys.filter((k) => k !== targetCol);
    }

    for (const row of batch.rows) {
      totalSamples++;

      for (const col of [targetCol, ...targetCols].filter(Boolean) as string[]) {
        const val = row[col];
        if (typeof val === "number") {
          const rng = numericRanges.get(col);
          if (!rng) {
            numericRanges.set(col, { min: val, max: val });
          } else {
            if (val < rng.min) rng.min = val;
            if (val > rng.max) rng.max = val;
          }
        }
      }

      // Track target single counts
      let targetValStr = "";
      if (targetCol) {
        const rawT = row[targetCol];
        targetValStr = rawT !== null && rawT !== undefined ? String(rawT) : "<null>";
        let tMap = singleCounts.get(targetCol);
        if (!tMap) {
          tMap = new Map<string, number>();
          singleCounts.set(targetCol, tMap);
        }
        tMap.set(targetValStr, (tMap.get(targetValStr) || 0) + 1);
      }

      // Track feature single & joint counts
      for (const col of targetCols) {
        const rawVal = row[col];
        let valStr = "";
        const rng = numericRanges.get(col);
        if (typeof rawVal === "number" && rng) {
          valStr = discretizeNumeric(rawVal, rng.min, rng.max, bins);
        } else {
          valStr = rawVal !== null && rawVal !== undefined ? String(rawVal) : "<null>";
        }

        let fMap = singleCounts.get(col);
        if (!fMap) {
          fMap = new Map<string, number>();
          singleCounts.set(col, fMap);
        }
        fMap.set(valStr, (fMap.get(valStr) || 0) + 1);

        if (targetCol) {
          let jFeature = jointCounts.get(col);
          if (!jFeature) {
            jFeature = new Map<string, Map<string, number>>();
            jointCounts.set(col, jFeature);
          }
          let tMap = jFeature.get(targetValStr);
          if (!tMap) {
            tMap = new Map<string, number>();
            jFeature.set(targetValStr, tMap);
          }
          tMap.set(valStr, (tMap.get(valStr) || 0) + 1);
        }
      }
    }
  }

  if (totalSamples === 0 || !targetCols) {
    throw new Error("No data available for entropy computation.");
  }

  // Calculate Target Entropy H(Y)
  let targetEntropy = 0;
  if (targetCol) {
    const tMap = singleCounts.get(targetCol);
    if (tMap) {
      for (const count of tMap.values()) {
        const p = count / totalSamples;
        if (p > 0) {
          targetEntropy -= p * Math.log2(p);
        }
      }
    }
  }

  const features: FeatureImportanceResult[] = [];

  for (const col of targetCols) {
    const fMap = singleCounts.get(col);
    if (!fMap) continue;

    let featureEntropy = 0;
    for (const count of fMap.values()) {
      const p = count / totalSamples;
      if (p > 0) {
        featureEntropy -= p * Math.log2(p);
      }
    }

    let infoGain = 0;
    let gainRatio = 0;

    if (targetCol && targetEntropy > 0) {
      const jFeature = jointCounts.get(col);
      let condEntropy = 0;

      if (jFeature) {
        // H(Y|X) = - sum_{x,y} p(x,y) log2(p(x,y) / p(x))
        for (const [tVal, tMap] of jFeature.entries()) {
          for (const [fVal, count] of tMap.entries()) {
            const pXY = count / totalSamples;
            const pX = (fMap.get(fVal) || 0) / totalSamples;
            if (pXY > 0 && pX > 0) {
              condEntropy -= pXY * Math.log2(pXY / pX);
            }
          }
        }
      }
      infoGain = Math.max(0, targetEntropy - condEntropy);
      gainRatio = featureEntropy > 0 ? infoGain / featureEntropy : 0;
    }

    features.push({
      feature: col,
      entropy: Math.round(featureEntropy * 10000) / 10000,
      information_gain: Math.round(infoGain * 10000) / 10000,
      gain_ratio: Math.round(gainRatio * 10000) / 10000,
      importance_score: targetEntropy > 0 ? Math.round((infoGain / targetEntropy) * 1000) / 10 : 0,
      distinct_values: fMap.size,
    });
  }

  // Sort by Information Gain / Importance descending
  features.sort((a, b) => b.information_gain - a.information_gain || b.entropy - a.entropy);

  const formattedRows: Row[] = targetCol
    ? features.map((f, idx) => ({
        rank: idx + 1,
        feature: f.feature,
        info_gain: f.information_gain,
        gain_ratio: f.gain_ratio,
        entropy: f.entropy,
        importance: `${f.importance_score.toFixed(1)}%`,
        cardinality: f.distinct_values,
      }))
    : features.map((f, idx) => ({
        rank: idx + 1,
        feature: f.feature,
        entropy: f.entropy,
        cardinality: f.distinct_values,
      }));

  return {
    target_column: targetCol,
    target_entropy: targetCol ? Math.round(targetEntropy * 10000) / 10000 : undefined,
    total_samples: totalSamples,
    features,
    formattedRows,
  };
}
