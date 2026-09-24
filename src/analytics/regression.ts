import type { DataStream } from "../core/types.js";

export interface LinearRegressionResult {
  xColumn: string;
  yColumn: string;
  sampleSize: number;
  slope: number;
  intercept: number;
  r: number;
  r2: number;
  stdError: number;
  formula: string;
  interpretation: string;
}

/**
 * Computes Ordinary Least Squares (OLS) Linear Regression in a single pass.
 */
export async function computeLinearRegression(
  stream: DataStream,
  xColumn: string,
  yColumn: string
): Promise<LinearRegressionResult> {
  let n = 0;
  let meanX = 0;
  let meanY = 0;
  let m2X = 0;
  let m2Y = 0;
  let coMoment = 0;

  for await (const batch of stream) {
    for (const row of batch.rows) {
      const rawX = row[xColumn];
      const rawY = row[yColumn];
      if (rawX === null || rawX === undefined || rawX === "") continue;
      if (rawY === null || rawY === undefined || rawY === "") continue;

      const x = typeof rawX === "number" ? rawX : parseFloat(String(rawX));
      const y = typeof rawY === "number" ? rawY : parseFloat(String(rawY));

      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

      n++;
      const dx = x - meanX;
      meanX += dx / n;

      const dy = y - meanY;
      meanY += dy / n;

      m2X += dx * (x - meanX);
      m2Y += dy * (y - meanY);
      coMoment += dx * (y - meanY);
    }
  }

  if (n < 2) {
    throw new Error(`Insufficient numeric data points for regression (found ${n} pairs).`);
  }

  const varX = m2X / (n - 1);
  const varY = m2Y / (n - 1);
  const cov = coMoment / (n - 1);

  if (varX === 0) {
    throw new Error(`Independent variable (${xColumn}) has zero variance.`);
  }

  const slope = cov / varX;
  const intercept = meanY - slope * meanX;

  const stdX = Math.sqrt(varX);
  const stdY = Math.sqrt(varY);
  const r = stdY > 0 ? cov / (stdX * stdY) : 0;
  const r2 = r * r;

  // Residual sum of squares & std error
  const ssTot = m2Y;
  const ssReg = slope * slope * m2X;
  const ssRes = Math.max(0, ssTot - ssReg);
  const stdError = n > 2 ? Math.sqrt(ssRes / (n - 2)) : 0;

  const sign = intercept >= 0 ? "+" : "-";
  const absIntercept = Math.abs(intercept);
  const formula = `${yColumn} = ${slope.toFixed(4)} * ${xColumn} ${sign} ${absIntercept.toFixed(4)}`;

  let interpretation = "No linear relationship";
  const absR = Math.abs(r);
  if (absR >= 0.8) interpretation = `Very strong ${r > 0 ? "positive" : "negative"} linear fit`;
  else if (absR >= 0.6) interpretation = `Strong ${r > 0 ? "positive" : "negative"} linear fit`;
  else if (absR >= 0.4) interpretation = `Moderate ${r > 0 ? "positive" : "negative"} linear fit`;
  else if (absR >= 0.2) interpretation = `Weak ${r > 0 ? "positive" : "negative"} linear fit`;

  return {
    xColumn,
    yColumn,
    sampleSize: n,
    slope: parseFloat(slope.toFixed(6)),
    intercept: parseFloat(intercept.toFixed(6)),
    r: parseFloat(r.toFixed(4)),
    r2: parseFloat(r2.toFixed(4)),
    stdError: parseFloat(stdError.toFixed(4)),
    formula,
    interpretation,
  };
}
