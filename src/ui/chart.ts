import { formatNumber } from "../utils/formatting.js";

export interface BarChartItem {
  label: string;
  value: number;
  percent?: string;
}

export interface BarChartOptions {
  maxWidth?: number;
  showPercent?: boolean;
  showValue?: boolean;
  barChar?: string;
}

const FRACTIONAL_BLOCKS = [" ", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"];

/**
 * Renders a horizontal bar chart with sub-character precision using Unicode block elements.
 */
export function renderBarChart(
  items: BarChartItem[],
  options: BarChartOptions = {}
): string {
  if (items.length === 0) return "";

  const maxWidth = options.maxWidth ?? 30;
  const maxVal = Math.max(...items.map((i) => (Number.isFinite(i.value) ? i.value : 0)), 0.0001);
  const maxLabelLen = Math.max(5, ...items.map((i) => i.label.length));
  const maxValLen = Math.max(5, ...items.map((i) => formatNumber(i.value).length));

  const lines: string[] = [];

  for (const item of items) {
    const label = item.label.padEnd(maxLabelLen);
    const ratio = Math.max(0, Math.min(1, item.value / maxVal));
    const totalEighths = Math.round(ratio * maxWidth * 8);
    const fullBlocks = Math.floor(totalEighths / 8);
    const remainder = totalEighths % 8;

    let bar = "█".repeat(fullBlocks);
    if (remainder > 0 && fullBlocks < maxWidth) {
      bar += FRACTIONAL_BLOCKS[remainder] || "";
    }
    if (bar.length === 0 && item.value > 0) {
      bar = "▏";
    }

    const paddedBar = bar.padEnd(maxWidth, " ");
    const valStr = formatNumber(item.value).padStart(maxValLen);
    const pctStr = item.percent ? ` (${item.percent})` : "";

    lines.push(`${label}  ${paddedBar}  ${valStr}${pctStr}`);
  }

  return lines.join("\n");
}

export interface HistogramOptions {
  bins?: number;
  width?: number;
  min?: number;
  max?: number;
}

/**
 * Computes frequency bins for numeric data and renders a horizontal ASCII/Unicode histogram.
 */
export function renderHistogram(
  values: number[],
  options: HistogramOptions = {}
): string {
  const valid = values.filter((v) => typeof v === "number" && !Number.isNaN(v) && Number.isFinite(v));
  if (valid.length === 0) return "No numeric data to display.";

  const binCount = Math.max(2, Math.min(50, options.bins || 10));
  const min = options.min !== undefined ? options.min : Math.min(...valid);
  const max = options.max !== undefined ? options.max : Math.max(...valid);

  if (min === max) {
    return `${min.toFixed(2)}  ${"█".repeat(options.width || 30)}  ${valid.length} (100.0%)`;
  }

  const binWidth = (max - min) / binCount;
  const bins = new Array(binCount).fill(0);

  for (const val of valid) {
    let idx = Math.floor((val - min) / binWidth);
    if (idx >= binCount) idx = binCount - 1;
    if (idx < 0) idx = 0;
    bins[idx]++;
  }

  const items: BarChartItem[] = [];
  for (let i = 0; i < binCount; i++) {
    const start = min + i * binWidth;
    const end = start + binWidth;
    const count = bins[i]!;
    const pct = ((count / valid.length) * 100).toFixed(1) + "%";

    const label = `${start.toFixed(1)} - ${end.toFixed(1)}`;
    items.push({
      label,
      value: count,
      percent: pct,
    });
  }

  return renderBarChart(items, { maxWidth: options.width || 30 });
}

const SPARK_CHARS = [" ", "▂", "▃", "▄", "▅", "▆", "▇", "█"];

/**
 * Generates an inline sparkline string for a sequence of numbers (e.g.  ▃▅█▇▄▂).
 */
export function renderSparkline(values: number[]): string {
  const valid = values.filter((v) => typeof v === "number" && !Number.isNaN(v));
  if (valid.length === 0) return "";

  const min = Math.min(...valid);
  const max = Math.max(...valid);
  if (min === max) return "▄".repeat(valid.length);

  return valid
    .map((v) => {
      const idx = Math.floor(((v - min) / (max - min)) * (SPARK_CHARS.length - 1));
      return SPARK_CHARS[Math.max(0, Math.min(SPARK_CHARS.length - 1, idx))] || " ";
    })
    .join("");
}
