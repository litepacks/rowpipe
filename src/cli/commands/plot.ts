import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { renderBarChart, renderHistogram, type BarChartItem } from "../../ui/chart.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface PlotCommandOptions extends UnifiedPipelineCliOptions {
  top?: string | number;
  bins?: string | number;
  width?: string | number;
  numeric?: boolean;
  categorical?: boolean;
  asc?: boolean;
  noNulls?: boolean;
}

export async function plotCommand(
  inputPath = "-",
  col1?: string,
  col2?: string,
  options: PlotCommandOptions = {}
): Promise<void> {
  let labelCol = col1;
  let valCol = col2;

  if (!labelCol) {
    throw new Error("Column name to plot is required (e.g. rowpipe plot netflix.csv country)");
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);
  const topLimit = options.top !== undefined ? Number(options.top) : 20;
  const maxWidth = options.width !== undefined ? Number(options.width) : 30;
  const binCount = options.bins !== undefined ? Number(options.bins) : 10;

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  // Mode 1: Two columns provided (label + value directly)
  if (valCol) {
    const directItems: BarChartItem[] = [];
    let totalVal = 0;

    for await (const batch of pipeline.batches()) {
      for (const row of batch.rows) {
        const lbl = String(row[labelCol] ?? "<null>");
        const rawV = row[valCol];
        const numV = typeof rawV === "number" ? rawV : parseFloat(String(rawV ?? 0));
        const safeV = Number.isFinite(numV) ? numV : 0;
        directItems.push({ label: lbl, value: safeV });
        totalVal += safeV;
      }
    }
    progress.done();

    const sorted = directItems.sort((a, b) => (options.asc ? a.value - b.value : b.value - a.value));
    const limited = topLimit > 0 ? sorted.slice(0, topLimit) : sorted;

    const itemsWithPct: BarChartItem[] = limited.map((item) => ({
      ...item,
      percent: totalVal > 0 ? `${((item.value / totalVal) * 100).toFixed(1)}%` : undefined,
    }));

    process.stdout.write(`\n--- Chart: ${labelCol} vs ${valCol} ---\n`);
    process.stdout.write(renderBarChart(itemsWithPct, { maxWidth }) + "\n");
    if (sorted.length > limited.length) {
      process.stdout.write(`... and ${formatNumber(sorted.length - limited.length)} more items.\n`);
    }
    process.stdout.write("\n");
    return;
  }

  // Mode 2: Single column provided - determine whether categorical frequency or numeric histogram
  let numericSampleCount = 0;
  let nonNumericSampleCount = 0;
  const numericValues: number[] = [];
  const freqCounts = new Map<string, number>();
  let totalRows = 0;

  for await (const batch of pipeline.batches()) {
    for (const row of batch.rows) {
      totalRows++;
      const rawVal = row[labelCol];

      if (rawVal === null || rawVal === undefined || rawVal === "") {
        if (options.noNulls) continue;
      }

      if (typeof rawVal === "number" && Number.isFinite(rawVal)) {
        numericSampleCount++;
        numericValues.push(rawVal);
      } else if (rawVal !== null && rawVal !== undefined && rawVal !== "") {
        const parsed = Number(rawVal);
        if (!Number.isNaN(parsed) && Number.isFinite(parsed) && String(rawVal).trim() !== "") {
          numericSampleCount++;
          numericValues.push(parsed);
        } else {
          nonNumericSampleCount++;
        }
      }

      const key = rawVal === null || rawVal === undefined ? "<null>" : String(rawVal);
      freqCounts.set(key, (freqCounts.get(key) || 0) + 1);
    }
  }
  progress.done();

  const isNumeric =
    options.numeric ||
    (!options.categorical &&
      numericSampleCount > 0 &&
      numericSampleCount / (numericSampleCount + nonNumericSampleCount) > 0.8 &&
      freqCounts.size > 15);

  if (isNumeric && numericValues.length > 0) {
    process.stdout.write(`\n--- Histogram: ${labelCol} (${formatNumber(numericValues.length)} numeric rows, ${binCount} bins) ---\n`);
    process.stdout.write(renderHistogram(numericValues, { bins: binCount, width: maxWidth }) + "\n\n");
  } else {
    // Categorical frequency bar chart
    const sorted = Array.from(freqCounts.entries()).sort((a, b) =>
      options.asc ? a[1] - b[1] : b[1] - a[1]
    );
    const limited = topLimit > 0 ? sorted.slice(0, topLimit) : sorted;

    const items: BarChartItem[] = limited.map(([lbl, count]) => ({
      label: lbl,
      value: count,
      percent: totalRows > 0 ? `${((count / totalRows) * 100).toFixed(1)}%` : undefined,
    }));

    process.stdout.write(`\n--- Frequency Chart: ${labelCol} (Total: ${formatNumber(totalRows)} rows) ---\n`);
    process.stdout.write(renderBarChart(items, { maxWidth }) + "\n");
    if (sorted.length > limited.length) {
      process.stdout.write(`... and ${formatNumber(sorted.length - limited.length)} more categories.\n`);
    }
    process.stdout.write("\n");
  }
}
