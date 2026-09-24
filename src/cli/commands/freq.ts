import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { renderBarChart } from "../../ui/chart.js";
import type { Row } from "../../core/types.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface FreqCommandOptions extends UnifiedPipelineCliOptions {
  top?: string | number;
  asc?: boolean;
  noNulls?: boolean;
  chart?: boolean;
  width?: string | number;
}

export async function freqCommand(
  inputPath = "-",
  column: string,
  options: FreqCommandOptions = {}
): Promise<void> {
  if (!column || column.trim().length === 0) {
    throw new Error("Column name for frequency analysis is required (e.g. rowpipe freq netflix.csv country)");
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);
  const topLimit = options.top !== undefined ? Number(options.top) : 20;

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

  const counts = new Map<string, number>();
  let totalRows = 0;
  let nullCount = 0;

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  for await (const batch of pipeline.batches()) {
    for (const row of batch.rows) {
      totalRows++;
      const val = row[column];

      if (val === null || val === undefined || val === "") {
        nullCount++;
        if (options.noNulls) continue;
      }

      const key = val === null || val === undefined ? "<null>" : String(val);
      counts.set(key, (counts.get(key) || 0) + 1);
    }
  }

  progress.done();

  // Sort by count
  const sorted = Array.from(counts.entries()).sort((a, b) => {
    return options.asc ? a[1] - b[1] : b[1] - a[1];
  });

  const limited = topLimit > 0 ? sorted.slice(0, topLimit) : sorted;

  const resultRows: Row[] = limited.map(([val, count]) => {
    const pct = totalRows > 0 ? ((count / totalRows) * 100).toFixed(2) : "0.00";
    return {
      [column]: val,
      count,
      percent: `${pct}%`,
    };
  });

  const isStructuredOutput = Boolean(options.to || options.output || options.json);

  if (isStructuredOutput) {
    let toFormat = options.to?.toLowerCase();
    if (options.json) {
      toFormat = "json";
    } else if (!toFormat && options.output && options.output !== "-") {
      toFormat = inferWriterFormat(options.output) ?? undefined;
    }
    if (!toFormat) toFormat = "csv";

    const writer = createWriter(options.output || "-", {
      format: toFormat,
      delimiter: options.delimiter,
      ...options,
    });

    async function* outputBatches() {
      yield { rows: resultRows, offset: 0 };
    }

    await writer.write(outputBatches());
    if (writer.close) await writer.close();
  } else {
    if (options.chart) {
      const maxWidth = options.width !== undefined ? Number(options.width) : 30;
      const chartItems = resultRows.map((r) => ({
        label: String(r[column]),
        value: Number(r["count"]),
        percent: String(r["percent"]),
      }));

      process.stdout.write(`\n--- Frequency Chart: ${column} (Total: ${formatNumber(totalRows)} rows) ---\n`);
      process.stdout.write(renderBarChart(chartItems, { maxWidth }) + "\n");
      if (sorted.length > limited.length) {
        process.stdout.write(`... and ${formatNumber(sorted.length - limited.length)} more distinct values.\n`);
      }
      process.stdout.write("\n");
    } else {
      // Pretty terminal print
      const colName = column;
      const maxValLen = Math.max(colName.length, ...resultRows.map((r) => String(r[column]).length));
      const maxCountLen = Math.max(5, ...resultRows.map((r) => formatNumber(Number(r["count"])).length));

      process.stdout.write(`\n--- Frequency Distribution: ${column} (Total: ${formatNumber(totalRows)} rows) ---\n`);
      const header = `${colName.padEnd(maxValLen)}  ${"COUNT".padStart(maxCountLen)}  ${"PERCENT".padStart(8)}\n`;
      process.stdout.write(header);
      process.stdout.write(`${"-".repeat(maxValLen)}  ${"-".repeat(maxCountLen)}  ${"-".repeat(8)}\n`);

      for (const r of resultRows) {
        const v = String(r[column]).padEnd(maxValLen);
        const c = formatNumber(Number(r["count"])).padStart(maxCountLen);
        const p = String(r["percent"]).padStart(8);
        process.stdout.write(`${v}  ${c}  ${p}\n`);
      }

      if (sorted.length > limited.length) {
        process.stdout.write(`... and ${formatNumber(sorted.length - limited.length)} more distinct values.\n`);
      }
      process.stdout.write("\n");
    }
  }
}
