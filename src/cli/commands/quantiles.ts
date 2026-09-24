import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeQuantiles } from "../../analytics/quantiles.js";
import { formatTable } from "../../writers/table.js";
import type { Row } from "../../core/types.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface QuantilesCommandOptions extends UnifiedPipelineCliOptions {
  p?: string | number | (string | number)[];
  percentiles?: string | number | (string | number)[];
}

export async function quantilesCommand(
  inputPath = "-",
  column?: string,
  options: QuantilesCommandOptions = {}
): Promise<void> {
  if (!column) {
    throw new Error("Column name is required (e.g. rowpipe quantiles latencies.csv duration_ms --p 50,90,95,99)");
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  const rawP = options.p || options.percentiles || "25,50,75,90,95,99";
  const pList = (Array.isArray(rawP) ? rawP : String(rawP).split(","))
    .map((s) => parseFloat(String(s).trim()))
    .filter((n) => !Number.isNaN(n) && n >= 0 && n <= 100);

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

  const result = await computeQuantiles(pipeline.batches(), column, pList);
  progress.done();

  const isStructuredOutput = Boolean(options.to || options.output || options.json);
  if (isStructuredOutput) {
    const outRows: Row[] = [
      { metric: "count", value: result.count },
      { metric: "min", value: result.min },
      { metric: "mean", value: result.mean },
      { metric: "median (P50)", value: result.median },
      { metric: "Q1 (P25)", value: result.q1 },
      { metric: "Q3 (P75)", value: result.q3 },
      { metric: "IQR", value: result.iqr },
      { metric: "max", value: result.max },
      ...Object.entries(result.percentiles).map(([p, val]) => ({
        metric: p.toUpperCase(),
        value: val,
      })),
    ];

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

    async function* singleBatch() {
      yield { rows: outRows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    process.stdout.write(`\n--- Quantile Distribution: ${column} (Count: ${formatNumber(result.count)}) ---\n`);

    const tableRows: Row[] = [
      { Metric: "Min", Value: result.min },
      { Metric: "Q1 (P25)", Value: result.q1 },
      { Metric: "Median (P50)", Value: result.median },
      { Metric: "Mean", Value: result.mean },
      { Metric: "Q3 (P75)", Value: result.q3 },
      { Metric: "IQR", Value: result.iqr },
      { Metric: "Max", Value: result.max },
      ...Object.entries(result.percentiles).map(([p, val]) => ({
        Metric: p.toUpperCase(),
        Value: val,
      })),
    ];

    process.stdout.write(formatTable(tableRows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
