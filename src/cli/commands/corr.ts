import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeCorrelationMatrix, formatCorrelationRows } from "../../analytics/correlation.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface CorrCommandOptions extends UnifiedPipelineCliOptions {
  cols?: string | string[];
  columns?: string | string[];
}

export async function corrCommand(
  inputPath = "-",
  options: CorrCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

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

  const rawCols = options.cols || options.columns;
  const targetCols = rawCols
    ? Array.isArray(rawCols)
      ? rawCols.flatMap((c) => c.split(",").map((s) => s.trim()))
      : rawCols.split(",").map((s) => s.trim())
    : undefined;

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  const result = await computeCorrelationMatrix(pipeline.batches(), targetCols);
  progress.done();

  if (result.columns.length === 0) {
    throw new Error("No numeric columns found for correlation analysis.");
  }

  const rows = formatCorrelationRows(result);

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

    async function* singleBatch() {
      yield { rows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    process.stdout.write(`\n--- Pearson Correlation Matrix (Sample size: ${formatNumber(result.sampleSize)} rows) ---\n`);
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
