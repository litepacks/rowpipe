import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { outliersTransform, type OutlierMethod } from "../../analytics/outliers.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface OutliersCommandOptions extends UnifiedPipelineCliOptions {
  col?: string;
  column?: string;
  method?: OutlierMethod;
  threshold?: string | number;
  onlyOutliers?: boolean;
  addColumns?: boolean;
  invert?: boolean;
}

export async function outliersCommand(
  inputPath = "-",
  options: OutliersCommandOptions = {}
): Promise<void> {
  const column = options.col || options.column;
  if (!column) {
    throw new Error(
      "Column name is required for outlier detection (e.g. rowpipe outliers data.csv --col amount --method zscore --threshold 3.0)"
    );
  }

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

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  const transformed = pipeline.pipe(
    outliersTransform({
      column,
      method: options.method || "zscore",
      threshold: options.threshold !== undefined ? Number(options.threshold) : undefined,
      onlyOutliers: options.onlyOutliers,
      addColumns: options.addColumns,
      invert: options.invert,
    })
  );

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

  await writer.write(transformed.batches());
  if (writer.close) await writer.close();
  progress.done();
}
