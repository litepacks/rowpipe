import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { pivotTransform, type PivotAggregator } from "../../transforms/pivot.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface PivotCommandOptions extends UnifiedPipelineCliOptions {
  index?: string;
  columns?: string;
  values?: string;
  agg?: PivotAggregator;
  fill?: string | number;
}

export async function pivotCommand(
  inputPath = "-",
  options: PivotCommandOptions = {}
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

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  const indexCols = options.index || (options as any).i || (options as any).rows;
  const columnsCol = options.columns || (options as any).c || (options as any).col;
  const valuesCol = options.values || (options as any).v || (options as any).val;

  const transform = pivotTransform({
    index: indexCols,
    columns: columnsCol,
    values: valuesCol,
    agg: options.agg,
    fill: options.fill,
    batchSize: effectiveBatchSize,
  });

  const transformedBatches = transform(pipeline.batches());

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

  await writer.write(transformedBatches);
  if (writer.close) await writer.close();
  progress.done();
}
