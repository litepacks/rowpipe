import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { technicalTransform, type TechnicalOptions } from "../../analytics/technical.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface TechnicalCommandOptions extends UnifiedPipelineCliOptions, TechnicalOptions {
  price?: string;
  volume?: string;
}

export async function technicalCommand(
  inputPath = "-",
  options: TechnicalCommandOptions = {}
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

  const transformedBatches = technicalTransform(pipeline.batches(), {
    priceCol: options.price || options.priceCol,
    volumeCol: options.volume || options.volumeCol,
    highCol: options.highCol,
    lowCol: options.lowCol,
    closeCol: options.closeCol,
    indicators: options.indicators,
  });

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
