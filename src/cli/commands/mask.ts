import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { maskTransform, type MaskOptions } from "../../dataops/mask.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface MaskCommandOptions extends Omit<UnifiedPipelineCliOptions, "batchSize">, MaskOptions {}

export async function maskCommand(
  inputPath = "-",
  options: MaskCommandOptions = {}
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

  const transform = maskTransform({
    email: options.email,
    phone: options.phone,
    card: options.card,
    ip: options.ip,
    name: options.name,
    hash: options.hash,
    redact: options.redact,
    salt: options.salt,
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
