import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { explodeRows } from "../../transforms/explode.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ExplodeCommandOptions extends UnifiedPipelineCliOptions {
  delimiter?: string;
  noTrim?: boolean;
  preserveEmpty?: boolean;
}

export async function explodeCommand(
  inputPath = "-",
  column: string,
  options: ExplodeCommandOptions = {}
): Promise<void> {
  if (!column || column.trim().length === 0) {
    throw new Error("Column name to explode is required (e.g. rowpipe explode data.csv country)");
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) toFormat = fromFormat === "xlsx" ? "csv" : fromFormat;

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const writer = createWriter(options.output || "-", {
    format: toFormat,
    delimiter: options.delimiter,
    ...options,
  });

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize })
    .pipe(
      explodeRows({
        column,
        delimiter: options.delimiter ?? ",",
        trim: options.noTrim !== true,
        dropEmpty: options.preserveEmpty !== true,
      })
    );

  pipeline.onProgress((info) => progress.update(info));

  try {
    await pipeline.to(writer);
  } finally {
    if (reader.close) await reader.close();
    if (writer.close) await writer.close();
  }

  progress.done();
}
