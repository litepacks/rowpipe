import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { flattenRows } from "../../transforms/flatten.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface FlattenCommandOptions extends UnifiedPipelineCliOptions {
  separator?: string;
  maxDepth?: string | number;
  arrays?: boolean;
}

export async function flattenCommand(
  inputPath = "-",
  options: FlattenCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "jsonl";

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) toFormat = "jsonl";

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
      flattenRows({
        separator: options.separator ?? ".",
        maxDepth: options.maxDepth ? Number(options.maxDepth) : 10,
        arrays: options.arrays === true,
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
