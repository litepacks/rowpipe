import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { splitStream, type SplitOptions } from "../../transforms/split.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface SplitCommandOptions extends UnifiedPipelineCliOptions, Omit<SplitOptions, "outPattern"> {
  chunkSize?: number;
  outPattern?: string;
}

export async function splitCommand(
  inputPath = "-",
  options: SplitCommandOptions = {}
): Promise<void> {
  const pattern = options.outPattern || options.output;
  if (!pattern) {
    throw new Error(
      "Output pattern is required for splitting (e.g. rowpipe split data.csv --chunk-size 10000 --out-pattern 'parts/chunk_{n:03d}.csv')"
    );
  }

  const chunkSize = options.chunkSize !== undefined ? Number(options.chunkSize) : 100000;
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

  const result = await splitStream(pipeline.batches(), {
    chunkSize,
    outPattern: pattern,
    format: options.to,
    delimiter: options.delimiter,
  });

  progress.done();

  if (!options.quiet) {
    process.stdout.write(
      `\nSplit complete: Split ${formatNumber(result.rowCount)} rows into ${formatNumber(result.fileCount)} chunk files (chunk size: ${formatNumber(chunkSize)} rows).\n`
    );
  }
}
