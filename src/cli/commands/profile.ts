import { DatasetProfiler, formatProfileMarkdown, formatProfileTerminal } from "../../analytics/profiler.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { safeJsonStringify } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface ProfileCommandOptions {
  sample?: string | number;
  json?: boolean;
  markdown?: boolean;
  from?: string;
  sheet?: string;
  delimiter?: string;
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compress?: string;
  compression?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

/**
 * Handles `rowpipe profile [input] [options]`
 */
export async function profileCommand(
  inputPath = "-",
  options: ProfileCommandOptions = {}
): Promise<void> {
  const progress = new ProgressReporter(options);
  const sampleLimit = options.sample ? Number(options.sample) : undefined;
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    maxRows: sampleLimit,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const profiler = new DatasetProfiler();
  const pipeline = createPipeline(reader, { maxRows: sampleLimit });
  pipeline.onProgress((info) => progress.update(info));

  try {
    const profileResult = await pipeline.reduce(profiler);
    progress.done();

    if (options.json) {
      process.stdout.write(safeJsonStringify(profileResult, 2) + "\n");
      return;
    }

    if (options.markdown) {
      process.stdout.write(formatProfileMarkdown(profileResult) + "\n");
      return;
    }

    process.stdout.write(formatProfileTerminal(profileResult));
  } finally {
    if (reader.close) {
      await reader.close();
    }
  }
}
