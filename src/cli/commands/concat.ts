import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { concatReaders, type ConcatSource } from "../../transforms/concat.js";
import { InvalidArgumentError } from "../../core/errors.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ConcatCommandOptions extends UnifiedPipelineCliOptions {
  sourceCol?: string;
  sourceColumn?: string;
  source?: string;
  align?: "union" | "intersect";
}

export async function concatCommand(
  files: string[],
  options: ConcatCommandOptions = {}
): Promise<void> {
  if (!files || files.length === 0) {
    throw new InvalidArgumentError("Usage: rowpipe concat <file1> <file2> [file3...] [options]");
  }

  const progress = new ProgressReporter(options);
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const sourceCol = options.sourceCol || options.sourceColumn || options.source;

  const sources: ConcatSource[] = [];

  for (const file of files) {
    let fromFormat = options.from?.toLowerCase();
    if (!fromFormat && file !== "-") {
      fromFormat = inferReaderFormat(file) ?? undefined;
    }
    if (!fromFormat) fromFormat = "csv";

    const readerInput = file === "-" ? openReadableStream("-", options) : file;
    const reader = createReader(readerInput, {
      format: fromFormat,
      sheet: options.sheet,
      delimiter: options.delimiter,
      filePath: file,
      ...options,
      batchSize: effectiveBatchSize,
    });

    sources.push({
      reader,
      label: file,
    });
  }

  const concatReader = concatReaders(sources, {
    sourceCol,
    align: options.align,
    batchSize: effectiveBatchSize,
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

  await writer.write(concatReader.read());
  if (writer.close) await writer.close();
  if (concatReader.close) await concatReader.close();
  progress.done();
}
