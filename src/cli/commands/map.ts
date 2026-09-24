import { InvalidArgumentError } from "../../core/errors.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter } from "../../writers/index.js";
import { mapRows, parseMapSpecs } from "../../transforms/map.js";
import { openReadableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface MapCommandOptions {
  from?: string;
  to?: string;
  sheet?: string;
  delimiter?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function mapCommand(
  inputPath = "-",
  specs: string[],
  options: MapCommandOptions = {}
): Promise<void> {
  if (!specs || specs.length === 0) {
    throw new InvalidArgumentError(
      "At least one map specification is required (e.g. rowpipe map users.csv 'full_name=concat(first, \" \", last)')"
    );
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);
  const parsedSpecs = parseMapSpecs(specs);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const toFormat = options.to?.toLowerCase() || (fromFormat === "xlsx" ? "csv" : fromFormat);

  const inputStream = openReadableStream(inputPath);
  const reader = createReader(inputStream, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    batchSize: effectiveBatchSize,
    filePath: inputPath,
  });

  const writer = createWriter(process.stdout, {
    format: toFormat,
    delimiter: options.delimiter,
  });

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.pipe(mapRows(parsedSpecs));
  pipeline.onProgress((info) => progress.update(info));

  await pipeline.to(writer);
  progress.done();
  logMemoryDebug();
}
