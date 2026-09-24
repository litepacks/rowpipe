import { InvalidArgumentError } from "../../core/errors.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter } from "../../writers/index.js";
import { parseSortSpecs, sortRows } from "../../transforms/sort/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface SortCommandOptions {
  by?: string | string[];
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
  memoryLimit?: string;
  tempDir?: string;
  from?: string;
  to?: string;
  sheet?: string;
  delimiter?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function sortCommand(
  inputPath = "-",
  options: SortCommandOptions = {}
): Promise<void> {
  if (!options.by) {
    throw new InvalidArgumentError(
      "The --by option is required for sort (e.g. rowpipe sort users.csv --by age:desc)"
    );
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  const specs = parseSortSpecs(options.by, {
    nulls: options.nulls,
    ignoreCase: options.ignoreCase,
    natural: options.natural,
  });

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
  pipeline.pipe(
    sortRows({
      by: specs,
      nulls: options.nulls,
      ignoreCase: options.ignoreCase,
      natural: options.natural,
      memoryLimit: options.memoryLimit,
      tempDir: options.tempDir,
      batchSize: effectiveBatchSize,
    })
  );
  pipeline.onProgress((info) => progress.update(info));

  await pipeline.to(writer);
  progress.done();
  logMemoryDebug();
}
