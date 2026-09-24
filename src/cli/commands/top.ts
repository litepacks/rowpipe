import { InvalidArgumentError } from "../../core/errors.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter } from "../../writers/index.js";
import { topRows } from "../../transforms/top.js";
import { openReadableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface TopCommandOptions {
  by?: string | string[];
  lines?: string | number;
  n?: string | number;
  order?: "asc" | "desc";
  smallest?: boolean;
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
  from?: string;
  to?: string;
  sheet?: string;
  delimiter?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function topCommand(
  inputPath = "-",
  options: TopCommandOptions = {}
): Promise<void> {
  if (!options.by) {
    throw new InvalidArgumentError(
      "The --by option is required for top (e.g. rowpipe top sales.csv --by revenue -n 10)"
    );
  }

  const count = Number(options.n ?? options.lines ?? 10) || 10;
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

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
    topRows({
      by: options.by,
      count,
      order: options.order,
      smallest: options.smallest,
      nulls: options.nulls,
      ignoreCase: options.ignoreCase,
      natural: options.natural,
    })
  );
  pipeline.onProgress((info) => progress.update(info));

  await pipeline.to(writer);
  progress.done();
  logMemoryDebug();
}
