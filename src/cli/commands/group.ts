import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter } from "../../writers/index.js";
import { groupRows, GroupOptions } from "../../transforms/group.js";
import { openReadableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface GroupCommandOptions extends Omit<GroupOptions, "batchSize"> {
  from?: string;
  to?: string;
  sheet?: string;
  delimiter?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function groupCommand(
  inputPath = "-",
  options: GroupCommandOptions = {}
): Promise<void> {
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
    groupRows({
      by: options.by,
      count: options.count,
      sum: options.sum,
      avg: options.avg,
      min: options.min,
      max: options.max,
      first: options.first,
      last: options.last,
      agg: options.agg,
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
