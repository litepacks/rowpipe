import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { PartitionManager, type PartitionOptions } from "../../transforms/partition.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface PartitionCommandOptions extends UnifiedPipelineCliOptions, Omit<PartitionOptions, "outPattern"> {
  outPattern?: string;
  maxOpen?: string | number;
}

export async function partitionCommand(
  inputPath = "-",
  options: PartitionCommandOptions = {}
): Promise<void> {
  const pattern = options.outPattern || options.output;
  if (!pattern) {
    throw new Error(
      "Output pattern is required for partitioning (e.g. rowpipe partition data.csv --by country --out-pattern 'dist/{country}.csv')"
    );
  }

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

  const manager = new PartitionManager({
    by: options.by,
    outPattern: pattern,
    format: options.to,
    delimiter: options.delimiter,
    maxOpenWriters: options.maxOpen ? Number(options.maxOpen) : 50,
  });

  const result = await manager.partition(pipeline.batches());
  progress.done();

  if (!options.quiet) {
    process.stdout.write(
      `\nPartition complete: Partitioned ${formatNumber(result.rowCount)} rows across ${formatNumber(result.fileCount)} files.\n`
    );
  }
}
