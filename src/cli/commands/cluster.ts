import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeKMeans, clusterAssignTransform, type ClusterOptions } from "../../analytics/cluster.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ClusterCommandOptions extends UnifiedPipelineCliOptions, ClusterOptions {
  cols?: string | string[];
  k?: number;
  summary?: boolean;
}

export async function clusterCommand(
  inputPath = "-",
  options: ClusterCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  // Pass 1: Compute Centroids
  const readerInput1 = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader1 = createReader(readerInput1, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const pipeline1 = createPipeline(reader1, { batchSize: effectiveBatchSize });
  pipeline1.onProgress((info) => progress.update(info));

  const result = await computeKMeans(pipeline1.batches(), {
    cols: options.cols,
    k: options.k ? Number(options.k) : 3,
    summary: options.summary,
  });
  progress.done();

  if (options.summary || !options.output && !options.to && !options.json) {
    if (options.json) {
      console.log(JSON.stringify(result, null, 2));
      return;
    }
    process.stdout.write(`\n--- Mini-Batch K-Means Centroids (K=${result.k}, Inertia: ${formatNumber(result.inertia)}, Samples: ${formatNumber(result.total_samples)}) ---\n`);
    process.stdout.write(formatTable(result.formattedRows, { style: "unicode" }));
    process.stdout.write("\n");
    return;
  }

  // Pass 2 or piped streaming: assign clusters to rows
  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  const readerInput2 = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader2 = createReader(readerInput2, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const pipeline2 = createPipeline(reader2, { batchSize: effectiveBatchSize });
  const assignedStream = clusterAssignTransform(pipeline2.batches(), result);

  let toFormat = options.to?.toLowerCase();
  if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) toFormat = "csv";

  const writer = createWriter(options.output || "-", {
    format: toFormat,
    delimiter: options.delimiter,
    ...options,
  });

  await writer.write(assignedStream);
  if (writer.close) await writer.close();
}
