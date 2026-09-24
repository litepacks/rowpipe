import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeEntropy, type EntropyOptions } from "../../analytics/entropy.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface EntropyCommandOptions extends UnifiedPipelineCliOptions, EntropyOptions {
  target?: string;
  cols?: string | string[];
  bins?: number;
}

export async function entropyCommand(
  inputPath = "-",
  options: EntropyCommandOptions = {}
): Promise<void> {
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

  const result = await computeEntropy(pipeline.batches(), {
    targetCol: options.target || options.targetCol,
    cols: options.cols,
    bins: options.bins ? Number(options.bins) : 10,
  });
  progress.done();

  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  const rows = result.formattedRows;

  const isStructuredOutput = Boolean(options.to || options.output);
  if (isStructuredOutput) {
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

    async function* singleBatch() {
      yield { rows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    if (result.target_column) {
      process.stdout.write(`\n--- Feature Importance & Information Gain (Target: '${result.target_column}', Target Entropy: ${result.target_entropy} bits, Samples: ${formatNumber(result.total_samples)}) ---\n`);
    } else {
      process.stdout.write(`\n--- Column Shannon Entropy Analysis (Samples: ${formatNumber(result.total_samples)}) ---\n`);
    }
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
