import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { computeAbTest, type AbTestOptions } from "../../analytics/abtest.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface AbTestCommandOptions extends UnifiedPipelineCliOptions, AbTestOptions {
  group?: string;
  metric?: string;
  control?: string;
  variant?: string;
}

export async function abTestCommand(
  inputPath = "-",
  options: AbTestCommandOptions = {}
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

  const result = await computeAbTest(pipeline.batches(), {
    groupCol: options.group || options.groupCol,
    metricCol: options.metric || options.metricCol,
    controlGroup: options.control || options.controlGroup,
    variantGroup: options.variant || options.variantGroup,
    type: options.type,
    confidenceLevel: options.confidenceLevel ? Number(options.confidenceLevel) : 0.95,
  });
  progress.done();

  const rows = result.formattedRows;

  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

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
    process.stdout.write(`\n--- A/B Test Statistical Significance Report (${result.test_type}) ---\n`);
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
