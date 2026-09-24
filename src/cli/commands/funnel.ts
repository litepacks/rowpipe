import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeFunnel, type FunnelOptions } from "../../analytics/funnel.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface FunnelCommandOptions extends UnifiedPipelineCliOptions, FunnelOptions {
  steps?: string | string[];
  userId?: string;
  step?: string;
  timeCol?: string;
  strict?: boolean;
}

export async function funnelCommand(
  inputPath = "-",
  options: FunnelCommandOptions = {}
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

  const result = await computeFunnel(pipeline.batches(), {
    steps: options.steps,
    userIdCol: options.userId || options.userIdCol,
    stepCol: options.step || options.stepCol,
    timeCol: options.timeCol,
    strict: options.strict,
    windowMs: options.windowMs,
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
    process.stdout.write(`\n--- Conversion Funnel Analysis (Entry Users: ${formatNumber(result.total_entry_users)}, Overall Conversion: ${result.overall_conversion_rate.toFixed(1)}%) ---\n`);
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
