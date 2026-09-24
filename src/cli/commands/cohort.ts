import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeCohort, type CohortOptions, type CohortInterval } from "../../analytics/cohort.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";
import type { Row } from "../../core/types.js";

export interface CohortCommandOptions extends UnifiedPipelineCliOptions, CohortOptions {
  userId?: string;
  timeCol?: string;
  interval?: CohortInterval;
  counts?: boolean;
}

export async function cohortCommand(
  inputPath = "-",
  options: CohortCommandOptions = {}
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

  const result = await computeCohort(pipeline.batches(), {
    userIdCol: options.userId || options.userIdCol,
    timeCol: options.timeCol,
    interval: options.interval || "1mo",
    percent: options.counts ? false : options.percent ?? true,
    maxPeriods: options.maxPeriods ?? 12,
  });
  progress.done();

  const rows = result.formattedRows;

  const isStructuredOutput = Boolean(options.to || options.output || options.json);
  if (isStructuredOutput) {
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

    async function* singleBatch() {
      yield { rows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    process.stdout.write(`\n--- User Retention Cohort Matrix (Interval: ${result.interval}, Cohorts: ${result.cohorts.length}) ---\n`);
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
