import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeRfm, type RfmOptions } from "../../analytics/rfm.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";
import type { Row } from "../../core/types.js";

export interface RfmCommandOptions extends UnifiedPipelineCliOptions, RfmOptions {
  summary?: boolean;
  customerId?: string;
  dateCol?: string;
  amountCol?: string;
  asOf?: string;
}

export async function rfmCommand(
  inputPath = "-",
  options: RfmCommandOptions = {}
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

  const result = await computeRfm(pipeline.batches(), {
    customerIdCol: options.customerId || options.customerIdCol,
    dateCol: options.dateCol,
    amountCol: options.amountCol,
    asOfDate: options.asOf || options.asOfDate,
  });
  progress.done();

  const outputRows: Row[] = options.summary
    ? (result.summary as unknown as Row[])
    : (result.customers as unknown as Row[]);

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
      yield { rows: outputRows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    if (options.summary) {
      process.stdout.write(`\n--- RFM Segment Breakdown (Reference Date: ${result.referenceDate}, Total Customers: ${formatNumber(result.customers.length)}) ---\n`);
      process.stdout.write(formatTable(outputRows, { style: "unicode" }));
      process.stdout.write("\n");
    } else {
      process.stdout.write(`\n--- RFM Customer Segmentation (Reference Date: ${result.referenceDate}) ---\n`);
      process.stdout.write(formatTable(outputRows.slice(0, 50), { style: "unicode" }));
      if (outputRows.length > 50) {
        process.stdout.write(`\n... showing first 50 of ${formatNumber(outputRows.length)} customers (use --to csv/json to export all)\n`);
      }
      process.stdout.write("\n");
    }
  }
}
