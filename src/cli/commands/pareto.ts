import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computePareto, type ParetoOptions } from "../../analytics/pareto.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ParetoCommandOptions extends UnifiedPipelineCliOptions, ParetoOptions {
  item?: string;
  value?: string;
}

export async function paretoCommand(
  inputPath = "-",
  options: ParetoCommandOptions = {}
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

  const result = await computePareto(pipeline.batches(), {
    itemCol: options.item || options.itemCol,
    valueCol: options.value || options.valueCol,
    summary: options.summary,
    aThreshold: options.aThreshold ? Number(options.aThreshold) : 80,
    bThreshold: options.bThreshold ? Number(options.bThreshold) : 95,
  });
  progress.done();

  const rows = result.formattedRows;

  if (options.json) {
    console.log(JSON.stringify(options.summary ? result.summary : result, null, 2));
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
    if (options.summary) {
      process.stdout.write(`\n--- Pareto ABC Analysis Summary (Total Items: ${formatNumber(result.total_items)}, Total Value: ${formatNumber(result.total_value)}) ---\n`);
      process.stdout.write(`💡 Pareto 80/20 Rule: Top ${result.pareto_80_item_percentage.toFixed(1)}% of items (${formatNumber(result.pareto_80_item_count)}) generate 80% of total value\n\n`);
      process.stdout.write(formatTable(rows, { style: "unicode" }));
      process.stdout.write("\n");
    } else {
      process.stdout.write(`\n--- Pareto 80/20 & ABC Item Ranking (Total Items: ${formatNumber(result.total_items)}, Total Value: ${formatNumber(result.total_value)}) ---\n`);
      process.stdout.write(`💡 Pareto 80/20 Rule: Top ${result.pareto_80_item_percentage.toFixed(1)}% of items (${formatNumber(result.pareto_80_item_count)}) generate 80% of total value\n\n`);
      process.stdout.write(formatTable(rows.slice(0, 50), { style: "unicode" }));
      if (rows.length > 50) {
        process.stdout.write(`\n... showing top 50 of ${formatNumber(rows.length)} items (use --to csv/json to export all)\n`);
      }
      process.stdout.write("\n");
    }
  }
}
