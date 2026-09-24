import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeCrosstab, formatCrosstabRows } from "../../analytics/crosstab.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface CrosstabCommandOptions extends UnifiedPipelineCliOptions {
  row?: string;
  col?: string;
  top?: string | number;
  topRows?: string | number;
  topCols?: string | number;
  normalize?: "row" | "col" | "all";
}

export async function crosstabCommand(
  inputPath = "-",
  rowCol?: string,
  colCol?: string,
  options: CrosstabCommandOptions = {}
): Promise<void> {
  const rCol = rowCol || options.row;
  const cCol = colCol || options.col;

  if (!rCol || !cCol) {
    throw new Error(
      "Both row and column dimensions are required for crosstab (e.g. rowpipe crosstab netflix.csv type rating)"
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

  const topLimit = options.top !== undefined ? Number(options.top) : undefined;
  const result = await computeCrosstab(pipeline.batches(), rCol, cCol, {
    topRows: options.topRows !== undefined ? Number(options.topRows) : topLimit,
    topCols: options.topCols !== undefined ? Number(options.topCols) : topLimit,
    normalize: options.normalize,
  });
  progress.done();

  const formattedRows = formatCrosstabRows(result, options.normalize);

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
      yield { rows: formattedRows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    process.stdout.write(
      `\n--- Cross-Tabulation: ${rCol} × ${cCol} (Grand Total: ${formatNumber(result.grandTotal)} | Chi-Square: ${result.chiSquare.toFixed(2)}, df: ${result.degreesOfFreedom}) ---\n`
    );
    process.stdout.write(formatTable(formattedRows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
