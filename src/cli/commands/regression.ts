import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeLinearRegression } from "../../analytics/regression.js";
import { formatTable } from "../../writers/table.js";
import type { Row } from "../../core/types.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface RegressionCommandOptions extends UnifiedPipelineCliOptions {
  x?: string;
  y?: string;
}

export async function regressionCommand(
  inputPath = "-",
  xCol?: string,
  yCol?: string,
  options: RegressionCommandOptions = {}
): Promise<void> {
  const x = xCol || options.x;
  const y = yCol || options.y;

  if (!x || !y) {
    throw new Error(
      "Both independent (X) and dependent (Y) columns are required (e.g. rowpipe regression ads.csv spend revenue)"
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

  const result = await computeLinearRegression(pipeline.batches(), x, y);
  progress.done();

  const isStructuredOutput = Boolean(options.to || options.output || options.json);
  if (isStructuredOutput) {
    const outRows: Row[] = [
      { metric: "x_column", value: result.xColumn },
      { metric: "y_column", value: result.yColumn },
      { metric: "sample_size", value: result.sampleSize },
      { metric: "slope", value: result.slope },
      { metric: "intercept", value: result.intercept },
      { metric: "pearson_r", value: result.r },
      { metric: "r_squared", value: result.r2 },
      { metric: "std_error", value: result.stdError },
      { metric: "formula", value: result.formula },
      { metric: "interpretation", value: result.interpretation },
    ];

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
      yield { rows: outRows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    process.stdout.write(`\n--- Linear Regression Model: ${y} ~ ${x} (N = ${formatNumber(result.sampleSize)}) ---\n`);

    const tableRows: Row[] = [
      { Parameter: "Regression Formula", Value: result.formula },
      { Parameter: "Slope (m)", Value: result.slope },
      { Parameter: "Intercept (b)", Value: result.intercept },
      { Parameter: "R² (Coefficient of Determination)", Value: `${(result.r2 * 100).toFixed(2)}% (${result.r2})` },
      { Parameter: "Pearson r", Value: result.r },
      { Parameter: "Standard Error", Value: result.stdError },
      { Parameter: "Fit Interpretation", Value: result.interpretation },
    ];

    process.stdout.write(formatTable(tableRows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
