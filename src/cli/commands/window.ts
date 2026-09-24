import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { windowRows } from "../../transforms/window.js";
import { openReadableStream, openWritableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";
import { InvalidArgumentError } from "../../core/errors.js";

export interface WindowCommandOptions extends UnifiedPipelineCliOptions {
  spec?: string | string[];
  partitionBy?: string | string[];
}

/**
 * Handles `rowpipe window [input] [options]`
 */
export async function windowCommand(
  inputPath = "-",
  options: WindowCommandOptions = {}
): Promise<void> {
  const specs: Record<string, string> = {};

  const rawSpecs = options.spec
    ? Array.isArray(options.spec)
      ? options.spec
      : [options.spec]
    : options.window
    ? Array.isArray(options.window)
      ? options.window
      : [options.window]
    : [];

  for (const s of rawSpecs) {
    const eqIdx = s.indexOf("=");
    if (eqIdx > 0) {
      specs[s.slice(0, eqIdx).trim()] = s.slice(eqIdx + 1).trim();
    }
  }

  if (Object.keys(specs).length === 0) {
    throw new InvalidArgumentError(
      "At least one window spec is required (e.g. rowpipe window data.csv --spec 'rn=row_number()' --spec 'prev=lag(revenue)')"
    );
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const inputStream = openReadableStream(inputPath);
  const reader = createReader(inputStream, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    batchSize: effectiveBatchSize,
    filePath: inputPath,
  });

  const byCols = options.partitionBy || options.groupBy || options.by;

  const windowTransform = windowRows({
    specs,
    by: byCols,
    batchSize: effectiveBatchSize,
  });

  const rawOperations = buildOperationsFromOptions(options);
  const plan = optimizePipeline(rawOperations);

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = fromFormat === "xlsx" ? "csv" : fromFormat;
  }

  const writer = createWriter(openWritableStream(options.output || "-"), {
    format: toFormat,
    delimiter: options.delimiter,
  });

  const pipeline = executePlannedPipeline(reader, plan, { batchSize: effectiveBatchSize });
  pipeline.pipe(windowTransform);
  pipeline.onProgress((info) => progress.update(info));

  try {
    await pipeline.to(writer);
  } finally {
    if (reader.close) await reader.close();
    if (writer.close) await writer.close();
  }

  progress.done();
  logMemoryDebug();
}
