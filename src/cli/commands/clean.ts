import { cleanRows } from "../../transforms/clean.js";
import { uniqueRows } from "../../transforms/unique.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream, openWritableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";

export interface CleanCommandOptions extends UnifiedPipelineCliOptions {
  trim?: boolean;
  nullValues?: string;
  fillNulls?: string | string[];
  coerce?: boolean;
  stripChars?: string;
  case?: "lower" | "upper" | "title";
  dropEmptyRows?: boolean;
}

/**
 * Handles `rowpipe clean [input] [options]`
 */
export async function cleanCommand(
  inputPath = "-",
  options: CleanCommandOptions = {}
): Promise<void> {
  const progress = new ProgressReporter(options);
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = fromFormat === "xlsx" ? "csv" : fromFormat;
  }

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  // Parse fill-nulls specs (e.g. --fill-nulls 'revenue=0' or --fill-nulls '0')
  let fillNullsParsed: Record<string, unknown> | unknown = undefined;
  if (options.fillNulls) {
    const rawList = Array.isArray(options.fillNulls) ? options.fillNulls : [options.fillNulls];
    const fillMap: Record<string, unknown> = {};
    let isGlobal = false;
    let globalVal: unknown = undefined;

    for (const item of rawList) {
      const eqIdx = item.indexOf("=");
      if (eqIdx > 0) {
        const col = item.slice(0, eqIdx).trim();
        const valStr = item.slice(eqIdx + 1).trim();
        try {
          fillMap[col] = JSON.parse(valStr);
        } catch {
          fillMap[col] = valStr;
        }
      } else {
        isGlobal = true;
        try {
          globalVal = JSON.parse(item);
        } catch {
          globalVal = item;
        }
      }
    }
    fillNullsParsed = isGlobal ? globalVal : fillMap;
  }

  const nullVals = options.nullValues ? options.nullValues.split(",").map((s) => s.trim()) : undefined;

  const cleanTransform = cleanRows({
    trim: options.trim !== false,
    nullValues: nullVals,
    fillNulls: fillNullsParsed,
    coerce: options.coerce === true,
    stripChars: options.stripChars,
    case: options.case,
    dropEmptyRows: options.dropEmptyRows,
  });

  const rawOps = buildOperationsFromOptions(options);
  const plan = optimizePipeline(rawOps);

  const writer = createWriter(options.output || "-", {
    format: toFormat,
    delimiter: options.delimiter,
    ...options,
  });

  const pipeline = executePlannedPipeline(reader, plan, { batchSize: effectiveBatchSize });
  pipeline.pipe(cleanTransform);

  if (options.unique !== undefined && options.unique !== false) {
    pipeline.pipe(uniqueRows({ by: typeof options.unique === "string" ? options.unique.split(",") : undefined }));
  }

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
