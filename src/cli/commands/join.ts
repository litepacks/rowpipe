import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { joinStreams } from "../../transforms/join/index.js";
import type { JoinType } from "../../transforms/join/types.js";
import { openReadableStream, openWritableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";
import { InvalidArgumentError } from "../../core/errors.js";

export interface JoinCommandOptions extends UnifiedPipelineCliOptions {
  on?: string | string[];
  leftKey?: string | string[];
  rightKey?: string | string[];
  type?: JoinType;
  prefixLeft?: string;
  suffixLeft?: string;
  prefixRight?: string;
  suffixRight?: string;
  fromLeft?: string;
  fromRight?: string;
  leftSheet?: string;
  rightSheet?: string;
  leftDelimiter?: string;
  rightDelimiter?: string;
}

/**
 * Handles `rowpipe join <left> <right> [options]`
 */
export async function joinCommand(
  leftPath: string,
  rightPath: string,
  options: JoinCommandOptions = {}
): Promise<void> {
  if (!leftPath || !rightPath) {
    throw new InvalidArgumentError("Usage: rowpipe join <left_dataset> <right_dataset> [options]");
  }

  const progress = new ProgressReporter(options);
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  let fromLeft = options.fromLeft || options.from;
  if (!fromLeft && leftPath !== "-") {
    fromLeft = inferReaderFormat(leftPath) ?? undefined;
  }

  let fromRight = options.fromRight || options.from;
  if (!fromRight && rightPath !== "-") {
    fromRight = inferReaderFormat(rightPath) ?? undefined;
  }

  const leftReaderInput = leftPath === "-" ? openReadableStream("-", options) : leftPath;
  const leftReader = createReader(leftReaderInput, {
    format: fromLeft,
    sheet: options.leftSheet || options.sheet,
    delimiter: options.leftDelimiter || options.delimiter,
    filePath: leftPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const rightReaderInput = rightPath === "-" ? openReadableStream("-", options) : rightPath;
  const rightReader = createReader(rightReaderInput, {
    format: fromRight,
    sheet: options.rightSheet || options.sheet,
    delimiter: options.rightDelimiter || options.delimiter,
    filePath: rightPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const joinedDataStream = joinStreams(leftReader, rightReader, {
    on: options.on,
    leftKey: options.leftKey,
    rightKey: options.rightKey,
    type: options.type || "inner",
    prefixLeft: options.prefixLeft,
    suffixLeft: options.suffixLeft,
    prefixRight: options.prefixRight,
    suffixRight: options.suffixRight,
    memoryLimit: options.memoryLimit,
    tempDir: options.tempDir,
    batchSize: effectiveBatchSize,
  });

  // Apply any downstream operations (e.g. filter, select, sort, limit)
  const operations = buildOperationsFromOptions(options);
  const plan = optimizePipeline(operations);

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = "csv";
  }

  const targetWriter = options.toDb
    ? createWriter(options.toDb, {
        table: options.toTable || options.table || "joined",
        transaction: options.transaction,
        createTable: options.createTable,
        upsert: options.upsert,
        conflictColumns: options.conflict ? options.conflict.split(",").map((s) => s.trim()) : undefined,
        truncate: options.truncate,
      })
    : createWriter(options.output || "-", {
        format: toFormat,
        delimiter: options.delimiter,
        ...options,
      });

  const pipeline = executePlannedPipeline(joinedDataStream, plan, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  try {
    await pipeline.to(targetWriter);
  } finally {
    if (targetWriter.close) await targetWriter.close();
  }

  progress.done();
  logMemoryDebug();
}
