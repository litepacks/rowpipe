import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { fuzzyJoinTransform, type FuzzyJoinType, type FuzzyMethod } from "../../transforms/fuzzy-join.js";
import { InvalidArgumentError } from "../../core/errors.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface FuzzyJoinCommandOptions extends UnifiedPipelineCliOptions {
  on?: string;
  leftKey?: string;
  rightKey?: string;
  type?: FuzzyJoinType;
  method?: FuzzyMethod;
  threshold?: number | string;
  bestMatch?: boolean;
  scoreCol?: string;
  scoreColumn?: string;
  prefixLeft?: string;
  suffixLeft?: string;
  prefixRight?: string;
  suffixRight?: string;
  fromLeft?: string;
  fromRight?: string;
  leftSheet?: string;
  rightSheet?: string;
  caseInsensitive?: boolean;
  trim?: boolean;
}

export async function fuzzyJoinCommand(
  leftPath: string,
  rightPath: string,
  options: FuzzyJoinCommandOptions = {}
): Promise<void> {
  if (!leftPath || !rightPath) {
    throw new InvalidArgumentError("Usage: rowpipe fuzzy-join <left_file> <right_file> --on <col> [options]");
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
    delimiter: options.delimiter,
    filePath: leftPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const rightReaderInput = rightPath === "-" ? openReadableStream("-", options) : rightPath;
  const rightReader = createReader(rightReaderInput, {
    format: fromRight,
    sheet: options.rightSheet || options.sheet,
    delimiter: options.delimiter,
    filePath: rightPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const onKey = options.on || "name";
  const leftKey = options.leftKey || onKey;
  const rightKey = options.rightKey || onKey;
  const numThreshold = options.threshold !== undefined ? Number(options.threshold) : 0.75;
  const scoreCol = options.scoreCol || options.scoreColumn;

  const transform = fuzzyJoinTransform({
    rightReader,
    leftKey,
    rightKey,
    type: options.type || "left",
    method: options.method || "levenshtein",
    threshold: numThreshold,
    bestMatch: options.bestMatch !== false,
    scoreCol,
    prefixLeft: options.prefixLeft,
    suffixLeft: options.suffixLeft,
    prefixRight: options.prefixRight,
    suffixRight: options.suffixRight,
    caseInsensitive: options.caseInsensitive !== false,
    trim: options.trim !== false,
    batchSize: effectiveBatchSize,
  });

  const transformedBatches = transform(leftReader.read());

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

  await writer.write(transformedBatches);
  if (writer.close) await writer.close();
  if (leftReader.close) await leftReader.close();
  if (rightReader.close) await rightReader.close();
  progress.done();
}
