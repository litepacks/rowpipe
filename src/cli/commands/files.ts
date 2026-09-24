import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { FileSystemReader } from "../../files/reader.js";
import type { FileTypeFilter, HashAlgorithm } from "../../files/types.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openWritableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";
import { diffCommand, DiffCommandOptions } from "./diff.js";

export interface FilesCommandOptions extends UnifiedPipelineCliOptions {
  recursive?: boolean;
  maxDepth?: string | number;
  include?: string | string[];
  exclude?: string | string[];
  hidden?: boolean;
  followSymlinks?: boolean;
  type?: FileTypeFilter;
  hash?: string | false;
  mime?: boolean;
  human?: boolean;
  concurrency?: string | number;
  onError?: "abort" | "skip" | "log" | "fail";
  to?: string;
  json?: boolean;
  output?: string;
  batchSize?: string | number;
  deterministic?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
}

/**
 * Main handler for `rowpipe files <path> [options]`.
 */
export async function filesCommand(
  targetPath = ".",
  options: FilesCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const maxDepth = options.maxDepth !== undefined ? Number(options.maxDepth) : undefined;
  const concurrency = options.concurrency !== undefined ? Number(options.concurrency) : undefined;

  let hashAlgo: false | HashAlgorithm = false;
  if (options.hash) {
    const rawHash = String(options.hash).toLowerCase();
    if (["sha256", "sha1", "md5", "fast"].includes(rawHash)) {
      hashAlgo = rawHash as HashAlgorithm;
    }
  }

  // Parse includes and excludes
  const include = options.include
    ? (Array.isArray(options.include) ? options.include : [options.include])
    : undefined;

  const exclude = options.exclude
    ? (Array.isArray(options.exclude) ? options.exclude : [options.exclude])
    : undefined;

  const reader = new FileSystemReader({
    root: targetPath,
    recursive: options.recursive ?? true,
    maxDepth,
    include,
    exclude,
    hidden: options.hidden ?? false,
    followSymlinks: options.followSymlinks ?? false,
    type: options.type ?? "file",
    hash: hashAlgo,
    mime: options.mime ?? false,
    human: options.human ?? false,
    concurrency,
    onError: options.onError ?? "fail",
    deterministic: options.deterministic ?? false,
    batchSize: effectiveBatchSize,
  });

  const operations = buildOperationsFromOptions(options);
  const plan = optimizePipeline(operations);

  // Determine output format
  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = "csv";
  }

  const effectiveOutput = options.output || "-";
  const outputStream = openWritableStream(effectiveOutput);
  const writer = createWriter(outputStream, {
    format: toFormat,
  });

  const progress = new ProgressReporter(options);
  const pipeline = executePlannedPipeline(reader, plan, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  await pipeline.to(writer);
  progress.done();
  logMemoryDebug();
}

/**
 * Convenience handler for `rowpipe files snapshot <path> [options]`.
 */
export async function filesSnapshotCommand(
  targetPath = ".",
  options: FilesCommandOptions = {}
): Promise<void> {
  const snapshotOptions: FilesCommandOptions = {
    ...options,
    deterministic: true,
    hash: options.hash || "sha256",
    to: options.to || (options.json ? "json" : "jsonl"),
  };

  await filesCommand(targetPath, snapshotOptions);
}

/**
 * Convenience handler for `rowpipe files diff <left> <right> [options]`.
 */
export async function filesDiffCommand(
  leftPath: string,
  rightPath: string,
  options: DiffCommandOptions & FilesCommandOptions = {}
): Promise<void> {
  const diffOpts: DiffCommandOptions = {
    ...options,
    fromLeft: "files",
    fromRight: "files",
    key: options.key || "relative_path",
    ignore: options.ignore || "modified_at,accessed_at,created_at",
  };

  await diffCommand(leftPath, rightPath, diffOpts);
}
