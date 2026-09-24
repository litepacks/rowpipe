import { createWriteStream } from "node:fs";
import { DiffMismatchError, InvalidArgumentError } from "../../core/errors.js";
import type { ReaderOptions } from "../../core/types.js";
import { computeDiff, diffRows } from "../../diff/engine.js";
import {
  formatJson,
  formatPatchEvent,
  formatRowEvent,
  formatSummary,
} from "../../diff/reporter.js";
import type {
  DiffEngineOptions,
  DiffOnlyFilter,
  DiffOutputFormat,
  DuplicateKeyPolicy,
} from "../../diff/types.js";
import { formatNumber } from "../../utils/formatting.js";
import { parseMemoryLimit } from "../../diff/storage/spillable-index.js";

export interface DiffCommandOptions {
  key?: string;
  format?: DiffOutputFormat;
  only?: DiffOnlyFilter;
  columns?: string;
  ignore?: string;
  limit?: string | number;
  json?: boolean;
  output?: string;
  failOnDiff?: boolean;
  duplicateKey?: DuplicateKeyPolicy;
  coerce?: boolean;
  epsilon?: string | number;
  ignoreCase?: boolean;
  trim?: boolean;
  memoryLimit?: string;
  sheet?: string;
  leftSheet?: string;
  rightSheet?: string;
  from?: string;
  fromLeft?: string;
  fromRight?: string;
  schema?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
  batchSize?: string | number;
}

export async function diffCommand(
  leftPath: string,
  rightPath: string,
  options: DiffCommandOptions = {}
): Promise<void> {
  if (leftPath === "-" && rightPath === "-") {
    throw new InvalidArgumentError(
      "Cannot diff stdin against stdin ('- -'). One side must be a file or named source."
    );
  }

  if (!options.key) {
    throw new InvalidArgumentError("Missing required option: --key <column1,column2,...>");
  }

  const keys = options.key.split(",").map((k) => k.trim()).filter(Boolean);
  const compareColumns = options.columns
    ? options.columns.split(",").map((c) => c.trim()).filter(Boolean)
    : undefined;
  const ignoreColumns = options.ignore
    ? options.ignore.split(",").map((c) => c.trim()).filter(Boolean)
    : undefined;

  const leftSheet = options.leftSheet || options.sheet;
  const rightSheet = options.rightSheet || options.sheet;
  const leftFormat = options.fromLeft || options.from;
  const rightFormat = options.fromRight || options.from;
  const batchSize = Number(options.batchSize) || 1000;

  const leftOptions: ReaderOptions = {
    sheet: leftSheet,
    format: leftFormat,
    batchSize,
    filePath: leftPath,
  };

  const rightOptions: ReaderOptions = {
    sheet: rightSheet,
    format: rightFormat,
    batchSize,
    filePath: rightPath,
  };

  const isTty = process.stderr.isTTY && !options.quiet && options.noProgress !== true;

  const onProgress = (info: any) => {
    if (!isTty) return;
    const isSpilledText = info.isSpilled ? " (disk spilled)" : "";
    if (info.phase === "indexing_left") {
      process.stderr.write(
        `\r\x1b[2K  Indexing left: ${formatNumber(info.leftRowsProcessed)} rows${isSpilledText}...`
      );
    } else if (info.phase === "comparing_right") {
      process.stderr.write(
        `\r\x1b[2K  Comparing right: ${formatNumber(info.rightRowsProcessed)} rows [added: ${formatNumber(info.addedCount)}, changed: ${formatNumber(info.changedCount)}, unchanged: ${formatNumber(info.unchangedCount)}]${isSpilledText}...`
      );
    } else if (info.phase === "finishing") {
      process.stderr.write(`\r\x1b[2K  Done comparing.\n`);
    }
  };

  const engineOptions: DiffEngineOptions = {
    left: leftPath,
    right: rightPath,
    keys,
    columns: compareColumns,
    ignore: ignoreColumns,
    duplicateKey: options.duplicateKey || "error",
    coerce: options.coerce,
    epsilon: options.epsilon !== undefined ? Number(options.epsilon) : undefined,
    ignoreCase: options.ignoreCase,
    trim: options.trim,
    memoryLimitBytes: parseMemoryLimit(options.memoryLimit),
    includeSchema: options.schema !== false,
    leftOptions,
    rightOptions,
    onProgress,
  };

  const formatMode = options.format || "summary";
  const limit = options.limit !== undefined ? Number(options.limit) : undefined;
  const onlyFilter = options.only;

  let outStream = process.stdout;
  let fileStream: any = null;

  if (options.output) {
    fileStream = createWriteStream(options.output, "utf-8");
    outStream = fileStream;
  }

  const writeOut = async (chunk: string): Promise<void> => {
    if (!outStream.write(chunk)) {
      await new Promise<void>((resolve) => outStream.once("drain", resolve));
    }
  };

  try {
    if (options.json || formatMode === "summary") {
      const summary = await computeDiff(engineOptions);
      if (isTty) {
        process.stderr.write("\r\x1b[2K");
      }

      if (options.json) {
        await writeOut(formatJson(summary));
      } else {
        await writeOut(formatSummary(summary));
      }

      const totalDiffs =
        summary.rows.added + summary.rows.removed + summary.rows.changed;
      if (options.failOnDiff && totalDiffs > 0) {
        throw new DiffMismatchError(totalDiffs);
      }
      return;
    }

    // -----------------------------------------------------------
    // Streaming row-level output (rows or patch)
    // -----------------------------------------------------------
    let emittedCount = 0;
    let totalDiffCount = 0;

    for await (const event of diffRows(engineOptions)) {
      if (event.type === "added" || event.type === "removed" || event.type === "changed") {
        totalDiffCount++;
      }

      // Filter by --only
      if (onlyFilter && event.type !== onlyFilter) {
        continue;
      }

      if (limit !== undefined && emittedCount >= limit) {
        continue;
      }

      let formattedText: string | null = null;
      if (formatMode === "rows") {
        formattedText = formatRowEvent(event);
      } else if (formatMode === "patch") {
        formattedText = formatPatchEvent(event);
      }

      if (formattedText) {
        await writeOut(formattedText);
        emittedCount++;
      }
    }

    if (isTty) {
      process.stderr.write("\r\x1b[2K");
    }

    if (options.failOnDiff && totalDiffCount > 0) {
      throw new DiffMismatchError(totalDiffCount);
    }
  } finally {
    if (fileStream) {
      await new Promise<void>((resolve) => fileStream.end(() => resolve()));
    }
  }
}
