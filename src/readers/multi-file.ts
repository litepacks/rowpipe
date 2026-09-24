import { promises as fsPromises } from "node:fs";
import * as path from "node:path";
import { InvalidArgumentError, RowpipeError } from "../core/errors.js";
import type { DataBatch, DataStream, InspectionMetadata, ReaderOptions, TabularReader } from "../core/types.js";
import { createGlobMatcher, normalizeGlobPath } from "../files/glob.js";
import { openReadableStream } from "../utils/compression.js";
import { createReader, inferFormatFromPath } from "./index.js";

export interface MultiFileReaderOptions extends ReaderOptions {
  files?: string[];
  pattern?: string;
  addFilename?: boolean;
  fileCol?: string;
  strictSchema?: boolean;
}

/**
 * Checks if a string contains glob wildcard characters (*, ?, [, {).
 */
export function isGlobPattern(str: string): boolean {
  return /[*?[\]{}]/.test(str);
}

/**
 * Expands a glob pattern or path list into an array of existing file paths.
 */
export async function resolveFilePatterns(patternOrPaths: string | string[], baseDir = process.cwd()): Promise<string[]> {
  const inputs = Array.isArray(patternOrPaths) ? patternOrPaths : [patternOrPaths];
  const matchedFiles: string[] = [];

  for (const input of inputs) {
    if (!isGlobPattern(input)) {
      matchedFiles.push(path.isAbsolute(input) ? input : path.resolve(baseDir, input));
      continue;
    }

    // Determine root directory before first wildcard
    const normalized = normalizeGlobPath(input);
    const wildcardIdx = normalized.search(/[*?[\]{}]/);
    const prefix = wildcardIdx >= 0 ? normalized.slice(0, wildcardIdx) : "";
    const lastSlash = prefix.lastIndexOf("/");
    const searchRootRel = lastSlash >= 0 ? prefix.slice(0, lastSlash) : ".";
    const searchRootAbs = path.resolve(baseDir, searchRootRel);

    const matcher = createGlobMatcher(input);

    async function walk(dir: string): Promise<void> {
      let entries;
      try {
        entries = await fsPromises.readdir(dir, { withFileTypes: true });
      } catch {
        return;
      }

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        const relFromBase = path.relative(baseDir, fullPath);

        if (entry.isDirectory()) {
          await walk(fullPath);
        } else if (entry.isFile()) {
          if (matcher(relFromBase) || matcher(fullPath) || matcher(entry.name)) {
            matchedFiles.push(fullPath);
          }
        }
      }
    }

    await walk(searchRootAbs);
  }

  // Sort files for deterministic reading order
  matchedFiles.sort();
  return Array.from(new Set(matchedFiles));
}

/**
 * Streaming multi-file and glob reader that concatenates multiple tabular files
 * into a single unified DataStream with bounded memory and sequential file reading.
 */
export class MultiFileReader implements TabularReader {
  private files: string[];
  private options: MultiFileReaderOptions;

  constructor(filesOrPattern: string | string[], options: MultiFileReaderOptions = {}) {
    this.options = options;
    if (Array.isArray(filesOrPattern)) {
      this.files = filesOrPattern;
    } else {
      this.files = [filesOrPattern];
    }
  }

  private async getResolvedFiles(): Promise<string[]> {
    if (this.files.some(isGlobPattern) || this.files.length > 1) {
      const resolved = await resolveFilePatterns(this.files);
      return resolved;
    }
    return this.files;
  }

  public async *read(readOptions?: ReaderOptions): DataStream {
    const files = await this.getResolvedFiles();
    if (files.length === 0) {
      throw new RowpipeError(`No files found matching pattern: ${this.files.join(", ")}`);
    }

    const effectiveBatchSize = readOptions?.batchSize || this.options.batchSize || 1000;
    const fileColName = this.options.fileCol || (this.options.addFilename ? "_file" : undefined);
    let globalOffset = 0;
    let totalRowsYielded = 0;
    const maxRows = readOptions?.maxRows ?? this.options.maxRows ?? Infinity;

    for (const filePath of files) {
      if (totalRowsYielded >= maxRows) {
        break;
      }

      const format = this.options.format || inferFormatFromPath(filePath) || "csv";
      const reader = createReader(filePath, {
        ...this.options,
        ...readOptions,
        format,
        filePath,
        batchSize: effectiveBatchSize,
      });

      try {
        const fileBaseName = path.basename(filePath);
        for await (const batch of reader.read({ batchSize: effectiveBatchSize, signal: readOptions?.signal })) {
          if (readOptions?.signal?.aborted) {
            break;
          }

          let rows = batch.rows;
          if (fileColName) {
            rows = rows.map((r) => ({ [fileColName]: fileBaseName, ...r }));
          }

          if (totalRowsYielded + rows.length > maxRows) {
            rows = rows.slice(0, maxRows - totalRowsYielded);
          }

          if (rows.length > 0) {
            yield {
              rows,
              offset: globalOffset,
            };
            globalOffset += rows.length;
            totalRowsYielded += rows.length;
          }

          if (totalRowsYielded >= maxRows) {
            break;
          }
        }
      } finally {
        if (reader.close) {
          await reader.close();
        }
      }
    }
  }

  public async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    const files = await this.getResolvedFiles();
    if (files.length === 0) {
      throw new RowpipeError(`No files found matching pattern: ${this.files.join(", ")}`);
    }

    const firstFile = files[0]!;
    const format = this.options.format || inferFormatFromPath(firstFile) || "csv";
    const reader = createReader(firstFile, {
      ...this.options,
      ...options,
      format,
      filePath: firstFile,
    });

    try {
      let meta: InspectionMetadata = {
        format: `multi-file (${files.length} files)`,
        sheetsCount: files.length,
        sheets: files.map((f) => ({ name: path.basename(f), rowCount: 0 })),
      };

      if (reader.inspect) {
        const singleMeta = await reader.inspect(options);
        meta.columns = singleMeta.columns;
        meta.columnCount = singleMeta.columnCount;
      }

      return meta;
    } finally {
      if (reader.close) {
        await reader.close();
      }
    }
  }
}
