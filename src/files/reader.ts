import { promises as fsPromises, Dirent, Stats } from "node:fs";
import * as path from "node:path";
import { FileSystemError, InvalidArgumentError } from "../core/errors.js";
import type { DataBatch, DataStream, InspectionMetadata, ReaderOptions, TabularReader } from "../core/types.js";
import { createGlobMatcher, normalizeGlobPath } from "./glob.js";
import { computeFileHash } from "./hash.js";
import { inferMimeAndCategory } from "./mime.js";
import { formatBytes } from "../utils/formatting.js";
import type { FileRecord, FileSystemReaderOptions, FileType } from "./types.js";

/**
 * Normalizes a relative path to standard forward slashes.
 */
function normalizeRelativePath(relPath: string): string {
  let normalized = relPath.replace(/\\/g, "/");
  while (normalized.startsWith("./")) {
    normalized = normalized.slice(2);
  }
  return normalized || ".";
}

/**
 * First-class streaming filesystem reader.
 * Converts directories and files into streaming tabular DataBatches.
 */
export class FileSystemReader implements TabularReader {
  private readonly options: FileSystemReaderOptions;

  constructor(optionsOrRoot: string | FileSystemReaderOptions) {
    if (typeof optionsOrRoot === "string") {
      this.options = { root: optionsOrRoot };
    } else {
      this.options = { ...optionsOrRoot };
    }
  }

  /**
   * Reads the filesystem stream yielding DataBatches.
   */
  public async *read(readOptions?: ReaderOptions): DataStream {
    const rootPath = path.resolve(this.options.root);
    const recursive = this.options.recursive ?? true;
    const maxDepth = this.options.maxDepth ?? Infinity;
    const includeHidden = this.options.hidden ?? false;
    const followSymlinks = this.options.followSymlinks ?? false;
    const typeFilter = this.options.type ?? "file";
    const hashAlgorithm = this.options.hash ?? false;
    const enableMime = this.options.mime ?? false;
    const concurrency = Math.max(1, this.options.concurrency ?? 8);
    const onError = this.options.onError ?? "fail";
    const deterministic = this.options.deterministic ?? false;
    const batchSize = readOptions?.batchSize ?? this.options.batchSize ?? 1000;

    const includeMatcher = this.options.include ? createGlobMatcher(this.options.include) : () => true;
    const excludeMatcher = this.options.exclude
      ? createGlobMatcher(this.options.exclude)
      : () => false;

    // Check if root exists
    let rootStat: Stats;
    try {
      rootStat = await fsPromises.stat(rootPath);
    } catch (err: unknown) {
      const fsErr = new FileSystemError(`Root path not found or unreadable: "${rootPath}"`, {
        path: rootPath,
        operation: "stat",
        cause: err instanceof Error ? err : String(err),
      });
      if (onError === "fail") throw fsErr;
      return;
    }

    let rowsAccumulator: FileRecord[] = [];
    let offset = 0;

    // Helper to flush batch
    const flushBatch = (): DataBatch | null => {
      if (rowsAccumulator.length >= batchSize) {
        const batch: DataBatch = {
          rows: rowsAccumulator as unknown as Record<string, unknown>[],
          offset,
        };
        offset += rowsAccumulator.length;
        rowsAccumulator = [];
        return batch;
      }
      return null;
    };

    // Helper to enrich a single entry
    const enrichFileEntry = async (
      fullPath: string,
      relPath: string,
      dirPath: string,
      name: string,
      stat: Stats,
      isSymlink: boolean
    ): Promise<FileRecord | null> => {
      try {
        const dotIdx = name.lastIndexOf(".");
        let basename = name;
        let extension = "";
        if (dotIdx > 0) {
          basename = name.slice(0, dotIdx);
          extension = name.slice(dotIdx + 1).toLowerCase();
        }

        let fileType: FileType = "file";
        if (isSymlink) {
          fileType = "symlink";
        } else if (stat.isDirectory()) {
          fileType = "directory";
        }

        const normRelPath = normalizeRelativePath(relPath);

        const record: FileRecord = {
          path: fullPath,
          relative_path: normRelPath,
          name,
          basename,
          extension,
          directory: dirPath,
          type: fileType,
          size: stat.isDirectory() ? 0 : stat.size,
          created_at: stat.birthtime ? stat.birthtime.toISOString() : stat.ctime.toISOString(),
          modified_at: stat.mtime.toISOString(),
          mode: stat.mode,
          is_file: stat.isFile(),
          is_directory: stat.isDirectory(),
          is_symlink: isSymlink,
        };

        if (!deterministic) {
          record.accessed_at = stat.atime.toISOString();
        }

        if (enableMime || this.options.category) {
          const { mime, category } = inferMimeAndCategory(extension || name);
          if (enableMime) record.mime = mime;
          record.category = category;
        }

        if (this.options.human) {
          record.size_human = formatBytes(record.size);
        }

        if (hashAlgorithm && stat.isFile()) {
          record.hash = await computeFileHash(fullPath, hashAlgorithm);
        }

        return record;
      } catch (err: unknown) {
        const fsErr = new FileSystemError(`Failed to process file "${fullPath}"`, {
          path: fullPath,
          operation: "enrich",
          cause: err instanceof Error ? err : String(err),
        });
        if (onError === "fail") throw fsErr;
        return null;
      }
    };

    // Case 1: Root is a single file
    if (!rootStat.isDirectory()) {
      const name = path.basename(rootPath);
      const dirPath = path.dirname(rootPath);
      const record = await enrichFileEntry(rootPath, name, dirPath, name, rootStat, false);
      if (record) {
        yield {
          rows: [record as unknown as Record<string, unknown>],
          offset: 0,
        };
      }
      return;
    }

    // Case 2: Root is a directory - walk with bounded memory and concurrency
    const visitedRealPaths = new Set<string>();
    visitedRealPaths.add(rootPath);

    interface WorkItem {
      fullPath: string;
      relPath: string;
      dirent: Dirent;
    }

    const dirQueue: Array<{ dirPath: string; depth: number }> = [{ dirPath: rootPath, depth: 0 }];

    while (dirQueue.length > 0) {
      const current = dirQueue.shift()!;
      let dirHandle;

      try {
        dirHandle = await fsPromises.opendir(current.dirPath);
      } catch (err: unknown) {
        const fsErr = new FileSystemError(`Cannot open directory "${current.dirPath}"`, {
          path: current.dirPath,
          operation: "opendir",
          cause: err instanceof Error ? err : String(err),
        });
        if (onError === "fail") throw fsErr;
        continue;
      }

      const pendingWork: WorkItem[] = [];

      try {
        for await (const dirent of dirHandle) {
          const entryName = dirent.name;

          // Hidden file filtering
          if (!includeHidden && entryName.startsWith(".")) {
            continue;
          }

          // Default safe exclusions when not hidden
          if (!includeHidden && (entryName === ".git" || entryName === "node_modules")) {
            continue;
          }

          const fullPath = path.join(current.dirPath, entryName);
          const relPath = normalizeRelativePath(path.relative(rootPath, fullPath));

          // Check excludes
          if (excludeMatcher(relPath)) {
            continue;
          }

          if (dirent.isDirectory()) {
            if (recursive && current.depth + 1 <= maxDepth) {
              dirQueue.push({ dirPath: fullPath, depth: current.depth + 1 });
            }

            if (typeFilter === "directory" || typeFilter === "all") {
              if (includeMatcher(relPath)) {
                pendingWork.push({ fullPath, relPath, dirent });
              }
            }
          } else if (dirent.isSymbolicLink()) {
            if (followSymlinks) {
              try {
                const realPath = await fsPromises.realpath(fullPath);
                if (visitedRealPaths.has(realPath)) {
                  // Cycle detected, skip
                  continue;
                }
                visitedRealPaths.add(realPath);

                const targetStat = await fsPromises.stat(fullPath);
                if (targetStat.isDirectory()) {
                  if (recursive && current.depth + 1 <= maxDepth) {
                    dirQueue.push({ dirPath: fullPath, depth: current.depth + 1 });
                  }
                  if (typeFilter === "directory" || typeFilter === "all") {
                    if (includeMatcher(relPath)) {
                      pendingWork.push({ fullPath, relPath, dirent });
                    }
                  }
                } else if (targetStat.isFile()) {
                  if (typeFilter === "file" || typeFilter === "symlink" || typeFilter === "all") {
                    if (includeMatcher(relPath)) {
                      pendingWork.push({ fullPath, relPath, dirent });
                    }
                  }
                }
              } catch (err: unknown) {
                if (onError === "fail") {
                  throw new FileSystemError(`Broken symlink at "${fullPath}"`, {
                    path: fullPath,
                    operation: "realpath",
                    cause: err instanceof Error ? err : String(err),
                  });
                }
              }
            } else {
              if (typeFilter === "symlink" || typeFilter === "all") {
                if (includeMatcher(relPath)) {
                  pendingWork.push({ fullPath, relPath, dirent });
                }
              }
            }
          } else if (dirent.isFile()) {
            if (typeFilter === "file" || typeFilter === "all") {
              if (includeMatcher(relPath)) {
                pendingWork.push({ fullPath, relPath, dirent });
              }
            }
          }
        }
      } finally {
        // opendir automatically closes on iterator completion
      }

      // Process pending work in chunks using bounded concurrency pool
      for (let i = 0; i < pendingWork.length; i += concurrency) {
        const chunk = pendingWork.slice(i, i + concurrency);
        const results = await Promise.all(
          chunk.map(async (item) => {
            try {
              const isSymlink = item.dirent.isSymbolicLink();
              const stat = isSymlink && !followSymlinks
                ? await fsPromises.lstat(item.fullPath)
                : await fsPromises.stat(item.fullPath);
              return await enrichFileEntry(
                item.fullPath,
                item.relPath,
                current.dirPath,
                item.dirent.name,
                stat,
                isSymlink
              );
            } catch (err: unknown) {
              const fsErr = new FileSystemError(`Cannot stat "${item.fullPath}"`, {
                path: item.fullPath,
                operation: "stat",
                cause: err instanceof Error ? err : String(err),
              });
              if (onError === "fail") throw fsErr;
              return null;
            }
          })
        );

        for (const record of results) {
          if (record) {
            rowsAccumulator.push(record);
            const batch = flushBatch();
            if (batch) {
              yield batch;
            }
          }
        }
      }
    }

    // Flush any remaining rows
    if (rowsAccumulator.length > 0) {
      yield {
        rows: rowsAccumulator as unknown as Record<string, unknown>[],
        offset,
      };
      offset += rowsAccumulator.length;
      rowsAccumulator = [];
    }
  }

  /**
   * Inspects filesystem directory metadata without full memory accumulation.
   */
  public async inspect(_options?: ReaderOptions): Promise<InspectionMetadata> {
    let totalFiles = 0;
    let totalSizeBytes = 0;
    const extensionCounts = new Map<string, number>();

    for await (const batch of this.read({ batchSize: 5000 })) {
      for (const row of batch.rows) {
        totalFiles += 1;
        totalSizeBytes += Number(row["size"] ?? 0);
        const ext = String(row["extension"] ?? "") || "<none>";
        extensionCounts.set(ext, (extensionCounts.get(ext) ?? 0) + 1);
      }
    }

    const columns: InspectionMetadata["columns"] = [
      { name: "path", type: "string", nullPercentage: 0 },
      { name: "relative_path", type: "string", nullPercentage: 0 },
      { name: "name", type: "string", nullPercentage: 0 },
      { name: "extension", type: "string", nullPercentage: 0 },
      { name: "type", type: "string", nullPercentage: 0 },
      { name: "size", type: "integer", nullPercentage: 0 },
      { name: "created_at", type: "date", nullPercentage: 0 },
      { name: "modified_at", type: "date", nullPercentage: 0 },
      { name: "is_file", type: "boolean", nullPercentage: 0 },
      { name: "is_directory", type: "boolean", nullPercentage: 0 },
    ];

    if (this.options.hash) {
      columns.push({ name: "hash", type: "string", nullPercentage: 0 });
    }
    if (this.options.mime) {
      columns.push({ name: "mime", type: "string", nullPercentage: 0 });
    }

    return {
      format: "FILESYSTEM",
      rowCount: totalFiles,
      sizeBytes: totalSizeBytes,
      columnCount: columns.length,
      columns,
    };
  }
}
