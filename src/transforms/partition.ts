import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import type { DataStream, Row, TabularWriter } from "../core/types.js";
import { createWriter } from "../writers/index.js";

export interface PartitionOptions {
  by?: string | string[];
  outPattern: string;
  format?: string;
  maxOpenWriters?: number;
  delimiter?: string;
  sheet?: string;
}

interface WriterEntry {
  path: string;
  writer: TabularWriter;
  lastUsed: number;
  rowCount: number;
  initialized: boolean;
}

function sanitizePathSegment(val: any): string {
  if (val === null || val === undefined || val === "") return "_null_";
  return String(val)
    .replace(/[<>:"|?*\\/]+/g, "_")
    .trim();
}

/**
 * Manages streaming partition fan-out across multiple files based on row values.
 */
export class PartitionManager {
  private options: PartitionOptions;
  private byCols: string[];
  private writers = new Map<string, WriterEntry>();
  private createdFiles = new Set<string>();
  private maxOpenWriters: number;
  private totalPartitionRows = new Map<string, number>();

  constructor(options: PartitionOptions) {
    this.options = options;
    this.byCols = Array.isArray(options.by)
      ? options.by
      : options.by
        ? options.by.split(",").map((s) => s.trim())
        : [];
    this.maxOpenWriters = options.maxOpenWriters || 50;
  }

  /**
   * Resolves the target file path for a given row.
   */
  resolvePath(row: Row): string {
    let pattern = this.options.outPattern;

    // Pattern placeholders like {country}, {year}
    pattern = pattern.replace(/\{([^}]+)\}/g, (_match, colName) => {
      const val = row[colName];
      return sanitizePathSegment(val);
    });

    // If pattern does not contain placeholders but --by is provided and pattern is a directory
    if (!pattern.includes("{") && this.byCols.length > 0 && (pattern.endsWith("/") || pattern.endsWith("\\") || !pattern.includes("."))) {
      const parts = this.byCols.map((col) => sanitizePathSegment(row[col]));
      const ext = this.options.format ? `.${this.options.format}` : ".csv";
      const baseDir = pattern.replace(/[/\\]+$/, "");
      return `${baseDir}/${parts.join("_")}${ext}`;
    }

    return pattern;
  }

  private async getWriter(targetPath: string): Promise<WriterEntry> {
    const existing = this.writers.get(targetPath);
    if (existing) {
      existing.lastUsed = Date.now();
      return existing;
    }

    // Evict least recently used if pool is full
    if (this.writers.size >= this.maxOpenWriters) {
      let oldestKey: string | null = null;
      let oldestTime = Infinity;
      for (const [key, entry] of this.writers.entries()) {
        if (entry.lastUsed < oldestTime) {
          oldestTime = entry.lastUsed;
          oldestKey = key;
        }
      }
      if (oldestKey) {
        const evicted = this.writers.get(oldestKey);
        if (evicted) {
          if (evicted.writer.close) {
            await evicted.writer.close();
          }
          this.writers.delete(oldestKey);
        }
      }
    }

    const dir = dirname(targetPath);
    if (dir && dir !== "." && !existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    const isFirstTime = !this.createdFiles.has(targetPath);
    this.createdFiles.add(targetPath);

    // Create appropriate writer
    const writer = createWriter(targetPath, {
      format: this.options.format,
      delimiter: this.options.delimiter,
    });

    const entry: WriterEntry = {
      path: targetPath,
      writer,
      lastUsed: Date.now(),
      rowCount: 0,
      initialized: true,
    };

    this.writers.set(targetPath, entry);
    return entry;
  }

  /**
   * Partitions an input stream into multiple files.
   */
  async partition(stream: DataStream): Promise<{ fileCount: number; rowCount: number; files: string[] }> {
    let totalRows = 0;
    const pathBatches = new Map<string, Row[]>();

    for await (const batch of stream) {
      for (const row of batch.rows) {
        totalRows++;
        const targetPath = this.resolvePath(row);
        this.totalPartitionRows.set(targetPath, (this.totalPartitionRows.get(targetPath) || 0) + 1);

        let rowsForPath = pathBatches.get(targetPath);
        if (!rowsForPath) {
          rowsForPath = [];
          pathBatches.set(targetPath, rowsForPath);
        }
        rowsForPath.push(row);

        // Flush batch to disk if accumulator reaches threshold
        if (rowsForPath.length >= 500) {
          const entry = await this.getWriter(targetPath);
          async function* singleBatch() {
            yield { rows: rowsForPath!, offset: 0 };
          }
          await entry.writer.write(singleBatch());
          entry.rowCount += rowsForPath.length;
          pathBatches.set(targetPath, []);
        }
      }
    }

    // Flush remaining buffered rows
    for (const [targetPath, remainingRows] of pathBatches.entries()) {
      if (remainingRows.length > 0) {
        const entry = await this.getWriter(targetPath);
        async function* singleBatch() {
          yield { rows: remainingRows, offset: 0 };
        }
        await entry.writer.write(singleBatch());
        entry.rowCount += remainingRows.length;
      }
    }

    // Close all open writers
    for (const entry of this.writers.values()) {
      if (entry.writer.close) {
        await entry.writer.close();
      }
    }
    this.writers.clear();

    return {
      fileCount: this.createdFiles.size,
      rowCount: totalRows,
      files: Array.from(this.createdFiles),
    };
  }
}
