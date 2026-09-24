import { closeSync, existsSync, openSync, readSync, unlinkSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { safeJsonStringify } from "../../utils/formatting.js";
import type { DiffIndex, IndexedRow } from "../types.js";

interface DiskRowMetadata {
  offset: number;
  length: number;
  hash: number;
  rowNumber: number;
}

/**
 * High-performance Disk-backed Diff Index using an append-only temp file and offset index.
 * Offloads entire row object memory to disk with microsecond random-access lookups.
 */
export class DiskDiffIndex implements DiffIndex {
  private tempFilePath: string;
  private fd: number;
  private index = new Map<string, DiskRowMetadata>();
  private currentOffset = 0;
  private isClosed = false;
  private cleanupHandler?: () => void;

  constructor() {
    const randomId = randomBytes(8).toString("hex");
    this.tempFilePath = join(tmpdir(), `rowpipe-diff-${process.pid}-${randomId}.tmp`);
    this.fd = openSync(this.tempFilePath, "w+");

    this.cleanupHandler = () => {
      this.cleanupSync();
    };
    process.once("exit", this.cleanupHandler);
    process.once("SIGINT", this.cleanupHandler);
    process.once("SIGTERM", this.cleanupHandler);
  }

  async set(key: string, indexedRow: IndexedRow): Promise<void> {
    const jsonStr = safeJsonStringify(indexedRow.row);
    const buf = Buffer.from(jsonStr, "utf-8");
    const offset = this.currentOffset;

    writeSync(this.fd, buf, 0, buf.length, offset);
    this.currentOffset += buf.length;

    this.index.set(key, {
      offset,
      length: buf.length,
      hash: indexedRow.hash,
      rowNumber: indexedRow.rowNumber,
    });
  }

  async get(key: string): Promise<IndexedRow | undefined> {
    const meta = this.index.get(key);
    if (!meta) return undefined;

    const buf = Buffer.allocUnsafe(meta.length);
    readSync(this.fd, buf, 0, meta.length, meta.offset);
    const row = JSON.parse(buf.toString("utf-8"));

    return {
      rowNumber: meta.rowNumber,
      row,
      hash: meta.hash,
    };
  }

  async has(key: string): Promise<boolean> {
    return this.index.has(key);
  }

  async delete(key: string): Promise<boolean> {
    return this.index.delete(key);
  }

  async *entries(): AsyncIterable<[string, IndexedRow]> {
    for (const [key, meta] of this.index.entries()) {
      const buf = Buffer.allocUnsafe(meta.length);
      readSync(this.fd, buf, 0, meta.length, meta.offset);
      const row = JSON.parse(buf.toString("utf-8"));
      yield [key, { rowNumber: meta.rowNumber, row, hash: meta.hash }];
    }
  }

  count(): number {
    return this.index.size;
  }

  getSpillStats(): { isSpilled: boolean; spilledBytes: number; entryCount: number } {
    return {
      isSpilled: true,
      spilledBytes: this.currentOffset,
      entryCount: this.index.size,
    };
  }

  private cleanupSync(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    try {
      closeSync(this.fd);
    } catch {
      // Ignore
    }
    try {
      if (existsSync(this.tempFilePath)) {
        unlinkSync(this.tempFilePath);
      }
    } catch {
      // Ignore
    }
  }

  async close(): Promise<void> {
    if (this.cleanupHandler) {
      process.removeListener("exit", this.cleanupHandler);
      process.removeListener("SIGINT", this.cleanupHandler);
      process.removeListener("SIGTERM", this.cleanupHandler);
    }
    this.cleanupSync();
  }
}
