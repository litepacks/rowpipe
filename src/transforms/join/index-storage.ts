import { closeSync, existsSync, openSync, readSync, unlinkSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import type { Row } from "../../core/types.js";
import { parseMemoryLimit } from "../../diff/storage/spillable-index.js";
import { safeJsonStringify } from "../../utils/formatting.js";
import type { JoinIndex } from "./types.js";

interface DiskRowLocation {
  offset: number;
  length: number;
}

/**
 * High-performance Spillable Join Index that stores build-side rows in memory,
 * and transparently spills to disk when memory threshold is reached.
 * Uses append-only disk storage with microsecond random-access read offsets.
 */
export class SpillableJoinIndex implements JoinIndex {
  private memoryMap = new Map<string, Row[]>();
  private matchedKeys = new Set<string>();
  private estimatedBytes = 0;
  private memoryLimitBytes: number;
  private isSpilled = false;

  private tempDir?: string;
  private tempFilePath?: string;
  private fd?: number;
  private currentOffset = 0;
  private diskIndex = new Map<string, DiskRowLocation[]>();
  private totalCount = 0;
  private isClosed = false;
  private cleanupHandler?: () => void;

  constructor(options?: { memoryLimit?: number | string; tempDir?: string }) {
    this.memoryLimitBytes = parseMemoryLimit(options?.memoryLimit, 256 * 1024 * 1024);
    this.tempDir = options?.tempDir;
  }

  private spillToDisk(): void {
    if (this.isSpilled) return;

    const tmpFolder = this.tempDir || tmpdir();
    const randomId = randomBytes(8).toString("hex");
    this.tempFilePath = join(tmpFolder, `rowpipe_join_${process.pid}_${randomId}.tmp`);
    this.fd = openSync(this.tempFilePath, "w+");

    this.cleanupHandler = () => {
      this.cleanupSync();
    };
    process.once("exit", this.cleanupHandler);
    process.once("SIGINT", this.cleanupHandler);
    process.once("SIGTERM", this.cleanupHandler);

    for (const [key, rows] of this.memoryMap.entries()) {
      let list = this.diskIndex.get(key);
      if (!list) {
        list = [];
        this.diskIndex.set(key, list);
      }
      for (const r of rows) {
        const jsonStr = safeJsonStringify(r);
        const buf = Buffer.from(jsonStr, "utf-8");
        const offset = this.currentOffset;
        writeSync(this.fd, buf, 0, buf.length, offset);
        this.currentOffset += buf.length;
        list.push({ offset, length: buf.length });
      }
    }

    this.memoryMap.clear();
    this.isSpilled = true;
  }

  async set(key: string, row: Row): Promise<void> {
    this.totalCount++;

    if (this.isSpilled && this.fd !== undefined) {
      const jsonStr = safeJsonStringify(row);
      const buf = Buffer.from(jsonStr, "utf-8");
      const offset = this.currentOffset;
      writeSync(this.fd, buf, 0, buf.length, offset);
      this.currentOffset += buf.length;

      let list = this.diskIndex.get(key);
      if (!list) {
        list = [];
        this.diskIndex.set(key, list);
      }
      list.push({ offset, length: buf.length });
      return;
    }

    let existing = this.memoryMap.get(key);
    if (!existing) {
      existing = [];
      this.memoryMap.set(key, existing);
    }
    existing.push(row);

    // Size estimate: key bytes + base object overhead + properties
    this.estimatedBytes += key.length * 2 + 128 + Object.keys(row).length * 32;

    if (this.estimatedBytes >= this.memoryLimitBytes) {
      this.spillToDisk();
    }
  }

  async get(key: string): Promise<Row[] | undefined> {
    if (this.isSpilled && this.fd !== undefined) {
      const list = this.diskIndex.get(key);
      if (!list || list.length === 0) return undefined;

      const results: Row[] = [];
      for (const loc of list) {
        const buf = Buffer.allocUnsafe(loc.length);
        readSync(this.fd, buf, 0, loc.length, loc.offset);
        results.push(JSON.parse(buf.toString("utf-8")));
      }
      return results;
    }
    return this.memoryMap.get(key);
  }

  async has(key: string): Promise<boolean> {
    if (this.isSpilled) {
      return this.diskIndex.has(key);
    }
    return this.memoryMap.has(key);
  }

  markMatched(key: string): void {
    this.matchedKeys.add(key);
  }

  async *getUnmatched(): AsyncIterable<Row> {
    if (this.isSpilled && this.fd !== undefined) {
      for (const [key, list] of this.diskIndex.entries()) {
        if (!this.matchedKeys.has(key)) {
          for (const loc of list) {
            const buf = Buffer.allocUnsafe(loc.length);
            readSync(this.fd, buf, 0, loc.length, loc.offset);
            yield JSON.parse(buf.toString("utf-8"));
          }
        }
      }
    } else {
      for (const [k, rows] of this.memoryMap.entries()) {
        if (!this.matchedKeys.has(k)) {
          for (const row of rows) {
            yield row;
          }
        }
      }
    }
  }

  count(): number {
    return this.totalCount;
  }

  private cleanupSync(): void {
    if (this.isClosed) return;
    this.isClosed = true;

    if (this.fd !== undefined) {
      try {
        closeSync(this.fd);
      } catch {
        // Ignore
      }
      this.fd = undefined;
    }

    if (this.tempFilePath) {
      try {
        if (existsSync(this.tempFilePath)) {
          unlinkSync(this.tempFilePath);
        }
      } catch {
        // Ignore
      }
      this.tempFilePath = undefined;
    }
  }

  async close(): Promise<void> {
    this.memoryMap.clear();
    this.matchedKeys.clear();
    this.diskIndex.clear();

    if (this.cleanupHandler) {
      process.removeListener("exit", this.cleanupHandler);
      process.removeListener("SIGINT", this.cleanupHandler);
      process.removeListener("SIGTERM", this.cleanupHandler);
      this.cleanupHandler = undefined;
    }

    this.cleanupSync();
  }
}
