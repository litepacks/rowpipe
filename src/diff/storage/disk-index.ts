import { closeSync, existsSync, openSync, readSync, unlinkSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { safeJsonReplacer } from "../../utils/formatting.js";
import type { DiffIndex, IndexedRow } from "../types.js";

interface DiskRowMetadata {
  offset: number;
  length: number;
  hash: number;
  rowNumber: number;
}

function fastStringifyRow(row: unknown): string {
  try {
    return JSON.stringify(row);
  } catch {
    return JSON.stringify(row, safeJsonReplacer);
  }
}

/**
 * High-performance Disk-backed Diff Index using an append-only temp file and offset index.
 * Offloads entire row object memory to disk with microsecond random-access lookups,
 * chunked write buffering, and 512KB read-window caching.
 */
export class DiskDiffIndex implements DiffIndex {
  private tempFilePath: string;
  private fd: number;
  private index = new Map<string, DiskRowMetadata>();
  private currentOffset = 0;
  private isClosed = false;
  private cleanupHandler?: () => void;

  // Chunked Write Buffer (512 KB) to batch system write calls
  private readonly bufferSize = 512 * 1024;
  private writeBuffer: Buffer;
  private writeBufferPos = 0;
  private writeBufferFileOffset = 0;

  // Chunked Read Window Cache (512 KB) to batch system read calls
  private readBlockBuffer: Buffer;
  private readBlockOffset = -1;
  private readBlockLength = 0;

  constructor() {
    const randomId = randomBytes(8).toString("hex");
    this.tempFilePath = join(tmpdir(), `rowpipe-diff-${process.pid}-${randomId}.tmp`);
    this.fd = openSync(this.tempFilePath, "w+");
    this.writeBuffer = Buffer.allocUnsafe(this.bufferSize);
    this.readBlockBuffer = Buffer.allocUnsafe(this.bufferSize);

    this.cleanupHandler = () => {
      this.cleanupSync();
    };
    process.once("exit", this.cleanupHandler);
    process.once("SIGINT", this.cleanupHandler);
    process.once("SIGTERM", this.cleanupHandler);
  }

  private flushWriteBuffer(): void {
    if (this.writeBufferPos > 0) {
      writeSync(this.fd, this.writeBuffer, 0, this.writeBufferPos, this.writeBufferFileOffset);
      this.writeBufferFileOffset += this.writeBufferPos;
      this.writeBufferPos = 0;
    }
  }

  async set(key: string, indexedRow: IndexedRow): Promise<void> {
    const jsonStr = fastStringifyRow(indexedRow.row);
    const byteLen = Buffer.byteLength(jsonStr, "utf-8");
    const offset = this.currentOffset;

    if (byteLen > this.bufferSize) {
      // Very large row: flush current buffer and write directly
      this.flushWriteBuffer();
      const largeBuf = Buffer.from(jsonStr, "utf-8");
      writeSync(this.fd, largeBuf, 0, byteLen, offset);
      this.writeBufferFileOffset += byteLen;
    } else {
      if (this.writeBufferPos + byteLen > this.bufferSize) {
        this.flushWriteBuffer();
      }
      this.writeBuffer.write(jsonStr, this.writeBufferPos, byteLen, "utf-8");
      this.writeBufferPos += byteLen;
    }

    this.currentOffset += byteLen;

    this.index.set(key, {
      offset,
      length: byteLen,
      hash: indexedRow.hash,
      rowNumber: indexedRow.rowNumber,
    });
  }

  async get(key: string): Promise<IndexedRow | undefined> {
    const meta = this.index.get(key);
    if (!meta) return undefined;

    let jsonStr: string;
    // 1. Check if the record is still in the active in-memory write buffer
    if (meta.offset >= this.writeBufferFileOffset && meta.offset + meta.length <= this.currentOffset) {
      const relStart = meta.offset - this.writeBufferFileOffset;
      jsonStr = this.writeBuffer.toString("utf-8", relStart, relStart + meta.length);
    } else if (
      this.readBlockOffset >= 0 &&
      meta.offset >= this.readBlockOffset &&
      meta.offset + meta.length <= this.readBlockOffset + this.readBlockLength
    ) {
      // 2. Check if the record is within the active read block cache
      const relStart = meta.offset - this.readBlockOffset;
      jsonStr = this.readBlockBuffer.toString("utf-8", relStart, relStart + meta.length);
    } else {
      // 3. Read window from disk
      if (this.writeBufferPos > 0) {
        this.flushWriteBuffer();
      }

      if (meta.length > this.bufferSize) {
        const buf = Buffer.allocUnsafe(meta.length);
        readSync(this.fd, buf, 0, meta.length, meta.offset);
        jsonStr = buf.toString("utf-8");
      } else {
        const bytesToRead = Math.min(this.bufferSize, this.currentOffset - meta.offset);
        const bytesRead = readSync(this.fd, this.readBlockBuffer, 0, bytesToRead, meta.offset);
        this.readBlockOffset = meta.offset;
        this.readBlockLength = bytesRead;
        jsonStr = this.readBlockBuffer.toString("utf-8", 0, meta.length);
      }
    }

    const row = JSON.parse(jsonStr);

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
    this.flushWriteBuffer();
    for (const [key, meta] of this.index.entries()) {
      let jsonStr: string;
      if (
        this.readBlockOffset >= 0 &&
        meta.offset >= this.readBlockOffset &&
        meta.offset + meta.length <= this.readBlockOffset + this.readBlockLength
      ) {
        const relStart = meta.offset - this.readBlockOffset;
        jsonStr = this.readBlockBuffer.toString("utf-8", relStart, relStart + meta.length);
      } else {
        const bytesToRead = Math.min(this.bufferSize, this.currentOffset - meta.offset);
        const bytesRead = readSync(this.fd, this.readBlockBuffer, 0, bytesToRead, meta.offset);
        this.readBlockOffset = meta.offset;
        this.readBlockLength = bytesRead;
        jsonStr = this.readBlockBuffer.toString("utf-8", 0, meta.length);
      }
      const row = JSON.parse(jsonStr);
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
