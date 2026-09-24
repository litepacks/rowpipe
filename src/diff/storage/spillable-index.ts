import type { DiffIndex, IndexedRow } from "../types.js";
import { DiskDiffIndex } from "./disk-index.js";
import { MemoryDiffIndex } from "./memory-index.js";

/**
 * Parses a human-readable memory limit string (e.g. "256mb", "1gb", "64MB") into bytes.
 */
export function parseMemoryLimit(input?: string | number, defaultBytes = 256 * 1024 * 1024): number {
  if (!input) return defaultBytes;
  if (typeof input === "number") return input;

  const str = input.trim().toLowerCase();
  const match = /^(\d+(?:\.\d+)?)\s*([a-z]+)?$/.exec(str);
  if (!match) return defaultBytes;

  const val = Number.parseFloat(match[1]!);
  const unit = match[2] || "b";

  switch (unit) {
    case "gb":
    case "g":
      return Math.round(val * 1024 * 1024 * 1024);
    case "mb":
    case "m":
      return Math.round(val * 1024 * 1024);
    case "kb":
    case "k":
      return Math.round(val * 1024);
    default:
      return Math.round(val);
  }
}

/**
 * Hybrid Spillable Diff Index that stays in memory for high speed,
 * and automatically spills to partitioned disk storage when memory exceeds the threshold.
 */
export class SpillableDiffIndex implements DiffIndex {
  private memoryIndex = new MemoryDiffIndex();
  private diskIndex?: DiskDiffIndex;
  private memoryLimitBytes: number;
  private isSpilled = false;

  constructor(memoryLimit: number | string = 256 * 1024 * 1024) {
    this.memoryLimitBytes = parseMemoryLimit(memoryLimit);
  }

  private async spillToDisk(): Promise<void> {
    if (this.isSpilled) return;

    this.diskIndex = new DiskDiffIndex();
    for await (const [key, row] of this.memoryIndex.entries()) {
      await this.diskIndex.set(key, row);
    }

    await this.memoryIndex.close();
    this.isSpilled = true;
  }

  async set(key: string, row: IndexedRow): Promise<void> {
    if (this.isSpilled && this.diskIndex) {
      await this.diskIndex.set(key, row);
      return;
    }

    await this.memoryIndex.set(key, row);

    if (this.memoryIndex.getEstimatedBytes() >= this.memoryLimitBytes) {
      await this.spillToDisk();
    }
  }

  async get(key: string): Promise<IndexedRow | undefined> {
    if (this.isSpilled && this.diskIndex) {
      return this.diskIndex.get(key);
    }
    return this.memoryIndex.get(key);
  }

  async has(key: string): Promise<boolean> {
    if (this.isSpilled && this.diskIndex) {
      return this.diskIndex.has(key);
    }
    return this.memoryIndex.has(key);
  }

  async delete(key: string): Promise<boolean> {
    if (this.isSpilled && this.diskIndex) {
      return this.diskIndex.delete(key);
    }
    return this.memoryIndex.delete(key);
  }

  async *entries(): AsyncIterable<[string, IndexedRow]> {
    if (this.isSpilled && this.diskIndex) {
      yield* this.diskIndex.entries();
    } else {
      yield* this.memoryIndex.entries();
    }
  }

  count(): number {
    if (this.isSpilled && this.diskIndex) {
      return this.diskIndex.count();
    }
    return this.memoryIndex.count();
  }

  getSpillStats(): { isSpilled: boolean; spilledBytes: number; entryCount: number } {
    if (this.isSpilled && this.diskIndex) {
      return this.diskIndex.getSpillStats();
    }
    return this.memoryIndex.getSpillStats();
  }

  async close(): Promise<void> {
    await this.memoryIndex.close();
    if (this.diskIndex) {
      await this.diskIndex.close();
    }
  }
}
