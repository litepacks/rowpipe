import { safeJsonStringify } from "../../utils/formatting.js";
import type { DiffIndex, IndexedRow } from "../types.js";

/**
 * High-performance in-memory Diff Index backed by JavaScript Map.
 */
export class MemoryDiffIndex implements DiffIndex {
  private store = new Map<string, IndexedRow>();
  private estimatedBytes = 0;

  async set(key: string, row: IndexedRow): Promise<void> {
    const existing = this.store.get(key);
    if (existing) {
      this.estimatedBytes -= this.estimateRowBytes(key, existing);
    }
    this.store.set(key, row);
    this.estimatedBytes += this.estimateRowBytes(key, row);
  }

  async get(key: string): Promise<IndexedRow | undefined> {
    return this.store.get(key);
  }

  async has(key: string): Promise<boolean> {
    return this.store.has(key);
  }

  async delete(key: string): Promise<boolean> {
    const existing = this.store.get(key);
    if (existing) {
      this.estimatedBytes -= this.estimateRowBytes(key, existing);
      return this.store.delete(key);
    }
    return false;
  }

  async *entries(): AsyncIterable<[string, IndexedRow]> {
    for (const entry of this.store.entries()) {
      yield entry;
    }
  }

  count(): number {
    return this.store.size;
  }

  getEstimatedBytes(): number {
    return this.estimatedBytes;
  }

  getSpillStats(): { isSpilled: boolean; spilledBytes: number; entryCount: number } {
    return {
      isSpilled: false,
      spilledBytes: 0,
      entryCount: this.store.size,
    };
  }

  async close(): Promise<void> {
    this.store.clear();
    this.estimatedBytes = 0;
  }

  private estimateRowBytes(key: string, item: IndexedRow): number {
    // Key string bytes + row keys/values + Map object entry overhead in V8 (~128 bytes)
    let bytes = key.length * 2 + 128;
    for (const [k, v] of Object.entries(item.row)) {
      bytes += k.length * 2 + 48;
      if (typeof v === "string") {
        bytes += v.length * 2 + 32;
      } else if (typeof v === "number" || typeof v === "boolean" || typeof v === "bigint") {
        bytes += 16;
      } else if (v !== null && v !== undefined) {
        bytes += safeJsonStringify(v).length * 2 + 48;
      }
    }
    return bytes;
  }
}
