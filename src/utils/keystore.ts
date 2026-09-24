import { promises as fsPromises } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { parseMemoryLimit } from "../diff/storage/spillable-index.js";

export interface KeyStore {
  has(key: string): Promise<boolean>;
  add(key: string): Promise<boolean>;
  delete(key: string): Promise<boolean>;
  count(): number;
  close(): Promise<void>;
  getSpillStats?(): { isSpilled: boolean; spilledBytes: number; entryCount: number };
}

export class MemoryKeyStore implements KeyStore {
  private set = new Set<string>();
  private estimatedBytes = 0;

  async has(key: string): Promise<boolean> {
    return this.set.has(key);
  }

  async add(key: string): Promise<boolean> {
    if (this.set.has(key)) {
      return false;
    }
    this.set.add(key);
    this.estimatedBytes += key.length * 2 + 16;
    return true;
  }

  async delete(key: string): Promise<boolean> {
    if (this.set.delete(key)) {
      this.estimatedBytes = Math.max(0, this.estimatedBytes - (key.length * 2 + 16));
      return true;
    }
    return false;
  }

  count(): number {
    return this.set.size;
  }

  getEstimatedBytes(): number {
    return this.estimatedBytes;
  }

  async *keys(): AsyncIterable<string> {
    for (const key of this.set) {
      yield key;
    }
  }

  async close(): Promise<void> {
    this.set.clear();
    this.estimatedBytes = 0;
  }

  getSpillStats() {
    return { isSpilled: false, spilledBytes: 0, entryCount: this.set.size };
  }
}

/**
 * Disk-backed KeyStore using 256 hash partitions.
 */
export class DiskKeyStore implements KeyStore {
  private tempDir: string;
  private partitionSets = new Map<number, Set<string>>();
  private totalCount = 0;
  private spilledBytes = 0;
  private isClosed = false;

  constructor(customTempDir?: string) {
    const baseDir = customTempDir || os.tmpdir();
    this.tempDir = path.join(
      baseDir,
      `rowpipe-keystore-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
  }

  private async ensureDir(): Promise<void> {
    await fsPromises.mkdir(this.tempDir, { recursive: true });
  }

  private getPartition(key: string): number {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
    }
    return hash % 256;
  }

  private getPartitionFilePath(partition: number): string {
    return path.join(this.tempDir, `part_${partition}.txt`);
  }

  private async loadPartition(partition: number): Promise<Set<string>> {
    if (this.partitionSets.has(partition)) {
      return this.partitionSets.get(partition)!;
    }

    const set = new Set<string>();
    const filePath = this.getPartitionFilePath(partition);
    try {
      const content = await fsPromises.readFile(filePath, "utf-8");
      const lines = content.split("\n");
      for (const line of lines) {
        if (line.length > 0) {
          set.add(line);
        }
      }
    } catch {
      // File does not exist yet
    }

    // Keep active partitions bounded in memory
    if (this.partitionSets.size >= 16) {
      const firstKey = this.partitionSets.keys().next().value;
      if (firstKey !== undefined) {
        await this.flushPartition(firstKey);
        this.partitionSets.delete(firstKey);
      }
    }

    this.partitionSets.set(partition, set);
    return set;
  }

  private async flushPartition(partition: number): Promise<void> {
    const set = this.partitionSets.get(partition);
    if (!set) return;
    await this.ensureDir();
    const filePath = this.getPartitionFilePath(partition);
    const content = Array.from(set).join("\n") + "\n";
    await fsPromises.writeFile(filePath, content, "utf-8");
    this.spilledBytes += Buffer.byteLength(content, "utf-8");
  }

  async has(key: string): Promise<boolean> {
    const part = this.getPartition(key);
    const set = await this.loadPartition(part);
    return set.has(key);
  }

  async add(key: string): Promise<boolean> {
    const part = this.getPartition(key);
    const set = await this.loadPartition(part);
    if (set.has(key)) {
      return false;
    }
    set.add(key);
    this.totalCount++;
    return true;
  }

  async delete(key: string): Promise<boolean> {
    const part = this.getPartition(key);
    const set = await this.loadPartition(part);
    if (set.delete(key)) {
      this.totalCount--;
      return true;
    }
    return false;
  }

  count(): number {
    return this.totalCount;
  }

  getSpillStats() {
    return { isSpilled: true, spilledBytes: this.spilledBytes, entryCount: this.totalCount };
  }

  async close(): Promise<void> {
    if (this.isClosed) return;
    this.isClosed = true;
    this.partitionSets.clear();
    try {
      await fsPromises.rm(this.tempDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup error
    }
  }
}

/**
 * Hybrid Spillable KeyStore.
 */
export class SpillableKeyStore implements KeyStore {
  private memoryStore = new MemoryKeyStore();
  private diskStore?: DiskKeyStore;
  private memoryLimitBytes: number;
  private tempDir?: string;
  private isSpilled = false;

  constructor(options: { memoryLimit?: number | string; tempDir?: string } = {}) {
    this.memoryLimitBytes = parseMemoryLimit(options.memoryLimit, 256 * 1024 * 1024);
    this.tempDir = options.tempDir;
  }

  private async spillToDisk(): Promise<void> {
    if (this.isSpilled) return;
    this.diskStore = new DiskKeyStore(this.tempDir);
    for await (const key of this.memoryStore.keys()) {
      await this.diskStore.add(key);
    }
    await this.memoryStore.close();
    this.isSpilled = true;
  }

  async has(key: string): Promise<boolean> {
    if (this.isSpilled && this.diskStore) {
      return this.diskStore.has(key);
    }
    return this.memoryStore.has(key);
  }

  async add(key: string): Promise<boolean> {
    if (this.isSpilled && this.diskStore) {
      return this.diskStore.add(key);
    }

    const added = await this.memoryStore.add(key);
    if (this.memoryStore.getEstimatedBytes() >= this.memoryLimitBytes) {
      await this.spillToDisk();
    }
    return added;
  }

  async delete(key: string): Promise<boolean> {
    if (this.isSpilled && this.diskStore) {
      return this.diskStore.delete(key);
    }
    return this.memoryStore.delete(key);
  }

  count(): number {
    if (this.isSpilled && this.diskStore) {
      return this.diskStore.count();
    }
    return this.memoryStore.count();
  }

  getSpillStats() {
    if (this.isSpilled && this.diskStore) {
      return this.diskStore.getSpillStats();
    }
    return this.memoryStore.getSpillStats();
  }

  async close(): Promise<void> {
    await this.memoryStore.close();
    if (this.diskStore) {
      await this.diskStore.close();
    }
  }
}
