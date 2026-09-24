import type { ColumnType, ReaderOptions, Row, TabularReader } from "../core/types.js";

export type DuplicateKeyPolicy = "error" | "first" | "last";
export type DiffOutputFormat = "summary" | "rows" | "patch";
export type DiffOnlyFilter = "added" | "removed" | "changed" | "unchanged";

export interface ColumnChange {
  column: string;
  oldVal: unknown;
  newVal: unknown;
}

export type DiffEvent =
  | {
      type: "added";
      key: Record<string, unknown>;
      encodedKey: string;
      row: Row;
      rowNumber: number;
    }
  | {
      type: "removed";
      key: Record<string, unknown>;
      encodedKey: string;
      row: Row;
      rowNumber: number;
    }
  | {
      type: "changed";
      key: Record<string, unknown>;
      encodedKey: string;
      changes: ColumnChange[];
      leftRow: Row;
      rightRow: Row;
      leftRowNumber: number;
      rightRowNumber: number;
    }
  | {
      type: "unchanged";
      key: Record<string, unknown>;
      encodedKey: string;
      row: Row;
      leftRowNumber: number;
      rightRowNumber: number;
    };

export interface IndexedRow {
  rowNumber: number;
  row: Row;
  hash: number;
}

export interface DiffIndex {
  set(key: string, row: IndexedRow): Promise<void>;
  get(key: string): Promise<IndexedRow | undefined>;
  has(key: string): Promise<boolean>;
  delete(key: string): Promise<boolean>;
  entries(): AsyncIterable<[string, IndexedRow]>;
  count(): number;
  getSpillStats?(): { isSpilled: boolean; spilledBytes: number; entryCount: number };
  close(): Promise<void>;
}

export interface SchemaColumnDiff {
  added: Array<{ name: string; type: ColumnType }>;
  removed: Array<{ name: string; type: ColumnType }>;
  changed: Array<{ name: string; leftType: ColumnType; rightType: ColumnType }>;
}

export interface DiffSummary {
  leftSource: string;
  rightSource: string;
  keyColumns: string[];
  rows: {
    added: number;
    removed: number;
    changed: number;
    unchanged: number;
    totalLeft: number;
    totalRight: number;
  };
  columns: Record<string, { changed: number }>;
  schema?: SchemaColumnDiff;
  executionTimeMs?: number;
  spillStats?: { isSpilled: boolean; spilledBytes: number };
}

export interface DiffEngineOptions {
  left?: TabularReader | string;
  leftPath?: string;
  fromLeft?: string;
  right?: TabularReader | string;
  rightPath?: string;
  fromRight?: string;
  keys: string[];
  columns?: string[];
  ignore?: string[];
  duplicateKey?: DuplicateKeyPolicy;
  coerce?: boolean;
  epsilon?: number;
  ignoreCase?: boolean;
  trim?: boolean;
  memoryLimitBytes?: number;
  includeSchema?: boolean;
  leftOptions?: ReaderOptions;
  rightOptions?: ReaderOptions;
  onProgress?: (info: DiffProgressInfo) => void;
}


export interface DiffProgressInfo {
  phase: "indexing_left" | "comparing_right" | "finishing";
  leftRowsProcessed: number;
  rightRowsProcessed: number;
  addedCount: number;
  removedCount: number;
  changedCount: number;
  unchangedCount: number;
  elapsedMs: number;
  isSpilled?: boolean;
}
