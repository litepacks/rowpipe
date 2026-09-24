import type { Row } from "../../core/types.js";

export type JoinType = "inner" | "left" | "right" | "full" | "semi" | "anti";

export interface JoinKeyMapping {
  left: string[];
  right: string[];
}

export interface JoinOptions {
  on?: string | string[];
  leftKey?: string | string[];
  rightKey?: string | string[];
  type?: JoinType;
  prefixLeft?: string;
  suffixLeft?: string;
  prefixRight?: string;
  suffixRight?: string;
  memoryLimit?: number | string;
  tempDir?: string;
  batchSize?: number;
}

export interface JoinIndex {
  set(key: string, row: Row): Promise<void>;
  get(key: string): Promise<Row[] | undefined>;
  has(key: string): Promise<boolean>;
  markMatched(key: string): void;
  getUnmatched(): AsyncIterable<Row>;
  count(): number;
  close(): Promise<void>;
}
