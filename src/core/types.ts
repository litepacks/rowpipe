import type { Readable, Writable } from "node:stream";

export type Row = Record<string, unknown>;

export interface DataBatch {
  rows: Row[];
  offset: number;
}

export type DataStream = AsyncIterable<DataBatch>;

export type ColumnType =
  | "string"
  | "integer"
  | "bigint"
  | "number"
  | "decimal"
  | "boolean"
  | "date"
  | "datetime"
  | "binary"
  | "json"
  | "null"
  | "mixed";

export type SemanticType =
  | "email"
  | "url"
  | "uuid"
  | "ipv4"
  | "ipv6"
  | "country-code"
  | "currency"
  | "phone";

export type ErrorHandlingStrategy = "abort" | "skip" | "log" | "fail";

export interface ReaderOptions {
  batchSize?: number;
  header?: boolean;
  delimiter?: string;
  path?: string;
  sheet?: string | number;
  maxRows?: number;
  filePath?: string;
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compress?: string;
  compression?: string;
  format?: string;
  table?: string;
  query?: string;
  params?: unknown[];
  signal?: AbortSignal;
  onError?: ErrorHandlingStrategy;
  badRowsLog?: string;
  onParseError?: (error: Error, raw?: unknown) => void;
}

export interface WriterOptions {
  delimiter?: string;
  header?: boolean;
  sheet?: string;
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compress?: string;
  compression?: string;
  format?: string;
  table?: string;
  transaction?: boolean;
  createTable?: boolean;
  upsert?: boolean;
  conflictColumns?: string[];
  truncate?: boolean;
}

export interface SheetMetadata {
  name: string;
  rowCount: number;
  columnCount?: number;
  columns?: string[];
}

export interface InspectionMetadata {
  format: string;
  sizeBytes?: number;
  rowCount?: number;
  columnCount?: number;
  columns?: Array<{
    name: string;
    type: ColumnType;
    nullPercentage: number;
    approxUniquePercentage?: number;
  }>;
  sheetsCount?: number;
  sheets?: SheetMetadata[];
}

export interface TabularReader {
  read(options?: ReaderOptions): DataStream;
  inspect?(options?: ReaderOptions): Promise<InspectionMetadata>;
  close?(): Promise<void>;
}

export interface TabularWriter {
  write(stream: DataStream, options?: WriterOptions): Promise<void>;
  close?(): Promise<void>;
}

export interface FormatAdapter {
  name: string;
  extensions: string[];
  createReader(
    input: Readable | string,
    options?: ReaderOptions
  ): TabularReader;
  createWriter(
    output: Writable | string,
    options?: WriterOptions
  ): TabularWriter;
}

export type TransformFunction = (stream: DataStream) => DataStream;

export interface Aggregator<T> {
  add(row: Row): void;
  merge?(other: Aggregator<T>): void;
  result(): T;
}

export interface ProgressCallbackInfo {
  rowsProcessed: number;
  bytesRead?: number;
  elapsedMs: number;
  rowsPerSecond: number;
  megabytesPerSecond?: number;
}

export type ProgressCallback = (info: ProgressCallbackInfo) => void;
