import type {
  Aggregator,
  ColumnType,
  DataBatch,
  DataStream,
  PipelinePreviewResult,
  ProgressCallback,
  ReaderOptions,
  Row,
  TabularReader,
  TabularWriter,
  TransformFunction,
  WriterOptions,
} from "./types.js";
import { batchesToRows, rowsToBatches } from "./batch.js";
import { filterRows } from "../transforms/filter.js";
import { selectColumns } from "../transforms/select.js";
import { limitRows } from "../transforms/limit.js";
import { offsetRows } from "../transforms/offset.js";
import { tailRows } from "../transforms/tail.js";
import { topRows, TopOptions } from "../transforms/top.js";
import { sortRows, SortOptions } from "../transforms/sort/index.js";
import { uniqueRows, UniqueOptions } from "../transforms/unique.js";
import { groupRows, GroupOptions } from "../transforms/group.js";
import { explodeRows, ExplodeOptions } from "../transforms/explode.js";
import { flattenRows, FlattenOptions } from "../transforms/flatten.js";
import { mapRows, parseMapSpecs, type MapOptions, type RowMapper } from "../transforms/map.js";
import { renameColumns, parseRenameSpecs } from "../transforms/rename.js";
import { castColumns, parseCastSpecs, type CastOptions } from "../transforms/cast.js";
import { sampleRows, type SampleOptions } from "../transforms/sample.js";
import { windowRows, type WindowOptions } from "../transforms/window.js";
import { cleanRows, type CleanOptions } from "../transforms/clean.js";
import { classifyPrimitiveType } from "../analytics/schema-inference.js";
import { createReader } from "../readers/index.js";
import { createWriter } from "../writers/index.js";

export class Pipeline {
  private stream: DataStream;
  private progressCallbacks: ProgressCallback[] = [];
  private readerToClose?: TabularReader;

  constructor(
    source: DataStream | TabularReader | Row[] | Iterable<Row> | AsyncIterable<Row> | string,
    options?: ReaderOptions
  ) {
    if (typeof source === "string") {
      const reader = createReader(source, options);
      this.readerToClose = reader;
      this.stream = reader.read(options);
    } else if (source && typeof source === "object" && "read" in source && typeof (source as any).read === "function") {
      this.readerToClose = source as TabularReader;
      this.stream = (source as TabularReader).read(options);
    } else if (Array.isArray(source)) {
      this.stream = rowsToBatches(source, options?.batchSize);
    } else {
      this.stream = source as DataStream;
    }
  }

  /**
   * Chains a transform function to the pipeline.
   */
  pipe(transform: TransformFunction): this {
    this.stream = transform(this.stream);
    return this;
  }

  /**
   * Adds a progress listener.
   */
  onProgress(callback: ProgressCallback): this {
    this.progressCallbacks.push(callback);
    return this;
  }

  /**
   * Wraps the current stream with a progress tracking generator if callbacks are registered.
   */
  private getTrackedStream(): DataStream {
    if (this.progressCallbacks.length === 0) {
      return this.stream;
    }

    const callbacks = this.progressCallbacks;
    const stream = this.stream;

    return (async function* () {
      let rowsProcessed = 0;
      const startTime = Date.now();
      let lastReport = startTime;

      for await (const batch of stream) {
        rowsProcessed += batch.rows.length;
        const now = Date.now();

        // Throttle progress notifications to ~100ms intervals
        if (now - lastReport >= 100) {
          const elapsedSec = (now - startTime) / 1000 || 0.001;
          const info = {
            rowsProcessed,
            elapsedMs: now - startTime,
            rowsPerSecond: Math.round(rowsProcessed / elapsedSec),
          };
          for (const cb of callbacks) {
            cb(info);
          }
          lastReport = now;
        }

        yield batch;
      }

      // Final progress callback
      const finalNow = Date.now();
      const finalElapsedSec = (finalNow - startTime) / 1000 || 0.001;
      const finalInfo = {
        rowsProcessed,
        elapsedMs: finalNow - startTime,
        rowsPerSecond: Math.round(rowsProcessed / finalElapsedSec),
      };
      for (const cb of callbacks) {
        cb(finalInfo);
      }
    })();
  }

  /**
   * Chains a filter expression.
   */
  filter(expression: string): this {
    return this.pipe(filterRows(expression));
  }

  /**
   * Chains a column projection.
   */
  select(columns: string[] | string): this {
    const cols = Array.isArray(columns) ? columns : columns.split(",").map((s) => s.trim()).filter(Boolean);
    return this.pipe(selectColumns(cols));
  }

  /**
   * Chains map transform to derive or transform columns.
   */
  map(specs: Record<string, string> | string | string[] | RowMapper, options?: MapOptions): this {
    if (typeof specs === "function") {
      return this.pipe(mapRows(specs, options));
    }
    if (typeof specs === "string") {
      return this.pipe(mapRows(parseMapSpecs([specs]), options));
    }
    if (Array.isArray(specs)) {
      return this.pipe(mapRows(parseMapSpecs(specs), options));
    }
    return this.pipe(mapRows(specs, options));
  }

  /**
   * Chains rename transform.
   */
  rename(specs: Record<string, string> | string | string[]): this {
    if (typeof specs === "string") {
      return this.pipe(renameColumns(parseRenameSpecs([specs])));
    }
    if (Array.isArray(specs)) {
      return this.pipe(renameColumns(parseRenameSpecs(specs)));
    }
    return this.pipe(renameColumns(specs));
  }

  /**
   * Chains cast transform to convert column types.
   */
  cast(specs: Record<string, string> | string | string[], options?: CastOptions): this {
    if (typeof specs === "string") {
      return this.pipe(castColumns(parseCastSpecs([specs]), options));
    }
    if (Array.isArray(specs)) {
      return this.pipe(castColumns(parseCastSpecs(specs), options));
    }
    return this.pipe(castColumns(specs, options));
  }

  /**
   * Chains reservoir sampling transform with bounded memory.
   */
  sample(options: SampleOptions | number): this {
    const opts = typeof options === "number" ? { rows: options } : options;
    return this.pipe(sampleRows(opts));
  }

  /**
   * Chains sliding window / rolling calculation transform.
   */
  window(options: WindowOptions): this {
    return this.pipe(windowRows(options));
  }

  /**
   * Chains clean and sanitize transform.
   */
  clean(options?: CleanOptions): this {
    return this.pipe(cleanRows(options));
  }

  /**
   * Chains row limit.
   */
  limit(count: number): this {
    return this.pipe(limitRows(count));
  }

  /**
   * Chains row offset.
   */
  offset(count: number): this {
    return this.pipe(offsetRows(count));
  }

  /**
   * Chains tail buffer.
   */
  tail(count = 10): this {
    return this.pipe(tailRows(count));
  }

  /**
   * Chains Top-K heap.
   */
  top(options: TopOptions): this {
    return this.pipe(topRows(options));
  }

  /**
   * Chains sort.
   */
  sort(options: SortOptions | string): this {
    const opts = typeof options === "string" ? { by: options } : options;
    return this.pipe(sortRows(opts));
  }

  /**
   * Chains unique deduplication.
   */
  unique(options: UniqueOptions = {}): this {
    return this.pipe(uniqueRows(options));
  }

  /**
   * Chains group-by aggregation.
   */
  group(options: GroupOptions = {}): this {
    return this.pipe(groupRows(options));
  }

  /**
   * Chains explode transform to expand delimited string or array column to rows.
   */
  explode(columnOrOptions: string | ExplodeOptions, options?: Partial<ExplodeOptions>): this {
    return this.pipe(explodeRows(columnOrOptions, options));
  }

  /**
   * Chains flatten transform to flatten nested JSON objects into dot-notated columns.
   */
  flatten(options?: FlattenOptions): this {
    return this.pipe(flattenRows(options));
  }

  /**
   * Pipes the stream into a TabularWriter or target file path.
   */
  async to(target: TabularWriter | string, options?: WriterOptions): Promise<void> {
    if (options?.dryRun || options?.preview) {
      const count = typeof options.preview === "number" ? options.preview :
                    typeof options.dryRun === "number" ? options.dryRun : 5;
      await this.dryRun(count);
      return;
    }

    const writer: TabularWriter = typeof target === "string"
      ? createWriter(target, options)
      : target;

    const tracked = this.getTrackedStream();
    try {
      await writer.write(tracked, options);
      if (writer.close) {
        await writer.close();
      }
    } finally {
      if (this.readerToClose?.close) {
        await this.readerToClose.close();
      }
    }
  }

  /**
   * Samples first N rows through the pipeline, detects columns and inferred primitive types,
   * without writing to any target file. Closes underlying reader cleanly.
   */
  async dryRun(count = 5): Promise<PipelinePreviewResult> {
    const previewRows: Row[] = [];
    const tracked = this.getTrackedStream();
    try {
      for await (const row of batchesToRows(tracked)) {
        previewRows.push(row);
        if (previewRows.length >= count) {
          break;
        }
      }
    } finally {
      if (this.readerToClose?.close) {
        await this.readerToClose.close();
      }
    }

    const columns = previewRows.length > 0
      ? Array.from(new Set(previewRows.flatMap((r) => Object.keys(r))))
      : [];

    const types: Record<string, ColumnType> = {};
    for (const col of columns) {
      let detectedType: ColumnType = "null";
      for (const row of previewRows) {
        const val = row[col];
        if (val !== null && val !== undefined && val !== "") {
          detectedType = classifyPrimitiveType(val);
          break;
        }
      }
      types[col] = detectedType;
    }

    return {
      rows: previewRows,
      columns,
      types,
      totalSampled: previewRows.length,
    };
  }

  /**
   * Alias for dryRun.
   */
  async preview(count = 5): Promise<PipelinePreviewResult> {
    return this.dryRun(count);
  }

  /**
   * Consumes the stream using an Aggregator.
   */
  async reduce<T>(aggregator: Aggregator<T>): Promise<T> {
    const tracked = this.getTrackedStream();
    try {
      for await (const row of batchesToRows(tracked)) {
        aggregator.add(row);
      }
      return aggregator.result();
    } finally {
      if (this.readerToClose?.close) {
        await this.readerToClose.close();
      }
    }
  }

  /**
   * Returns an AsyncIterable of DataBatches.
   */
  batches(): DataStream {
    return this.getTrackedStream();
  }

  /**
   * Returns an AsyncIterable of individual rows.
   */
  rows(): AsyncIterable<Row> {
    return batchesToRows(this.getTrackedStream());
  }

  /**
   * Collects all rows up to a maximum limit (default 10,000 to prevent unbounded memory).
   */
  async toArray(maxRows = 10000): Promise<Row[]> {
    const results: Row[] = [];
    try {
      for await (const row of this.rows()) {
        results.push(row);
        if (results.length >= maxRows) {
          break;
        }
      }
      return results;
    } finally {
      if (this.readerToClose?.close) {
        await this.readerToClose.close();
      }
    }
  }

  /**
   * Counts total rows flowing through the pipeline.
   */
  async count(): Promise<number> {
    let total = 0;
    try {
      for await (const batch of this.batches()) {
        total += batch.rows.length;
      }
      return total;
    } finally {
      if (this.readerToClose?.close) {
        await this.readerToClose.close();
      }
    }
  }

  /**
   * Closes underlying reader if one was created.
   */
  async close(): Promise<void> {
    if (this.readerToClose?.close) {
      await this.readerToClose.close();
    }
  }
}

export function createPipeline(
  source: DataStream | TabularReader | Row[] | Iterable<Row> | AsyncIterable<Row> | string,
  options?: ReaderOptions
): Pipeline {
  return new Pipeline(source, options);
}

export const rowpipe = {
  from(
    source: DataStream | TabularReader | Row[] | Iterable<Row> | AsyncIterable<Row> | string,
    options?: ReaderOptions
  ): Pipeline {
    return new Pipeline(source, options);
  },
  createPipeline,
  Pipeline,
};
