import type {
  Aggregator,
  DataBatch,
  DataStream,
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

export class Pipeline {
  private stream: DataStream;
  private progressCallbacks: ProgressCallback[] = [];

  constructor(
    source: DataStream | TabularReader | Row[] | Iterable<Row> | AsyncIterable<Row>,
    options?: ReaderOptions
  ) {
    if (source && typeof source === "object" && "read" in source && typeof (source as any).read === "function") {
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
   * Pipes the stream into a TabularWriter.
   */
  async to(writer: TabularWriter, options?: WriterOptions): Promise<void> {
    const tracked = this.getTrackedStream();
    await writer.write(tracked, options);
    if (writer.close) {
      await writer.close();
    }
  }

  /**
   * Consumes the stream using an Aggregator.
   */
  async reduce<T>(aggregator: Aggregator<T>): Promise<T> {
    const tracked = this.getTrackedStream();
    for await (const row of batchesToRows(tracked)) {
      aggregator.add(row);
    }
    return aggregator.result();
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
    for await (const row of this.rows()) {
      results.push(row);
      if (results.length >= maxRows) {
        break;
      }
    }
    return results;
  }
}

export function createPipeline(
  source: DataStream | TabularReader | Row[] | Iterable<Row> | AsyncIterable<Row>,
  options?: ReaderOptions
): Pipeline {
  return new Pipeline(source, options);
}
