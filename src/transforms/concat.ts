import type { DataBatch, DataStream, Row, TabularReader } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";

export interface ConcatSource {
  reader: TabularReader;
  label?: string;
}

export interface ConcatOptions {
  sourceCol?: string;
  align?: "union" | "intersect";
  batchSize?: number;
}

/**
 * Streaming concatenation reader that combines multiple TabularReaders sequentially
 * into a single unified DataStream with zero extra memory overhead and optional source tagging.
 */
export class ConcatReader implements TabularReader {
  private sources: ConcatSource[];
  private options: ConcatOptions;

  constructor(sources: Array<TabularReader | ConcatSource>, options: ConcatOptions = {}) {
    if (sources.length === 0) {
      throw new InvalidArgumentError("ConcatReader requires at least one reader or source");
    }

    this.sources = sources.map((s) => ("reader" in s ? s : { reader: s }));
    this.options = options;
  }

  async *read(): DataStream {
    const sourceCol = this.options.sourceCol;
    const batchSize = this.options.batchSize || 1000;
    let outRows: Row[] = [];
    let offset = 0;

    for (let i = 0; i < this.sources.length; i++) {
      const src = this.sources[i]!;
      const label = src.label || `source_${i + 1}`;

      for await (const batch of src.reader.read()) {
        for (const row of batch.rows) {
          const outRow: Row = { ...row };
          if (sourceCol) {
            outRow[sourceCol] = label;
          }

          outRows.push(outRow);
          if (outRows.length >= batchSize) {
            yield { rows: outRows, offset };
            offset += outRows.length;
            outRows = [];
          }
        }
      }
    }

    if (outRows.length > 0) {
      yield { rows: outRows, offset };
    }
  }

  async close(): Promise<void> {
    for (const src of this.sources) {
      if (src.reader.close) {
        await src.reader.close();
      }
    }
  }
}

/**
 * Helper to concatenate multiple tabular readers with options.
 */
export function concatReaders(
  sources: Array<TabularReader | ConcatSource>,
  options: ConcatOptions = {}
): TabularReader {
  return new ConcatReader(sources, options);
}
