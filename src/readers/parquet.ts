import { readFile } from "node:fs/promises";
import type { Readable } from "node:stream";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { ParseError } from "../core/errors.js";
import type {
  ColumnType,
  DataBatch,
  DataStream,
  InspectionMetadata,
  ReaderOptions,
  Row,
  TabularReader,
} from "../core/types.js";

export interface ParquetReaderOptions extends ReaderOptions {
  filePath?: string;
}

/**
 * Maps Parquet primitive and logical types to Rowpipe ColumnType.
 */
function mapParquetType(parquetType?: string): ColumnType {
  if (!parquetType) return "mixed";
  switch (parquetType) {
    case "BOOLEAN":
      return "boolean";
    case "INT32":
    case "INT64":
      return "integer";
    case "FLOAT":
    case "DOUBLE":
      return "number";
    case "BYTE_ARRAY":
    case "FIXED_LEN_BYTE_ARRAY":
    case "UTF8":
      return "string";
    case "INT96":
    case "DATE":
    case "TIME_MILLIS":
    case "TIME_MICROS":
    case "TIMESTAMP_MILLIS":
    case "TIMESTAMP_MICROS":
      return "datetime";
    default:
      return "mixed";
  }
}

/**
 * Normalizes values returned by Parquet reader (e.g. converting BigInt to Number).
 */
function normalizeParquetValue(val: unknown): unknown {
  if (typeof val === "bigint") {
    if (val <= BigInt(Number.MAX_SAFE_INTEGER) && val >= BigInt(Number.MIN_SAFE_INTEGER)) {
      return Number(val);
    }
    return Number(val);
  }
  if (val instanceof Uint8Array) {
    return new TextDecoder().decode(val);
  }
  return val;
}

/**
 * High-performance pure-JS Parquet Reader supporting Snappy, Zstd, Gzip, and Brotli decompression.
 */
export class ParquetReader implements TabularReader {
  private input: Readable | string;
  private options: ParquetReaderOptions;

  constructor(input: Readable | string, options: ParquetReaderOptions = {}) {
    this.input = input;
    this.options = { ...options };
    if (typeof input === "string") {
      this.options.filePath = input;
    }
  }

  private async getArrayBuffer(): Promise<ArrayBuffer> {
    if (typeof this.input === "string") {
      const buf = await readFile(this.input);
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    }

    // Readable stream: collect chunks into single Buffer
    const chunks: Buffer[] = [];
    for await (const chunk of this.input) {
      chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
    }
    const combined = Buffer.concat(chunks);
    return combined.buffer.slice(combined.byteOffset, combined.byteOffset + combined.byteLength);
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: ParquetReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);

    let arrayBuffer: ArrayBuffer;
    try {
      arrayBuffer = await this.getArrayBuffer();
    } catch (err: any) {
      throw new ParseError(`Failed to load Parquet data: ${err.message}`, {
        file: mergedOptions.filePath,
      });
    }

    let rows: Record<string, unknown>[];
    try {
      rows = (await parquetReadObjects({
        file: arrayBuffer,
        compressors,
      })) as Record<string, unknown>[];
    } catch (err: any) {
      throw new ParseError(`Failed to parse Parquet file: ${err.message}`, {
        file: mergedOptions.filePath,
      });
    }

    let currentBatch: Row[] = [];
    let globalOffset = 0;

    for (let i = 0; i < rows.length; i++) {
      const rawRow = rows[i]!;
      const normalizedRow: Row = {};
      for (const [key, val] of Object.entries(rawRow)) {
        normalizedRow[key] = normalizeParquetValue(val);
      }
      currentBatch.push(normalizedRow);

      if (currentBatch.length >= batchSize) {
        yield {
          rows: currentBatch,
          offset: globalOffset,
        };
        globalOffset += currentBatch.length;
        currentBatch = [];
      }
    }

    if (currentBatch.length > 0) {
      yield {
        rows: currentBatch,
        offset: globalOffset,
      };
    }
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    const arrayBuffer = await this.getArrayBuffer();
    const meta = await parquetMetadataAsync(arrayBuffer);

    const rowCount = typeof meta.num_rows === "bigint" ? Number(meta.num_rows) : Number(meta.num_rows || 0);
    const schemaColumns = (meta.schema || []).filter((s) => s.name && s.name !== "root");

    const columns = schemaColumns.map((col) => ({
      name: col.name,
      type: mapParquetType(col.type),
      nullPercentage: 0,
    }));

    return {
      format: "Parquet",
      rowCount,
      columnCount: columns.length,
      columns,
    };
  }
}
