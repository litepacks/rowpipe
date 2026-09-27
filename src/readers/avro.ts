import { Readable } from "node:stream";
import { openDecompressedReadStream, isCompressedPath } from "../utils/compression.js";
import { ParseError } from "../core/errors.js";
import { RowErrorHandler } from "../core/error-handler.js";
import type {
  ColumnType,
  DataBatch,
  DataStream,
  InspectionMetadata,
  ReaderOptions,
  Row,
  TabularReader,
} from "../core/types.js";
// @ts-ignore
import avsc from "avsc";

export interface AvroReaderOptions extends ReaderOptions {
  filePath?: string;
}

/**
 * Maps an Avro schema definition or union to a Rowpipe ColumnType.
 */
export function mapAvroType(schemaType: any): ColumnType {
  if (Array.isArray(schemaType)) {
    const nonNull = schemaType.filter(
      (t) => t !== "null" && (typeof t !== "object" || t.type !== "null")
    );
    if (nonNull.length === 0) return "null";
    if (nonNull.length === 1) return mapAvroType(nonNull[0]);
    return "mixed";
  }

  if (typeof schemaType === "string") {
    switch (schemaType) {
      case "null":
        return "null";
      case "boolean":
        return "boolean";
      case "int":
        return "integer";
      case "long":
        return "bigint";
      case "float":
      case "double":
        return "number";
      case "bytes":
      case "fixed":
        return "binary";
      case "string":
        return "string";
      default:
        return "string";
    }
  }

  if (typeof schemaType === "object" && schemaType !== null) {
    if (schemaType.logicalType) {
      const lt = String(schemaType.logicalType).toLowerCase();
      if (lt === "date") return "date";
      if (lt.includes("timestamp") || lt.includes("time")) return "datetime";
      if (lt === "decimal") return "decimal";
      if (lt === "uuid") return "string";
    }
    if (
      schemaType.type === "record" ||
      schemaType.type === "array" ||
      schemaType.type === "map"
    ) {
      return "json";
    }
    if (schemaType.type) {
      return mapAvroType(schemaType.type);
    }
  }

  return "mixed";
}

/**
 * Normalizes Avro record values into standard JavaScript primitives and objects.
 */
export function normalizeAvroValue(val: unknown): unknown {
  if (val === null || val === undefined) {
    return null;
  }

  if (typeof val === "bigint") {
    if (val <= BigInt(Number.MAX_SAFE_INTEGER) && val >= BigInt(Number.MIN_SAFE_INTEGER)) {
      return Number(val);
    }
    return Number(val);
  }

  if (val instanceof Date) {
    return val.toISOString();
  }

  if (Buffer.isBuffer(val)) {
    return val;
  }

  if (Array.isArray(val)) {
    return val.map(normalizeAvroValue);
  }

  if (typeof val === "object" && !(val instanceof Uint8Array)) {
    const plain: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(val)) {
      plain[k] = normalizeAvroValue(v);
    }
    return plain;
  }

  return val;
}

/**
 * Stream-first Apache Avro Reader.
 * Consumes Avro Container Files (OCF) block-by-block with bounded memory.
 */
export class AvroReader implements TabularReader {
  private input: Readable | string;
  private options: AvroReaderOptions;

  constructor(input: Readable | string | any, options: AvroReaderOptions = {}) {
    if (
      typeof input === "object" &&
      input !== null &&
      !(input instanceof Readable) &&
      ("inputPath" in input || "input" in input)
    ) {
      this.input = input.inputPath || input.input || "-";
      this.options = { ...input, ...options };
    } else {
      this.input = input;
      this.options = { ...options };
    }
    if (typeof this.input === "string") {
      this.options.filePath = this.input;
    }
  }

  private getInputStream(opts?: ReaderOptions): Readable {
    return openDecompressedReadStream(this.input, { ...this.options, ...opts });
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: AvroReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const inputStream = this.getInputStream(mergedOptions);

    const errorHandler = new RowErrorHandler({
      strategy: mergedOptions.onError,
      badRowsLog: mergedOptions.badRowsLog,
      sourceFile: mergedOptions.filePath,
    });

    const decoder = new avsc.streams.BlockDecoder();
    inputStream.pipe(decoder);

    let rowsInCurrentBatch: Row[] = [];
    let globalOffset = 0;
    let rowIndex = 0;

    let resolveNext: (() => void) | null = null;
    let rejectNext: ((err: any) => void) | null = null;
    const queuedRows: Row[] = [];
    let isEnded = false;
    let streamError: any = null;

    decoder.on("data", (record: any) => {
      rowIndex++;
      try {
        const row: Row = {};
        for (const [key, val] of Object.entries(record)) {
          row[key] = normalizeAvroValue(val);
        }
        queuedRows.push(row);
        if (resolveNext) {
          const r = resolveNext;
          resolveNext = null;
          r();
        }
      } catch (err: any) {
        errorHandler.handle(
          new ParseError(`Failed to decode Avro record at row ${rowIndex}: ${err.message}`, {
            file: mergedOptions.filePath,
            row: rowIndex,
          }),
          { row: rowIndex, file: mergedOptions.filePath }
        );
      }
    });

    decoder.on("end", () => {
      isEnded = true;
      if (resolveNext) {
        const r = resolveNext;
        resolveNext = null;
        r();
      }
    });

    decoder.on("error", (err: any) => {
      streamError = err;
      if (rejectNext) {
        const r = rejectNext;
        rejectNext = null;
        r(err);
      }
    });

    inputStream.on("error", (err: any) => {
      streamError = err;
      if (rejectNext) {
        const r = rejectNext;
        rejectNext = null;
        r(err);
      }
    });

    try {
      while (true) {
        if (streamError) {
          throw new ParseError(`Avro stream error: ${streamError.message}`, {
            file: mergedOptions.filePath,
          });
        }

        while (queuedRows.length > 0) {
          const row = queuedRows.shift()!;
          rowsInCurrentBatch.push(row);

          if (rowsInCurrentBatch.length >= batchSize) {
            yield {
              rows: rowsInCurrentBatch,
              offset: globalOffset,
            };
            globalOffset += rowsInCurrentBatch.length;
            rowsInCurrentBatch = [];
          }
        }

        if (isEnded && queuedRows.length === 0) {
          break;
        }

        await new Promise<void>((resolve, reject) => {
          resolveNext = resolve;
          rejectNext = reject;
        });
      }

      if (rowsInCurrentBatch.length > 0) {
        yield {
          rows: rowsInCurrentBatch,
          offset: globalOffset,
        };
      }
    } finally {
      errorHandler.close();
    }
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    // Try fast synchronous header extraction if uncompressed local file
    if (
      typeof this.input === "string" &&
      this.input !== "-" &&
      !isCompressedPath(this.input)
    ) {
      try {
        const header = avsc.extractFileHeader(this.input);
        if (header && header.meta && header.meta["avro.schema"]) {
          const schema = header.meta["avro.schema"];
          const fields: Array<{ name: string; type: any }> = schema.fields || [];

          const columns = fields.map((f) => ({
            name: f.name,
            type: mapAvroType(f.type),
            nullPercentage: 0,
          }));

          let rowCount = 0;
          for await (const batch of this.read({ ...options, batchSize: 5000 })) {
            rowCount += batch.rows.length;
          }

          return {
            format: "avro",
            rowCount,
            columnCount: columns.length,
            columns,
          };
        }
      } catch {
        // Fallback to stream-based inspection
      }
    }

    // Stream-based inspection
    let rowCount = 0;
    let columnNames: string[] = [];
    for await (const batch of this.read({ ...options, batchSize: 5000 })) {
      if (columnNames.length === 0 && batch.rows.length > 0 && batch.rows[0]) {
        columnNames = Object.keys(batch.rows[0]);
      }
      rowCount += batch.rows.length;
    }

    return {
      format: "avro",
      rowCount,
      columnCount: columnNames.length,
      columns: columnNames.map((name) => ({
        name,
        type: "mixed",
        nullPercentage: 0,
      })),
    };
  }
}
