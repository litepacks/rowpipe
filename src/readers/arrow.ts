import type { Readable } from "node:stream";
import { openDecompressedReadStream } from "../utils/compression.js";
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
import * as arrow from "apache-arrow";

export interface ArrowReaderOptions extends ReaderOptions {
  filePath?: string;
}

/**
 * Maps Apache Arrow DataType to Rowpipe ColumnType.
 */
export function mapArrowType(dataType?: arrow.DataType): ColumnType {
  if (!dataType) return "mixed";
  const typeId = dataType.typeId;

  switch (typeId) {
    case arrow.Type.Null:
      return "null";
    case arrow.Type.Bool:
      return "boolean";
    case arrow.Type.Int:
    case arrow.Type.Int8:
    case arrow.Type.Int16:
    case arrow.Type.Int32:
    case arrow.Type.Int64:
    case arrow.Type.Uint8:
    case arrow.Type.Uint16:
    case arrow.Type.Uint32:
    case arrow.Type.Uint64: {
      const intType = dataType as arrow.Int;
      if (intType && intType.bitWidth === 64) {
        return "bigint";
      }
      return "integer";
    }
    case arrow.Type.Float:
    case arrow.Type.Float16:
    case arrow.Type.Float32:
    case arrow.Type.Float64:
      return "number";
    case arrow.Type.Decimal:
      return "decimal";
    case arrow.Type.Date:
    case arrow.Type.DateDay:
    case arrow.Type.DateMillisecond:
      return "date";
    case arrow.Type.Time:
    case arrow.Type.TimeSecond:
    case arrow.Type.TimeMillisecond:
    case arrow.Type.TimeMicrosecond:
    case arrow.Type.TimeNanosecond:
    case arrow.Type.Timestamp:
    case arrow.Type.TimestampSecond:
    case arrow.Type.TimestampMillisecond:
    case arrow.Type.TimestampMicrosecond:
    case arrow.Type.TimestampNanosecond:
      return "datetime";
    case arrow.Type.Binary:
    case arrow.Type.LargeBinary:
    case arrow.Type.FixedSizeBinary:
      return "binary";
    case arrow.Type.Utf8:
    case arrow.Type.LargeUtf8:
    case arrow.Type.Utf8View:
      return "string";
    case arrow.Type.List:
    case arrow.Type.FixedSizeList:
    case arrow.Type.LargeList:
    case arrow.Type.Struct:
    case arrow.Type.Map:
      return "json";
    case arrow.Type.Dictionary: {
      const dict = dataType as arrow.Dictionary;
      return dict.valueType ? mapArrowType(dict.valueType) : "string";
    }
    default:
      return "mixed";
  }
}

/**
 * Normalizes values returned from Arrow vectors into standard JavaScript types.
 */
export function normalizeArrowValue(val: unknown, dataType?: arrow.DataType): unknown {
  if (val === null || val === undefined) {
    return null;
  }

  if (typeof val === "bigint") {
    if (val <= BigInt(Number.MAX_SAFE_INTEGER) && val >= BigInt(Number.MIN_SAFE_INTEGER)) {
      return Number(val);
    }
    return Number(val);
  }

  if (val instanceof Uint8Array) {
    if (dataType && (dataType.typeId === arrow.Type.Utf8 || dataType.typeId === arrow.Type.LargeUtf8)) {
      return new TextDecoder().decode(val);
    }
    return val;
  }

  if (val instanceof Date) {
    return val.toISOString();
  }

  if (dataType) {
    const tid = dataType.typeId;
    if (
      tid === arrow.Type.Date ||
      tid === arrow.Type.DateDay ||
      tid === arrow.Type.DateMillisecond ||
      tid === arrow.Type.Timestamp ||
      tid === arrow.Type.TimestampSecond ||
      tid === arrow.Type.TimestampMillisecond ||
      tid === arrow.Type.TimestampMicrosecond ||
      tid === arrow.Type.TimestampNanosecond
    ) {
      if (typeof val === "number") {
        const d = new Date(val);
        if (!Number.isNaN(d.getTime())) {
          return d.toISOString();
        }
      }
    }
  }

  return val;
}

/**
 * Stream-first Apache Arrow IPC & Feather Reader.
 * Consumes Arrow IPC stream or file batches and yields Rowpipe rows with bounded memory.
 */
export class ArrowReader implements TabularReader {
  private input: Readable | string;
  private options: ArrowReaderOptions;

  constructor(input: Readable | string, options: ArrowReaderOptions = {}) {
    this.input = input;
    this.options = { ...options };
    if (typeof input === "string") {
      this.options.filePath = input;
    }
  }

  private getInputStream(opts?: ReaderOptions): Readable {
    return openDecompressedReadStream(this.input, { ...this.options, ...opts });
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: ArrowReaderOptions = {
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

    let arrowReader: arrow.RecordBatchReader;
    try {
      arrowReader = await arrow.RecordBatchReader.from(inputStream);
      await arrowReader.open();
    } catch (err: any) {
      throw new ParseError(`Failed to initialize Arrow reader: ${err.message}`, {
        file: mergedOptions.filePath,
      });
    }

    const fields = arrowReader.schema.fields;
    const numFields = fields.length;

    let rowsInCurrentBatch: Row[] = [];
    let globalOffset = 0;
    let rowIndex = 0;

    try {
      for await (const recordBatch of arrowReader) {
        const batchRowCount = recordBatch.numRows;
        if (batchRowCount === 0) continue;

        const vectors = new Array(numFields);
        for (let c = 0; c < numFields; c++) {
          vectors[c] = recordBatch.getChildAt(c);
        }

        for (let r = 0; r < batchRowCount; r++) {
          rowIndex++;
          try {
            const row: Row = {};
            for (let c = 0; c < numFields; c++) {
              const field = fields[c]!;
              const rawVal = vectors[c] ? vectors[c].get(r) : null;
              row[field.name] = normalizeArrowValue(rawVal, field.type);
            }

            rowsInCurrentBatch.push(row);

            if (rowsInCurrentBatch.length >= batchSize) {
              yield {
                rows: rowsInCurrentBatch,
                offset: globalOffset,
              };
              globalOffset += rowsInCurrentBatch.length;
              rowsInCurrentBatch = [];
            }
          } catch (err: any) {
            errorHandler.handle(
              new ParseError(`Failed to process Arrow record at row ${rowIndex}: ${err.message}`, {
                file: mergedOptions.filePath,
                row: rowIndex,
              }),
              { row: rowIndex, file: mergedOptions.filePath }
            );
          }
        }
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
    const inputStream = this.getInputStream(options);
    let arrowReader: arrow.RecordBatchReader;
    try {
      arrowReader = await arrow.RecordBatchReader.from(inputStream);
      await arrowReader.open();
    } catch (err: any) {
      throw new ParseError(`Failed to inspect Arrow data: ${err.message}`, {
        file: this.options.filePath,
      });
    }

    const fields = arrowReader.schema.fields;
    const columns = fields.map((f) => ({
      name: f.name,
      type: mapArrowType(f.type),
      nullPercentage: 0,
    }));

    let rowCount = 0;
    for await (const recordBatch of arrowReader) {
      rowCount += recordBatch.numRows;
    }

    const formatName = this.options.format
      ? this.options.format.toUpperCase()
      : this.options.filePath?.toLowerCase().endsWith(".feather")
        ? "FEATHER"
        : "ARROW";

    return {
      format: formatName,
      rowCount,
      columnCount: columns.length,
      columns,
    };
  }
}

export const FeatherReader = ArrowReader;
export type FeatherReaderOptions = ArrowReaderOptions;
