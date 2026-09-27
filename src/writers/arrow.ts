import type { Writable } from "node:stream";
import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import type { DataStream, Row, TabularWriter, WriterOptions } from "../core/types.js";
import * as arrow from "apache-arrow";

export interface ArrowWriterOptions extends WriterOptions {
  ipcFormat?: "file" | "stream";
}

type ColumnArrowType =
  | arrow.Int32
  | arrow.Int64
  | arrow.Float64
  | arrow.Bool
  | arrow.TimestampMillisecond
  | arrow.Binary
  | arrow.Utf8;

/**
 * Infers appropriate Arrow DataType for each column from sample rows.
 */
function inferArrowSchema(sampleRows: Row[]): Record<string, ColumnArrowType> {
  const schema: Record<string, ColumnArrowType> = {};

  for (const row of sampleRows) {
    for (const [key, val] of Object.entries(row)) {
      if (val === null || val === undefined) {
        continue;
      }

      const existing = schema[key];
      if (existing instanceof arrow.Utf8) {
        continue;
      }

      if (typeof val === "boolean") {
        if (!existing) schema[key] = new arrow.Bool();
      } else if (typeof val === "bigint") {
        schema[key] = new arrow.Int64();
      } else if (typeof val === "number") {
        if (!Number.isFinite(val)) {
          schema[key] = new arrow.Float64();
        } else if (!Number.isInteger(val)) {
          schema[key] = new arrow.Float64();
        } else if (!existing) {
          schema[key] =
            val > 2147483647 || val < -2147483648
              ? new arrow.Int64()
              : new arrow.Int32();
        } else if (existing instanceof arrow.Int32 && (val > 2147483647 || val < -2147483648)) {
          schema[key] = new arrow.Int64();
        }
      } else if (val instanceof Date) {
        if (!existing) schema[key] = new arrow.TimestampMillisecond();
      } else if (Buffer.isBuffer(val) || val instanceof Uint8Array) {
        if (!existing) schema[key] = new arrow.Binary();
      } else if (typeof val === "string") {
        // Test numeric string
        if (/^-?\d+$/.test(val.trim())) {
          const num = Number(val);
          if (Number.isSafeInteger(num)) {
            if (!existing) schema[key] = new arrow.Int32();
            continue;
          }
        } else if (/^-?\d+\.\d+$/.test(val.trim())) {
          if (!existing || existing instanceof arrow.Int32 || existing instanceof arrow.Int64) {
            schema[key] = new arrow.Float64();
            continue;
          }
        }
        schema[key] = new arrow.Utf8();
      } else {
        schema[key] = new arrow.Utf8();
      }
    }
  }

  // Ensure every key present in sampleRows has a type (default to Utf8 if all values were null)
  for (const row of sampleRows) {
    for (const key of Object.keys(row)) {
      if (!schema[key]) {
        schema[key] = new arrow.Utf8();
      }
    }
  }

  return schema;
}

/**
 * Coerces row value to match target Arrow DataType.
 */
function coerceArrowValue(val: unknown, targetType: ColumnArrowType): unknown {
  if (val === null || val === undefined) {
    return null;
  }

  if (targetType instanceof arrow.Float64) {
    const num = Number(val);
    return Number.isFinite(num) ? num : null;
  }

  if (targetType instanceof arrow.Int64) {
    if (typeof val === "bigint") return val;
    const num = Number(val);
    return Number.isFinite(num) ? BigInt(Math.trunc(num)) : null;
  }

  if (targetType instanceof arrow.Int32) {
    const num = Number(val);
    return Number.isFinite(num) ? Math.trunc(num) : null;
  }

  if (targetType instanceof arrow.Bool) {
    if (typeof val === "boolean") return val;
    const s = String(val).trim().toLowerCase();
    if (s === "true" || s === "1") return true;
    if (s === "false" || s === "0") return false;
    return Boolean(val);
  }

  if (targetType instanceof arrow.TimestampMillisecond) {
    if (val instanceof Date) return val.getTime();
    if (typeof val === "number") return val;
    const d = new Date(String(val));
    return Number.isNaN(d.getTime()) ? null : d.getTime();
  }

  if (targetType instanceof arrow.Binary) {
    if (Buffer.isBuffer(val) || val instanceof Uint8Array) return val;
    return Buffer.from(String(val));
  }

  // Utf8 default
  if (typeof val === "object") {
    return JSON.stringify(val);
  }
  return String(val);
}

/**
 * Stream-first Apache Arrow IPC & Feather Writer.
 * Writes columnar batches sequentially with bounded memory.
 */
export class ArrowWriter implements TabularWriter {
  private output: Writable | string;
  private options: ArrowWriterOptions;
  private writer?: arrow.RecordBatchWriter;
  private outStream?: Writable;
  private donePromise?: Promise<void>;

  constructor(output: Writable | string, options: ArrowWriterOptions = {}) {
    this.output = output;
    this.options = { ...options };
  }

  private resolveIpcFormat(options?: ArrowWriterOptions): "file" | "stream" {
    if (options?.ipcFormat) return options.ipcFormat;
    if (this.options.ipcFormat) return this.options.ipcFormat;

    if (typeof this.output === "string" && this.output !== "-") {
      return "file";
    }
    return "stream";
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const mergedOptions: ArrowWriterOptions = {
      ...this.options,
      ...options,
    };

    const ipcFormat = this.resolveIpcFormat(mergedOptions);
    this.outStream = openCompressedWriteStream(this.output, mergedOptions);

    this.writer =
      ipcFormat === "file"
        ? new arrow.RecordBatchFileWriter()
        : new arrow.RecordBatchStreamWriter();

    const nodeStream = this.writer.toNodeStream();

    const isStdout = this.outStream === process.stdout;
    this.donePromise = new Promise<void>((resolve, reject) => {
      nodeStream.once("error", reject);
      if (isStdout) {
        nodeStream.once("end", resolve);
      } else {
        this.outStream!.once("finish", resolve);
        this.outStream!.once("error", reject);
      }
    });

    if (isStdout) {
      nodeStream.pipe(this.outStream, { end: false });
    } else {
      nodeStream.pipe(this.outStream);
    }

    let schema: Record<string, ColumnArrowType> | null = null;
    let schemaKeys: string[] = [];

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (!schema) {
        schema = inferArrowSchema(batch.rows);
        schemaKeys = Object.keys(schema);

        // Fallback for empty row objects
        if (schemaKeys.length === 0) {
          schema["value"] = new arrow.Utf8();
          schemaKeys = ["value"];
        }
      }

      const rowCount = batch.rows.length;
      const vectorMap: Record<string, arrow.Vector> = {};

      for (let c = 0; c < schemaKeys.length; c++) {
        const col = schemaKeys[c]!;
        const arrowType = schema[col]!;
        const colVals = new Array(rowCount);

        for (let r = 0; r < rowCount; r++) {
          const raw = batch.rows[r]![col];
          colVals[r] = coerceArrowValue(raw, arrowType);
        }

        vectorMap[col] = arrow.vectorFromArray(colVals, arrowType);
      }

      const arrowBatch = new (arrow.RecordBatch as any)(vectorMap);
      this.writer.write(arrowBatch);
    }

    if (!schema) {
      // Empty input stream fallback
      const emptyBatch = new (arrow.RecordBatch as any)({
        empty: arrow.vectorFromArray([], new arrow.Utf8()),
      });
      this.writer.write(emptyBatch);
    }

    this.writer.close();
    await this.donePromise;
  }

  async close(): Promise<void> {
    if (this.outStream && this.outStream !== process.stdout) {
      await closeWritableStream(this.outStream);
    }
  }
}

export const FeatherWriter = ArrowWriter;
export type FeatherWriterOptions = ArrowWriterOptions;
