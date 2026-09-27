import { createWriteStream } from "node:fs";
import type { Writable } from "node:stream";
// @ts-ignore
import parquet from "parquetjs-lite";
import type { DataStream, Row, TabularWriter, WriterOptions } from "../core/types.js";

export interface ParquetWriterOptions extends WriterOptions {
  schema?: Record<string, any>;
  compression?: "UNCOMPRESSED" | "SNAPPY" | "GZIP" | "ZSTD" | "BROTLI" | string;
}

/**
 * Infers a Parquet schema from sample rows.
 */
function inferParquetSchema(sampleRows: Row[]): any {
  const schemaDef: Record<string, { type: string; optional: boolean }> = {};

  for (const row of sampleRows) {
    for (const [key, val] of Object.entries(row)) {
      if (schemaDef[key] && schemaDef[key]!.type === "DOUBLE") {
        continue;
      }

      if (typeof val === "number") {
        if (!schemaDef[key] || schemaDef[key]!.type === "INT64") {
          schemaDef[key] = {
            type: Number.isInteger(val) ? "INT64" : "DOUBLE",
            optional: true,
          };
        }
      } else if (typeof val === "boolean") {
        schemaDef[key] = { type: "BOOLEAN", optional: true };
      } else {
        schemaDef[key] = { type: "UTF8", optional: true };
      }
    }
  }

  // Ensure at least one field
  if (Object.keys(schemaDef).length === 0) {
    schemaDef["value"] = { type: "UTF8", optional: true };
  }

  return new parquet.ParquetSchema(schemaDef);
}

export interface ParquetFieldSanitizer {
  key: string;
  fn: (val: unknown) => any;
}

const schemaSanitizerCache = new WeakMap<object, (row: Row) => Record<string, any>>();

/**
 * Pre-compiles specialized field coercers for a Parquet schema definition.
 * Eliminates repeated Object.entries allocations, type branch checks, and string coercions.
 */
export function compileParquetRowSanitizer(
  schemaDef: Record<string, any>
): (row: Row) => Record<string, any> {
  const fields: ParquetFieldSanitizer[] = [];

  for (const [key, fieldDef] of Object.entries(schemaDef)) {
    const type = fieldDef.type;
    let fn: (val: unknown) => any;

    if (type === "INT64") {
      fn = (val: unknown): number => {
        if (typeof val === "number") {
          return Number.isFinite(val) ? Math.trunc(val) : 0;
        }
        const num = Number(val);
        return Number.isNaN(num) ? 0 : Math.trunc(num);
      };
    } else if (type === "DOUBLE" || type === "FLOAT") {
      fn = (val: unknown): number => {
        if (typeof val === "number") return val;
        const num = Number(val);
        return Number.isNaN(num) ? 0 : num;
      };
    } else if (type === "BOOLEAN") {
      fn = (val: unknown): boolean => (typeof val === "boolean" ? val : Boolean(val));
    } else {
      fn = (val: unknown): string => {
        if (typeof val === "string") return val;
        if (typeof val === "object" && val !== null) return JSON.stringify(val);
        return String(val);
      };
    }

    fields.push({ key, fn });
  }

  const fieldCount = fields.length;

  return (row: Row): Record<string, any> => {
    const sanitized: Record<string, any> = {};
    for (let i = 0; i < fieldCount; i++) {
      const field = fields[i]!;
      const rawVal = row[field.key];
      if (rawVal !== null && rawVal !== undefined) {
        sanitized[field.key] = field.fn(rawVal);
      }
    }
    return sanitized;
  };
}

/**
 * Sanitizes a row according to schema definitions before passing to ParquetWriter.
 * Uses cached compiled field sanitizers to avoid per-row reflection and array allocations.
 */
export function sanitizeRowForParquet(
  row: Row,
  schemaDef: Record<string, any>
): Record<string, any> {
  let sanitizer = schemaSanitizerCache.get(schemaDef);
  if (!sanitizer) {
    sanitizer = compileParquetRowSanitizer(schemaDef);
    schemaSanitizerCache.set(schemaDef, sanitizer);
  }
  return sanitizer(row);
}

/**
 * Streaming Apache Parquet Writer.
 * Batches rows and writes binary columnar data with Snappy compression.
 */
export class ParquetWriter implements TabularWriter {
  private output: Writable | string;
  private options: ParquetWriterOptions;
  private writer?: any;

  constructor(output: Writable | string, options: ParquetWriterOptions = {}) {
    this.output = output;
    this.options = { ...options };
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const mergedOptions: ParquetWriterOptions = {
      ...this.options,
      ...options,
    };

    let parquetSchema: any = null;
    let rowSanitizer: ((row: Row) => Record<string, any>) | null = null;

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (!this.writer) {
        if (mergedOptions.schema) {
          parquetSchema =
            mergedOptions.schema instanceof parquet.ParquetSchema
              ? mergedOptions.schema
              : new parquet.ParquetSchema(mergedOptions.schema);
        } else {
          parquetSchema = inferParquetSchema(batch.rows);
        }

        const schemaFields = parquetSchema.schema;
        rowSanitizer = compileParquetRowSanitizer(schemaFields);

        if (typeof this.output === "string") {
          this.writer = await parquet.ParquetWriter.openFile(parquetSchema, this.output, {
            useDataPageV2: false,
          });
        } else {
          this.writer = await parquet.ParquetWriter.openStream(parquetSchema, this.output, {
            useDataPageV2: false,
          });
        }
      }

      const rows = batch.rows;
      const rLen = rows.length;
      const sanitize = rowSanitizer!;
      const writer = this.writer;

      for (let i = 0; i < rLen; i++) {
        const sanitized = sanitize(rows[i]!);
        await writer.appendRow(sanitized);
      }
    }

    if (!this.writer) {
      // Empty stream fallback
      parquetSchema = new parquet.ParquetSchema({
        empty: { type: "UTF8", optional: true },
      });
      if (typeof this.output === "string") {
        this.writer = await parquet.ParquetWriter.openFile(parquetSchema, this.output);
      } else {
        this.writer = await parquet.ParquetWriter.openStream(parquetSchema, this.output);
      }
    }

    await this.writer.close();
  }

  async close(): Promise<void> {
    if (this.writer) {
      try {
        await this.writer.close();
      } catch {
        // Writer already closed
      }
    }
  }
}
