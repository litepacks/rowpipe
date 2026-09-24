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

/**
 * Sanitizes a row according to schema definitions before passing to ParquetWriter.
 */
function sanitizeRowForParquet(row: Row, schemaDef: Record<string, any>): Record<string, any> {
  const sanitized: Record<string, any> = {};

  for (const [key, fieldDef] of Object.entries(schemaDef)) {
    const rawVal = row[key];
    if (rawVal === null || rawVal === undefined) {
      continue;
    }

    if (fieldDef.type === "INT64") {
      const num = Number(rawVal);
      sanitized[key] = isNaN(num) ? 0 : Math.trunc(num);
    } else if (fieldDef.type === "DOUBLE" || fieldDef.type === "FLOAT") {
      const num = Number(rawVal);
      sanitized[key] = isNaN(num) ? 0 : num;
    } else if (fieldDef.type === "BOOLEAN") {
      sanitized[key] = Boolean(rawVal);
    } else {
      sanitized[key] = typeof rawVal === "object" ? JSON.stringify(rawVal) : String(rawVal);
    }
  }

  return sanitized;
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
    let schemaFields: Record<string, any> = {};

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (!this.writer) {
        parquetSchema = inferParquetSchema(batch.rows);
        schemaFields = parquetSchema.schema;

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

      for (let i = 0; i < batch.rows.length; i++) {
        const row = batch.rows[i]!;
        const sanitized = sanitizeRowForParquet(row, schemaFields);
        await this.writer.appendRow(sanitized);
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
