import { Writable } from "node:stream";
import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import { ParseError } from "../core/errors.js";
import type { DataBatch, DataStream, Row, TabularWriter, WriterOptions } from "../core/types.js";
// @ts-ignore
import avsc from "avsc";

export interface AvroWriterOptions extends WriterOptions {
  schema?: any;
  recordName?: string;
}

/**
 * Infers an Avro record schema from sample rows.
 * Uses Avro unions ["null", type] to safely accommodate missing and null values.
 */
export function inferAvroSchema(sampleRows: Row[], recordName = "Row"): any {
  const fieldTypes: Record<string, { baseType: any; hasNull: boolean }> = {};

  for (const row of sampleRows) {
    for (const [key, val] of Object.entries(row)) {
      if (!fieldTypes[key]) {
        fieldTypes[key] = { baseType: null, hasNull: false };
      }

      if (val === null || val === undefined) {
        fieldTypes[key]!.hasNull = true;
        continue;
      }

      const current = fieldTypes[key]!.baseType;
      if (current === "string") {
        continue;
      }

      if (typeof val === "boolean") {
        if (!current) fieldTypes[key]!.baseType = "boolean";
      } else if (typeof val === "number") {
        if (!Number.isFinite(val)) {
          fieldTypes[key]!.baseType = "double";
        } else if (!Number.isInteger(val)) {
          fieldTypes[key]!.baseType = "double";
        } else if (!current || current === "int") {
          fieldTypes[key]!.baseType =
            val > 2147483647 || val < -2147483648 ? "long" : "int";
        } else if (current === "long") {
          // already long
        } else {
          fieldTypes[key]!.baseType = "double";
        }
      } else if (typeof val === "bigint") {
        fieldTypes[key]!.baseType = "long";
      } else if (val instanceof Date) {
        if (!current) {
          fieldTypes[key]!.baseType = { type: "long", logicalType: "timestamp-millis" };
        }
      } else if (Buffer.isBuffer(val) || val instanceof Uint8Array) {
        if (!current) fieldTypes[key]!.baseType = "bytes";
      } else if (Array.isArray(val)) {
        if (!current) fieldTypes[key]!.baseType = { type: "array", items: "string" };
      } else if (typeof val === "object") {
        if (!current) fieldTypes[key]!.baseType = { type: "map", values: "string" };
      } else if (typeof val === "string") {
        const trimmed = val.trim();
        if (/^-?\d+$/.test(trimmed)) {
          const num = Number(trimmed);
          if (Number.isSafeInteger(num)) {
            if (!current) {
              fieldTypes[key]!.baseType =
                num > 2147483647 || num < -2147483648 ? "long" : "int";
              continue;
            }
          }
        } else if (/^-?\d+\.\d+$/.test(trimmed)) {
          if (!current || current === "int" || current === "long") {
            fieldTypes[key]!.baseType = "double";
            continue;
          }
        }
        fieldTypes[key]!.baseType = "string";
      } else {
        fieldTypes[key]!.baseType = "string";
      }
    }
  }

  const fields = Object.entries(fieldTypes).map(([name, info]) => {
    const base = info.baseType || "string";
    // For stream robustness, make fields nullable unions by default
    return {
      name,
      type: ["null", base],
      default: null,
    };
  });

  if (fields.length === 0) {
    fields.push({
      name: "value",
      type: ["null", "string"],
      default: null,
    });
  }

  return {
    type: "record",
    name: recordName,
    fields,
  };
}

/**
 * Unwraps Avro union types (e.g. ["null", "int"]) to find the primary non-null target type.
 * Uses a zero-allocation linear scan instead of array filtering.
 */
export function resolveAvroTargetType(schemaType: any): any {
  if (Array.isArray(schemaType)) {
    for (let i = 0; i < schemaType.length; i++) {
      const t = schemaType[i];
      if (t !== "null" && (typeof t !== "object" || t?.type !== "null")) {
        return t;
      }
    }
    return "string";
  }
  return schemaType;
}

/**
 * Compiles a high-performance, specialized type coercer for an Avro field schema.
 * Pre-evaluates unions, logicalTypes, and base types once at schema compilation time,
 * eliminating repeated array allocations and type inspections in the inner streaming loop.
 */
export function createAvroFieldCoercer(schemaType: any): (val: unknown) => unknown {
  const target = resolveAvroTargetType(schemaType);
  let logicalType: string | undefined;
  let baseType: string | undefined;

  if (typeof target === "object" && target !== null) {
    logicalType = target.logicalType;
    if (typeof target.type === "string") {
      baseType = target.type;
    }
  } else if (typeof target === "string") {
    baseType = target;
  }

  if (logicalType === "timestamp-millis") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      if (val instanceof Date) return val.getTime();
      if (typeof val === "number") return val;
      const d = new Date(String(val));
      return Number.isNaN(d.getTime()) ? null : d.getTime();
    };
  }

  if (baseType === "int" || baseType === "long") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      if (typeof val === "number") return Math.trunc(val);
      if (typeof val === "bigint") return Number(val);
      if (typeof val === "string") {
        const s = val.trim();
        if (/^-?\d+$/.test(s)) return Number(s);
      }
      return val;
    };
  }

  if (baseType === "double" || baseType === "float") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      if (typeof val === "number") return val;
      if (typeof val === "string") {
        const s = val.trim();
        if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(s)) return Number(s);
      }
      return val;
    };
  }

  if (baseType === "boolean") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      if (typeof val === "boolean") return val;
      if (typeof val === "string") {
        const s = val.trim().toLowerCase();
        if (s === "true" || s === "1") return true;
        if (s === "false" || s === "0") return false;
      }
      return val;
    };
  }

  if (baseType === "bytes") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      if (Buffer.isBuffer(val)) return val;
      if (val instanceof Uint8Array) return Buffer.from(val);
      return val;
    };
  }

  if (baseType === "string") {
    return (val: unknown): unknown => {
      if (val === null || val === undefined) return null;
      return typeof val === "string" ? val : val;
    };
  }

  return (val: unknown): unknown => {
    if (val === null || val === undefined) return null;
    return val;
  };
}

/**
 * Coerces row value to conform to the Avro field schema.
 */
export function coerceAvroValue(val: unknown, schemaType: any): unknown {
  if (val === null || val === undefined) {
    return null;
  }

  let target = resolveAvroTargetType(schemaType);

  if (typeof target === "object" && target !== null) {
    if (target.logicalType === "timestamp-millis") {
      if (val instanceof Date) return val.getTime();
      if (typeof val === "number") return val;
      const d = new Date(String(val));
      return Number.isNaN(d.getTime()) ? null : d.getTime();
    }
    if (target.type) {
      target = target.type;
    }
  }

  if (target === "int" || target === "long") {
    if (typeof val === "number") return Math.trunc(val);
    if (typeof val === "bigint") return Number(val);
    if (typeof val === "string") {
      const s = val.trim();
      if (/^-?\d+$/.test(s)) return Number(s);
    }
    return val;
  }

  if (target === "double" || target === "float") {
    if (typeof val === "number") return val;
    if (typeof val === "string") {
      const s = val.trim();
      if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(s)) return Number(s);
    }
    return val;
  }

  if (target === "boolean") {
    if (typeof val === "boolean") return val;
    if (typeof val === "string") {
      const s = val.trim().toLowerCase();
      if (s === "true" || s === "1") return true;
      if (s === "false" || s === "0") return false;
    }
    return val;
  }

  if (target === "bytes") {
    if (Buffer.isBuffer(val)) return val;
    if (val instanceof Uint8Array) return Buffer.from(val);
    return val;
  }

  if (target === "string") {
    if (typeof val === "string") return val;
    return val;
  }

  return val;
}

/**
 * Stream-first Apache Avro Writer.
 * Writes Object Container Files (OCF) block-by-block with bounded memory.
 */
export class AvroWriter implements TabularWriter {
  private output: Writable | string;
  private options: AvroWriterOptions;
  private outStream?: Writable;
  private encoder?: any;
  private donePromise?: Promise<void>;

  constructor(output: Writable | string | any, options: AvroWriterOptions = {}) {
    if (
      typeof output === "object" &&
      output !== null &&
      !(output instanceof Writable) &&
      ("outputPath" in output || "output" in output)
    ) {
      this.output = output.outputPath || output.output || "-";
      this.options = { ...output, ...options };
    } else {
      this.output = output;
      this.options = { ...options };
    }
  }

  async write(dataStream: DataStream | any, options?: WriterOptions): Promise<void> {
    const mergedOptions: AvroWriterOptions = {
      ...this.options,
      ...options,
    };

    let stream: DataStream;
    if (Array.isArray(dataStream)) {
      const arr = dataStream;
      stream = (async function* () {
        yield { rows: arr, offset: 0 };
      })();
    } else if (
      dataStream &&
      (Symbol.asyncIterator in Object(dataStream) || Symbol.iterator in Object(dataStream))
    ) {
      stream = (async function* () {
        for await (const item of (dataStream as any)) {
          if (item && Array.isArray((item as any).rows)) {
            yield item as DataBatch;
          } else if (Array.isArray(item)) {
            yield { rows: item, offset: 0 };
          } else if (item) {
            yield { rows: [item], offset: 0 };
          }
        }
      })();
    } else {
      stream = dataStream;
    }

    this.outStream = openCompressedWriteStream(this.output, mergedOptions);

    let avroType: any = null;
    let fieldDefs: Array<{ name: string; type: any }> = [];
    let fieldCoercers: Array<{ name: string; coerce: (val: unknown) => unknown }> = [];
    let fieldCoercerCount = 0;
    let rowIndex = 0;

    const isStdout = this.outStream === process.stdout;

    for await (const batch of stream) {
      if (batch.rows.length === 0) continue;

      if (!avroType) {
        const schema =
          mergedOptions.schema ||
          inferAvroSchema(batch.rows, mergedOptions.recordName || "Row");

        try {
          avroType = avsc.Type.forSchema(schema);
        } catch (err: any) {
          throw new ParseError(`Failed to compile Avro schema: ${err.message}`);
        }

        fieldDefs = schema.fields || [];
        fieldCoercers = fieldDefs.map((field) => ({
          name: field.name,
          coerce: createAvroFieldCoercer(field.type),
        }));
        fieldCoercerCount = fieldCoercers.length;

        this.encoder = new avsc.streams.BlockEncoder(avroType);

        this.donePromise = new Promise<void>((resolve, reject) => {
          this.encoder.once("error", reject);
          if (isStdout) {
            this.encoder.once("end", resolve);
          } else {
            this.outStream!.once("finish", resolve);
            this.outStream!.once("error", reject);
          }
        });

        if (isStdout) {
          this.encoder.pipe(this.outStream, { end: false });
        } else {
          this.encoder.pipe(this.outStream);
        }
      }

      const batchRows = batch.rows;
      const bLen = batchRows.length;

      for (let r = 0; r < bLen; r++) {
        rowIndex++;
        const rawRow = batchRows[r]!;
        const sanitizedRow: Record<string, unknown> = {};

        for (let f = 0; f < fieldCoercerCount; f++) {
          const fc = fieldCoercers[f]!;
          sanitizedRow[fc.name] = fc.coerce(rawRow[fc.name]);
        }

        if (!avroType.isValid(sanitizedRow)) {
          const errors: any[] = [];
          avroType.isValid(sanitizedRow, {
            errorHook: (path: string[], anyVal: unknown, fieldType: any) => {
              errors.push({
                field: path.join(".") || fieldDefs[0]?.name || "(unknown)",
                expectedType:
                  typeof fieldType?.schema === "function" ? fieldType.schema() : fieldType,
                receivedValue: anyVal,
                receivedType: typeof anyVal,
              });
            },
          });

          if (errors.length > 0) {
            const err = errors[0]!;
            throw new ParseError(
              `Avro schema validation failed at record ${rowIndex}: field "${err.field}" expected ${JSON.stringify(
                err.expectedType
              )}, received ${err.receivedType} (${JSON.stringify(err.receivedValue)})`
            );
          } else {
            throw new ParseError(`Avro schema validation failed at record ${rowIndex}`);
          }
        }

        this.encoder.write(sanitizedRow);
      }
    }

    if (!avroType) {
      // Empty input stream fallback
      const schema = {
        type: "record",
        name: mergedOptions.recordName || "Row",
        fields: [{ name: "value", type: ["null", "string"], default: null }],
      };
      avroType = avsc.Type.forSchema(schema as any);
      this.encoder = new avsc.streams.BlockEncoder(avroType);

      this.donePromise = new Promise<void>((resolve, reject) => {
        this.encoder.once("error", reject);
        if (isStdout) {
          this.encoder.once("end", resolve);
        } else {
          this.outStream!.once("finish", resolve);
          this.outStream!.once("error", reject);
        }
      });

      if (isStdout) {
        this.encoder.pipe(this.outStream, { end: false });
      } else {
        this.encoder.pipe(this.outStream);
      }
    }

    this.encoder.end();
    await this.donePromise;
  }

  async close(): Promise<void> {
    if (this.outStream && this.outStream !== process.stdout) {
      await closeWritableStream(this.outStream);
    }
  }
}
